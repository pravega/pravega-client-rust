//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryInternal;
use crate::segment_reader::{AsyncSegmentReader, AsyncSegmentReaderImpl};
use bytes::{Buf, BufMut, BytesMut};
use pravega_rust_client_shared::ScopedSegment;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, warn};

#[derive(Debug)]
pub struct Event {
    offset_in_segment: u64,
    value: Vec<u8>,
}

pub struct SegmentSlice {
    start_offset: i64,
    segment: ScopedSegment,
    read_offset: i64,
    end_offset: i64,
    slice_rx: Receiver<BytesMut>,
}

///
/// Amount of Bytes Read ahead.
///
const READ_BUFFER_SIZE: i32 = 22;

impl SegmentSlice {
    pub(crate) fn new(
        segment: ScopedSegment,
        start_offset: i64,
        factory: Arc<ClientFactoryInternal>,
    ) -> Self {
        let (mut tx, mut rx) = mpsc::channel(2);
        let (mut slice_tx, mut slice_rx) = mpsc::channel(10);
        let handle = factory.get_runtime_handle();
        handle.enter(|| {
            tokio::spawn(SegmentSlice::get_segment_data(
                segment.clone(),
                start_offset,
                tx,
                factory.clone(),
            ))
        });
        handle.enter(|| tokio::spawn(SegmentSlice::read_events(rx, slice_tx, SegmentSlice::read_header)));
        SegmentSlice {
            start_offset,
            segment: segment.clone(),
            read_offset: 0,
            end_offset: i64::MAX,
            slice_rx,
        }
    }

    ///
    /// Method to fetch segment data in a loop
    ///
    async fn get_segment_data(
        segment: ScopedSegment,
        start_offset: i64,
        mut tx: Sender<BytesMut>,
        factory: Arc<ClientFactoryInternal>,
    ) {
        let mut offset: i64 = start_offset;
        let segment_reader = AsyncSegmentReaderImpl::init(segment, &factory).await;
        loop {
            debug!(
                "Send read request to Segment store at offset {:?} with length {:?}",
                offset, READ_BUFFER_SIZE
            );
            let read = segment_reader.read(offset, READ_BUFFER_SIZE).await;
            match read {
                Ok(reply) => {
                    debug!("Read Response from Segment store {:?}", reply);
                    let len = reply.data.len();
                    if len == 0 || reply.end_of_segment {
                        break;
                    } else {
                        let segment_data = bytes::BytesMut::from(reply.data.as_slice());
                        // send data: this waits until there is capacity in the channel.
                        if let Err(e) = tx.send(segment_data).await {
                            info!("Error while sending segment data to event parser {:?} ", e);
                            break;
                        }
                        offset = offset + len as i64;
                    }
                }
                Err(e) => {
                    warn!("Error while reading from segment {:?}", e);
                    break;
                }
            }
        }
    }

    fn get_starting_offset(&self) -> i64 {
        self.start_offset
    }
    fn get_segment(&self) -> String {
        //Returns the name of the segment
        self.segment.to_string()
    }

    ///
    /// Read individual events from received Segment data.
    ///
    async fn read_events(
        mut rx: Receiver<BytesMut>,
        mut event_tx: Sender<BytesMut>,
        header_parser: fn(&mut BytesMut) -> BytesMut,
    ) {
        let mut partial_event = BytesMut::with_capacity(0);
        loop {
            let segment_data = rx.recv().await;
            if let Some(data) = segment_data {
                partial_event =
                    SegmentSlice::extract_event_send(data, partial_event, event_tx.clone(), header_parser)
                        .await;
            } else {
                info!("No more data to read from the Segment store");
                break;
            }
        }
    }

    ///
    /// Extract individual events and transmit it using the Sender.
    /// In-case of a partial event return a BytesMut containing the partial event.
    ///
    async fn extract_event_send(
        mut segment_data: BytesMut,
        mut partial_event: BytesMut,
        mut tx: Sender<BytesMut>,
        parse_header: fn(&mut BytesMut) -> BytesMut,
    ) -> BytesMut {
        if partial_event.capacity() != 0 {
            let bytes_to_read = partial_event.capacity() - partial_event.len();
            if segment_data.remaining() >= bytes_to_read {
                let t = segment_data.split_to(bytes_to_read);
                partial_event.put(t);
                if let Err(e) = tx.send(partial_event).await {
                    warn!("Failed to send the Parsed event due to {:?}", e);
                }
            } else {
                debug!("Partial event being returned since there is more to fill");
                partial_event.put(segment_data.split());
                return partial_event; // return the partial event.
            }
        }

        while segment_data.has_remaining() {
            let mut event_data = parse_header(&mut segment_data);
            let bytes_to_read = event_data.capacity();
            if bytes_to_read == 0 {
                warn!("Found a header with len as zero");
                continue;
            }
            if segment_data.remaining() >= bytes_to_read {
                let t = segment_data.split_to(bytes_to_read);
                event_data.put(t);
                info!("Event data is {:?} with length {}", event_data, event_data.len());
                if let Err(e) = tx.send(event_data).await {
                    warn!("Failed to to send parsed event {:?}", e);
                };
            } else {
                let t = segment_data.split();
                event_data.put(t);
                debug!(
                    "Partial Event read: Current data read {:?} data_read {} to_read {}",
                    event_data,
                    event_data.len(),
                    event_data.capacity()
                );
                return event_data;
            }
        }

        let partial_event = BytesMut::with_capacity(0);
        return partial_event;
    }

    ///
    /// This method reads the header and returns a BytesMut whose size is as big as the event.
    ///
    fn read_header(mut data: &mut BytesMut) -> BytesMut {
        if data.remaining() >= 4 {
            let len = data.get_i32();
            println!("Header length read is {}", len);
            BytesMut::with_capacity(len as usize)
        } else {
            BytesMut::with_capacity(0)
        }
    }
}

///
/// Iterator implementation of SegmentSlice.
///
impl Iterator for SegmentSlice {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        let r = self.slice_rx.try_recv();
        match r {
            Ok(t) => Some(Event {
                offset_in_segment: 0,
                value: t.to_vec(),
            }),
            Err(TryRecvError::Empty) => Some(Event {
                offset_in_segment: 0,
                value: vec![],
            }),
            Err(TryRecvError::Closed) => None,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::client_factory::ClientFactory;
    use crate::segment_reader::AsyncSegmentReader;
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use pravega_controller_client::ControllerClient;
    use pravega_rust_client_shared::{
        Retention, RetentionType, ScaleType, Scaling, Scope, ScopedStream, Segment, Stream,
        StreamConfiguration,
    };
    use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
    use pravega_wire_protocol::commands::{Command, EventCommand, SegmentReadCommand};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::{Receiver, Sender};

    ///
    /// This method reads the header and returns a BytesMut whose size is as big as the event.
    ///
    fn custom_read_header(mut data: &mut BytesMut) -> BytesMut {
        if (data.remaining() >= 4) {
            let len = data.get_i32();
            println!("Header length read is {}", len);
            BytesMut::with_capacity(len as usize)
        } else {
            BytesMut::with_capacity(0)
        }
    }

    #[tokio::test]
    async fn test_read_partial_events() {
        let (mut tx, mut rx) = mpsc::channel(1);
        let (mut slice_tx, mut slice_rx) = mpsc::channel(10);
        tokio::spawn(generate_multiple_constant_size_events(tx));
        tokio::spawn(SegmentSlice::read_events(rx, slice_tx, custom_read_header));

        // receive events as bytes...
        let mut event_size: usize = 0;
        while let Some(slice_data) = slice_rx.recv().await {
            println!("====> Event read = {:?}", slice_data);
            assert!(event_size + 1 == slice_data.len(), "we missed an event ");
            event_size = event_size + 1;
        }
    }

    #[tokio::test]
    async fn test_read_events() {
        let (mut tx, mut rx) = mpsc::channel(2);
        let (mut slice_tx, mut slice_rx) = mpsc::channel(10);
        tokio::spawn(async move { generate_single_event(tx).await });

        tokio::spawn(SegmentSlice::read_events(rx, slice_tx, custom_read_header));

        // receive events as bytes...
        let mut event_size: usize = 0;
        while let Some(slice_data) = slice_rx.recv().await {
            println!("====> Event read = {:?}", slice_data);
            assert!(event_size + 1 == slice_data.len(), "we missed an event ");
            event_size = event_size + 1;
        }
    }

    async fn generate_multiple_constant_size_events(mut tx: Sender<BytesMut>) {
        let max_events = 10;
        let mut buf = BytesMut::with_capacity(10);
        let mut event_count = 0;

        buf.put_i32(1);
        buf.put_u8(b'a');
        buf.put_i32(2);
        buf.put(&b"aa"[..]);
        tx.send(buf).await;

        buf = BytesMut::with_capacity(10);
        buf.put_i32(3);
        buf.put(&b"aaa"[..]);
        tx.send(buf).await;

        buf = BytesMut::with_capacity(10);
        buf.put_i32(4);
        buf.put(&b"aaaa"[..]);
        tx.send(buf).await;

        buf = BytesMut::with_capacity(10);
        buf.put_i32(5);
        buf.put(&b"aaaaa"[..]);
        tx.send(buf).await;

        buf = BytesMut::with_capacity(10);
        buf.put_i32(6);
        buf.put(&b"aaaaaa"[..]);
        tx.send(buf).await;

        buf = BytesMut::with_capacity(10);
        buf.put_i32(7);
        buf.put(&b"aaaaaa"[..]);
        tx.send(buf).await;

        buf = BytesMut::with_capacity(10);
        buf.put_u8(b'a');
        buf.put_i32(8);
        buf.put(&b"aaaaa"[..]);
        tx.send(buf).await;

        buf = BytesMut::with_capacity(10);
        buf.put(&b"aaa"[..]);
        tx.send(buf).await;
    }

    ///
    /// This is a test function to generate single events of varying sizes
    ///
    async fn generate_single_event(mut tx: Sender<BytesMut>) {
        for i in 0..10 {
            println!("==> Send Async request {}", i);
            println!("==> Data from SSS is sent on the channel {}", i);
            let mut buf = BytesMut::with_capacity(32);
            buf.put_i32(i);
            println!("==>Header {} written", i);
            for _ in 0..i {
                buf.put(&b"a"[..]);
                println!("===> Data written {:?}", buf);
            }
            if let Err(_) = tx.send(buf).await {
                println!("==> receiver dropped");
                return;
            }
        }
    }

    #[test]
    fn test_api() {
        let scope_name = Scope::from("testScopeWriter".to_owned());
        let stream_name = Stream::from("testStreamWriter".to_owned());
        let config = ClientConfigBuilder::default()
            .controller_uri(TEST_CONTROLLER_URI)
            .build()
            .expect("creating config");
        let client_factory = ClientFactory::new(config);

        write_event(scope_name.clone(), stream_name.clone(), &client_factory);
        let str = ScopedStream {
            scope: scope_name.clone(),
            stream: stream_name.clone(),
        };
        client_factory
            .get_runtime_handle()
            .block_on(client_factory.get_controller_client().seal_stream(&str));
        let scoped_segment = ScopedSegment {
            scope: scope_name,
            stream: stream_name,
            segment: Segment {
                number: 0,
                tx_id: None,
            },
        };
        let mut slice = SegmentSlice::new(scoped_segment, 0, client_factory.0.clone());
        client_factory
            .get_runtime_handle()
            .block_on(slice.slice_rx.recv());

        client_factory
            .get_runtime_handle()
            .block_on(slice.slice_rx.recv());

        client_factory
            .get_runtime_handle()
            .block_on(slice.slice_rx.recv());

        client_factory
            .get_runtime_handle()
            .block_on(slice.slice_rx.recv());
        // while let Some(t) = slice.next() {
        //     println!("{:?}", t);
        // }
        // let r = slice.next();

        //
        // let handle = client_factory.get_runtime_handle();
        // let reader = handle
        //     .clone()
        //     .block_on(client_factory.create_async_event_reader(scoped_segment));
        // let wire_command: SegmentReadCommand = handle.block_on(reader.read(0, 11)).unwrap();
        // let data = EventCommand::read_from(wire_command.data.as_slice()).unwrap();
        // let data = std::str::from_utf8(data.data.as_slice()).unwrap();

        // println!("{:?}", data);
    }

    fn write_event(scope_name: Scope, stream_name: Stream, client_factory: &ClientFactory) {
        let controller_client = client_factory.get_controller_client();
        let handle = client_factory.get_runtime_handle();
        let new_stream = handle.block_on(create_scope_stream(
            controller_client,
            &scope_name,
            &stream_name,
            1,
        ));
        if new_stream {
            let scoped_stream = ScopedStream {
                scope: scope_name,
                stream: stream_name,
            };
            let mut writer = client_factory.create_event_stream_writer(scoped_stream);
            let rx1 = handle.block_on(writer.write_event(String::from("aaa").into_bytes()));
            let rx2 = handle.block_on(writer.write_event(String::from("bbb").into_bytes()));
            handle.block_on(rx1).unwrap();
            handle.block_on(rx2).unwrap();
            println!("Write completed");
        }
    }

    async fn create_scope_stream(
        controller_client: &dyn ControllerClient,
        scope_name: &Scope,
        stream_name: &Stream,
        segment_number: i32,
    ) -> bool {
        controller_client
            .create_scope(scope_name)
            .await
            .expect("create scope");
        info!("Scope created");
        let request = StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: scope_name.clone(),
                stream: stream_name.clone(),
            },
            scaling: Scaling {
                scale_type: ScaleType::FixedNumSegments,
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: segment_number,
            },
            retention: Retention {
                retention_type: RetentionType::None,
                retention_param: 0,
            },
        };
        let res = controller_client
            .create_stream(&request)
            .await
            .expect("create stream");
        info!("Stream created");
        res
    }
    // fn test_pending_event() {
    //     // test with legal event size
    //     let (tx, _rx) = oneshot::channel();
    //     let data = vec![];
    //     let routing_key = None;
    //
    //     let event = PendingEvent::without_header(routing_key, data, tx).expect("create pending event");
    //     assert_eq!(event.data.len(), 0);
    //
    //     // test with illegal event size
    //     let (tx, rx) = oneshot::channel();
    //     let data = vec![0; (PendingEvent::MAX_WRITE_SIZE + 1) as usize];
    //     let routing_key = None;
    //
    //     let event = PendingEvent::without_header(routing_key, data, tx);
    //     assert!(event.is_none());
    //
    //     let mut rt = Runtime::new().expect("get runtime");
    //     let reply = rt.block_on(rx).expect("get reply");
    //     assert!(reply.is_err());
    // }
}
