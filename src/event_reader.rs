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
use pravega_controller_client::ControllerClient;
use pravega_rust_client_shared::{ScopedSegment, ScopedStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

#[derive(new)]
struct EventReader<'a> {
    stream: ScopedStream,
    controller_client: &'a dyn ControllerClient,
    //config: ClientConfig,

    // ... nothing pub
}

struct Event {
    offset_in_segment: u64,
    value: Vec<u8>,
}

struct SegmentSlice {
    // ... nothing pub
    start_offset: i64,
    segment: ScopedSegment,
    read_offset: i64,
}

impl SegmentSlice {
    fn get_starting_offset(&self) -> i64 {
        self.start_offset
    }
    fn get_segment(&self) -> String {
        //Returns the name of the segment
        self.segment.to_string()
    }

    /// this is the current read offset.
    fn get_offset(&self) -> i64 {
        //Returns the offset up through which all data has been read
        self.read_offset
    }
}

impl<'a> EventReader<'a> {
    pub fn init(stream: ScopedStream, factory: &'a ClientFactoryInternal) -> EventReader<'a> {
        // create a reader object.
        //
        EventReader {
            stream,
            controller_client: factory.get_controller_client(),
        }
    }

    fn release_segment_at(&mut self, _slice: SegmentSlice, _offset_in_segment: u64) {
        //The above two call this with different offsets.
    }

    async fn acquire_segment(&mut self) -> SegmentSlice {
        //Returns a segment which is now owned by this process.
        //Because it returns a reference the slice cannot outlive the reader.
        let (segment, read_offset) = self.get_next_segment().await;
        info!("Acquire segment {} with read offset {}", segment, read_offset);
        SegmentSlice {
            segment,
            read_offset,
            start_offset: read_offset,
        }
    }

    async fn get_next_segment(&mut self) -> (ScopedSegment, i64) {
        // Mock method: Always returns first segment.
        // TODO: This method should check from the ReaderGroupState and return a segment and the corresponding read offset.
        let segments = self
            .controller_client
            .get_current_segments(&self.stream)
            .await
            .expect("Failed to talk to controller");
        (segments.get_segment(0.0), 0)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::client_factory::ClientFactory;
    use crate::segment_reader::AsyncSegmentReader;
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use pravega_rust_client_shared::{
        Retention, RetentionType, ScaleType, Scaling, Scope, Segment, Stream, StreamConfiguration,
    };
    use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
    use pravega_wire_protocol::commands::{Command, EventCommand, SegmentReadCommand};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::{Receiver, Sender};

    #[tokio::test]
    async fn learn_mpsc() {
        let (mut tx, mut rx) = mpsc::channel(100);
        tokio::spawn(async move {
            for i in 0..10 {
                if let Err(_) = tx.send(i).await {
                    println!("receiver dropped");
                    return;
                }
            }
        });

        while let Some(i) = rx.recv().await {
            println!("got = {}", i);
        }
    }

    async fn extract_event_send(
        mut segment_data: BytesMut,
        mut partial_event: BytesMut,
        mut tx: Sender<BytesMut>,
    ) -> BytesMut {
        if partial_event.capacity() != 0 {
            let bytes_to_read = partial_event.capacity() - partial_event.len();
            if (segment_data.remaining() >= bytes_to_read) {
                let t = segment_data.split_to(bytes_to_read);
                partial_event.put(t);
                tx.send(partial_event).await;
            } else {
                println!("Partial event being returned since there is more to fill");
                partial_event.put(segment_data.split());
                return partial_event; // return the partial event.
            }
        }

        while segment_data.has_remaining() {
            let mut event_data = read_header(&mut segment_data);
            let bytes_to_read = event_data.capacity();
            if bytes_to_read == 0 {
                continue;
            }
            if segment_data.remaining() >= bytes_to_read {
                let t = segment_data.split_to(bytes_to_read);
                event_data.put(t);
                println!("Event data is {:?} with length {}", event_data, event_data.len());
                tx.send(event_data).await;
            } else {
                let t = segment_data.split();
                event_data.put(t);
                println!(
                    "Partial Event read: Current data read {:?} data_read {} to_read {}",
                    event_data,
                    event_data.len(),
                    event_data.remaining_mut()
                );
                return event_data;
            }
        }

        let partial_event = BytesMut::with_capacity(0);
        return partial_event;
    }

    fn read_header(mut data: &mut BytesMut) -> BytesMut {
        if (data.remaining() >= 4) {
            let len = data.get_i32();
            println!("Header length read is {}", len);
            BytesMut::with_capacity(len as usize)
        } else {
            BytesMut::with_capacity(0)
        }
    }
    #[tokio::test]
    async fn learn_channel() {
        let (mut tx, mut rx) = mpsc::channel(10);
        let (mut slice_tx, mut slice_rx) = mpsc::channel(10);
        //tokio::spawn(async move { generate_single_event(tx).await });
        tokio::spawn(async move { generate_multiple_constant_size_events(tx).await });

        tokio::spawn(async move {
            let mut partial_event = BytesMut::with_capacity(0);
            loop {
                let segment_data = rx.recv().await.expect("Nothing to received");
                partial_event = extract_event_send(segment_data, partial_event, slice_tx.clone()).await;
            }
        });

        // tokio::spawn(async move {
        //     while let Some(mut segment_data) = rx.recv().await {
        //         let mut read_header = true;
        //         let mut len = -1;
        //         // let mut event_data: BytesMut; // = &mut BytesMut::with_capacity(1);
        //
        //         println!("=====>>>> Data {:?}", segment_data);
        //         while (segment_data.has_remaining()) {
        //             if read_header {
        //                 let event_data = read_header_data(read_header, &mut segment_data);
        //             } else {
        //                 if segment_data.has_remaining() {
        //                     if len > 0 {
        //                         // if event_data.has_remaining_mut() {
        //                         //     println!("=====>>>> header {:?}", len);
        //                         //     read_event_data(read_header, len, &mut segment_data, &mut event_data);
        //                         // }
        //
        //                         if event_data.len() == len as usize {
        //                             println!("=====>>>> event read is {:?} header {:?}", event_data, len);
        //                             let res = slice_tx.send(event_data).await;
        //                             println!("=====>>>> read Event data status {:?}", res);
        //                             read_header = true;
        //                         }
        //                     } else {
        //                         println!("=====>>>>Length of the data to be read is invalid");
        //                     }
        //                 }
        //             }
        //         }
        //
        //         // ///println!("got data of length {:?} with value= {:?} ", i.len(), i);
        //     }
        // });

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

        // while event_count < max_events {
        //     if write_header {
        //         if buf.remaining_mut() >= 4 {
        //             //write Header
        //             buf.put_i32(event_count);
        //             data_count = event_count;
        //             event_count = event_count + 1;
        //             write_header = false;
        //         } else {
        //             break;
        //         }
        //     } else {
        //         while (data_count > 0) {
        //             if (buf.has_remaining_mut()) {
        //                 buf.put_u8(b'a');
        //                 data_count = data_count - 1;
        //             } else {
        //                 break;
        //             }
        //         }
        //         if data_count == 0 {
        //             write_header = true;
        //         }
        //     }
        // }
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

    fn read_event_data(mut read_header: bool, mut len: i32, mut i: &mut BytesMut, mut msg: &mut BytesMut) {
        let to_read = len - msg.len() as i32;
        if i.remaining() >= to_read as usize {
            // data received is greater than event size
            let t = i.split_to(to_read as usize);
            println!("=====>>>>  Read message 1 {:?} len {}", t, t.len());
            msg.put(t);
            println!("=====>>>>  Read message {:?} len {}", msg, msg.len());
            read_header = true;
        } else {
            // data is less than event size.
            len = len - i.remaining() as i32;
            msg.put(i.split());
            if (len == 0) {
                println!("=====>>>>  Read message {:?}", msg);
                read_header = true;
            }
        }
    }

    fn read_header_data(mut read_header: bool, mut i: &mut BytesMut) -> BytesMut {
        println!(
            "=> Amount of data in buffer before reading header  {:?} capacity {:?}",
            i.len(),
            i.capacity()
        );
        let len = i.get_i32();
        println!(
            "=> Amount of data in buffer after reading header {:?} capacity {:?}",
            i.len(),
            i.capacity()
        );
        println!("=> Header length {:?}", len);
        if len == 0 {
            read_header = true;
        } else {
            read_header = false;
        }
        BytesMut::with_capacity(len as usize)
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
        let scoped_segment = ScopedSegment {
            scope: scope_name,
            stream: stream_name,
            segment: Segment {
                number: 0,
                tx_id: None,
            },
        };
        let handle = client_factory.get_runtime_handle();
        let reader = handle
            .clone()
            .block_on(client_factory.create_async_event_reader(scoped_segment));
        let wire_command: SegmentReadCommand = handle.block_on(reader.read(0, 11)).unwrap();
        let data = EventCommand::read_from(wire_command.data.as_slice()).unwrap();
        let data = std::str::from_utf8(data.data.as_slice()).unwrap();

        println!("{:?}", data);
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
            info!("Write completed");
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
