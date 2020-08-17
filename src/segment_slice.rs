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
use async_recursion::async_recursion;
use bytes::{Buf, BufMut, BytesMut};
use pravega_rust_client_shared::ScopedSegment;
use pravega_wire_protocol::commands::{Command, EventCommand};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, warn};

#[derive(Debug)]
pub struct Event {
    offset_in_segment: i64,
    value: Vec<u8>,
}

pub struct SegmentSlice {
    start_offset: i64,
    segment: ScopedSegment,
    read_offset: i64,
    end_offset: i64,
    event_rx: Receiver<BytePlaceholder>,
    segment_data: BytePlaceholder,
    handle: Handle,
    partial_event: BytePlaceholder,
}

enum ReadType {
    Event(Event),
    PartialEvent(),
}

///
/// Amount of Bytes Read ahead.
///
const READ_BUFFER_SIZE: i32 = 2048;

// Structure to track the offset and byte array.
#[derive(Debug)]
struct BytePlaceholder {
    offset_in_segment: i64,
    value: BytesMut,
}

impl BytePlaceholder {
    fn split(&mut self) -> BytePlaceholder {
        let res = self.value.split();
        let old_offset = self.offset_in_segment;
        let new_offset = old_offset + res.len() as i64;
        self.offset_in_segment = new_offset;
        BytePlaceholder {
            offset_in_segment: self.offset_in_segment,
            value: res,
        }
    }

    fn split_to(&mut self, at: usize) -> BytePlaceholder {
        let old_offset = self.offset_in_segment;
        let res = self.value.split_to(at);
        self.offset_in_segment = old_offset + at as i64;

        BytePlaceholder {
            offset_in_segment: old_offset,
            value: res,
        }
    }

    fn get_i32(&mut self) -> i32 {
        let result = self.value.get_i32();
        self.offset_in_segment += 4;
        result
    }

    fn empty() -> BytePlaceholder {
        BytePlaceholder {
            offset_in_segment: 0,
            value: BytesMut::with_capacity(0),
        }
    }
}

impl SegmentSlice {
    pub(crate) fn new(
        segment: ScopedSegment,
        start_offset: i64,
        factory: Arc<ClientFactoryInternal>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let handle = factory.get_runtime_handle();
        handle.enter(|| {
            tokio::spawn(SegmentSlice::get_segment_data(
                segment.clone(),
                start_offset,
                tx,
                factory.clone(),
            ))
        });
        SegmentSlice {
            start_offset,
            segment,
            read_offset: 0,
            end_offset: i64::MAX,
            event_rx: rx,
            segment_data: BytePlaceholder::empty(),
            handle,
            partial_event: BytePlaceholder::empty(),
        }
    }

    ///
    /// Method to fetch segment data in a loop
    ///
    async fn get_segment_data(
        segment: ScopedSegment,
        start_offset: i64,
        mut tx: Sender<BytePlaceholder>,
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
                        let data = BytePlaceholder {
                            offset_in_segment: offset,
                            value: segment_data,
                        };
                        // send data: this waits until there is capacity in the channel.
                        if let Err(e) = tx.send(data).await {
                            info!("Error while sending segment data to event parser {:?} ", e);
                            break;
                        }
                        offset += len as i64;
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
    /// Extract event from the data received from the Segment store. Return None in case of a Partial event.
    ///
    fn extract_event(&mut self, parse_header: fn(&mut BytePlaceholder) -> BytePlaceholder) -> Option<Event> {
        if self.partial_event.value.capacity() != 0 {
            let bytes_to_read = self.partial_event.value.capacity() - self.partial_event.value.len();
            if self.segment_data.value.remaining() >= bytes_to_read {
                let t = self.segment_data.split_to(bytes_to_read);
                self.partial_event.value.put(t.value);
                let event = Event {
                    offset_in_segment: self.partial_event.offset_in_segment,
                    value: self.partial_event.value.to_vec(),
                };
                self.partial_event = BytePlaceholder::empty();
                Some(event)
            } else {
                debug!("Partial event being returned since there is more to fill");
                self.partial_event.value.put(self.segment_data.value.split());
                None
            }
        } else {
            let mut event_data = parse_header(&mut self.segment_data);
            let bytes_to_read = event_data.value.capacity();
            if bytes_to_read == 0 {
                warn!("Found a header with length as zero");
                return None;
            }
            if self.segment_data.value.remaining() >= bytes_to_read {
                let t = self.segment_data.split_to(bytes_to_read);
                event_data.value.put(t.value);
                info!(
                    "Event data is {:?} with length {}",
                    event_data,
                    event_data.value.len()
                );
                //Convert to Event and send it.
                let event = Event {
                    offset_in_segment: event_data.offset_in_segment,
                    value: event_data.value.freeze().to_vec(),
                };
                Some(event)
            } else {
                let t = self.segment_data.split();
                event_data.value.put(t.value);
                debug!(
                    "Partial Event read: Current data read {:?} data_read {} to_read {}",
                    event_data,
                    event_data.value.len(),
                    event_data.value.capacity()
                );
                self.partial_event = event_data;
                None
            }
        }
    }

    ///
    /// This method reads the header and returns a BytesMut whose size is as big as the event.
    ///
    fn read_header(mut data: &mut BytePlaceholder) -> BytePlaceholder {
        let event_offset = data.offset_in_segment;
        let type_code = data.get_i32();
        assert_eq!(type_code, EventCommand::TYPE_CODE);
        let len = data.get_i32();
        data.offset_in_segment += 8; //increase the offset in segment by
        debug!("Event size is {}", len);
        BytePlaceholder {
            offset_in_segment: event_offset,
            value: BytesMut::with_capacity(len as usize),
        }
    }

    async fn read_next_event(
        &mut self,
        parse_header: fn(&mut BytePlaceholder) -> BytePlaceholder,
    ) -> Option<Event> {
        if self.segment_data.value.is_empty() {
            if let Some(response) = self.event_rx.recv().await {
                self.segment_data = response;
                self.extract_event(parse_header)
            } else {
                None
            }
        } else {
            self.extract_event(parse_header)
        }
    }

    #[async_recursion]
    async fn fetch_next_event(
        &mut self,
        parse_header: fn(&mut BytePlaceholder) -> BytePlaceholder,
    ) -> Option<Event> {
        if self.segment_data.value.is_empty() {
            if let Some(response) = self.event_rx.recv().await {
                self.segment_data = response;
            } else {
                return None;
            }
        }
        let res = self.extract_event(parse_header);
        match res {
            Some(d) => Some(d),
            None => {
                info!("None was returned by the extract_event method");
                self.fetch_next_event(parse_header).await
            }
        }
    }
}

///
/// Iterator implementation of SegmentSlice.
///
impl Iterator for SegmentSlice {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        // Wait for a response from the segment store only if we do not have pre-fetched data or in case
        // of a partial event.

        if self.segment_data.value.is_empty() {
            if let Some(response) = self.handle.block_on(self.event_rx.recv()) {
                self.segment_data = response;
            } else {
                return None;
            }
        }
        let res = self.extract_event(SegmentSlice::read_header);
        match res {
            Some(d) => Some(d),
            None => {
                info!("Partial event read by the extract_event method, invoke read again to fetch the complete event.");
                self.next()
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::client_factory::ClientFactory;
    use bytes::{Buf, BufMut, BytesMut};
    use pravega_controller_client::ControllerClient;
    use pravega_rust_client_shared::{
        Retention, RetentionType, ScaleType, Scaling, Scope, ScopedStream, Segment, Stream,
        StreamConfiguration,
    };
    use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::{Receiver, Sender};

    ///
    /// This method reads the header and returns a BytesMut whose size is as big as the event.
    ///
    fn custom_read_header(data: &mut BytePlaceholder) -> BytePlaceholder {
        if data.value.remaining() >= 4 {
            let len = data.get_i32();
            println!("Header length read is {}", len);
            BytePlaceholder {
                offset_in_segment: 0,
                value: BytesMut::with_capacity(len as usize),
            }
        } else {
            BytePlaceholder {
                offset_in_segment: 0,
                value: BytesMut::with_capacity(0),
            }
        }
    }

    #[tokio::test]
    async fn test_read_partial_events() {
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(generate_multiple_constant_size_events(tx));
        let mut segment_slice = create_segment_slice(rx);

        let mut event_size = 0;
        while let Some(event) = segment_slice.fetch_next_event(custom_read_header).await {
            assert!(is_all_same12(event.value.as_slice()));
            assert_eq!(event_size + 1, event.value.len(), "Event was missed out");
            event_size = event_size + 1;
        }
    }

    #[tokio::test]
    async fn test_read_events() {
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(generate_multiple_variable_sized_events(tx));
        let mut segment_slice = create_segment_slice(rx);
        let mut event_size = 0;
        while let Some(event) = segment_slice.fetch_next_event(custom_read_header).await {
            assert!(is_all_same12(event.value.as_slice()));
            assert_eq!(event_size + 1, event.value.len(), "Event was missed out");
            event_size = event_size + 1;
        }
        assert_eq!(event_size, 10);
    }

    fn create_segment_slice(rx: Receiver<BytePlaceholder>) -> SegmentSlice {
        let segment_slice = SegmentSlice {
            start_offset: 0,
            segment: ScopedSegment {
                scope: Scope { name: "scope".into() },
                stream: Stream {
                    name: "stream".into(),
                },
                segment: Segment {
                    number: 0,
                    tx_id: None,
                },
            },
            read_offset: 0,
            end_offset: i64::MAX,
            event_rx: rx,
            segment_data: BytePlaceholder::empty(),
            handle: Handle::current(),
            partial_event: BytePlaceholder::empty(),
        };
        segment_slice
    }

    fn is_all_same12<T: Eq>(slice: &[T]) -> bool {
        slice
            .get(0)
            .map(|first| slice.iter().all(|x| x == first))
            .unwrap_or(true)
    }

    async fn generate_multiple_constant_size_events(mut tx: Sender<BytePlaceholder>) {
        let mut buf = BytesMut::with_capacity(10);

        buf.put_i32(1);
        buf.put_u8(b'a');
        buf.put_i32(2);
        buf.put(&b"aa"[..]);
        tx.send(BytePlaceholder {
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(3);
        buf.put(&b"aaa"[..]);
        tx.send(BytePlaceholder {
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(4);
        buf.put(&b"aaaa"[..]);
        tx.send(BytePlaceholder {
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(5);
        buf.put(&b"aaaaa"[..]);
        tx.send(BytePlaceholder {
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(6);
        buf.put(&b"aaaaaa"[..]);
        tx.send(BytePlaceholder {
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(7);
        buf.put(&b"aaaaaa"[..]);
        tx.send(BytePlaceholder {
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_u8(b'a');
        buf.put_i32(8);
        buf.put(&b"aaaaa"[..]);
        tx.send(BytePlaceholder {
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put(&b"aaa"[..]);
        tx.send(BytePlaceholder {
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();
    }

    ///
    /// This is a test function to generate single events of varying sizes
    ///
    async fn generate_multiple_variable_sized_events(mut tx: Sender<BytePlaceholder>) {
        for i in 1..11 {
            let mut buf = BytesMut::with_capacity(32);
            buf.put_i32(i); // length.
            for _ in 0..i {
                buf.put(&b"a"[..]);
            }
            if let Err(_) = tx
                .send(BytePlaceholder {
                    offset_in_segment: 0,
                    value: buf,
                })
                .await
            {
                println!("==> receiver dropped");
                return;
            }
        }
    }

    // Integration test.
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
            .block_on(client_factory.get_controller_client().seal_stream(&str))
            .unwrap();
        let scoped_segment = ScopedSegment {
            scope: scope_name,
            stream: stream_name,
            segment: Segment {
                number: 0,
                tx_id: None,
            },
        };
        let mut slice = SegmentSlice::new(scoped_segment, 0, client_factory.0.clone());
        while let Some(d) = slice.next() {
            println!("{:?}", d);
        }
        client_factory
            .get_runtime_handle()
            .block_on(slice.event_rx.recv());

        // let s = client_factory
        //     .get_runtime_handle()
        //     .block_on(slice.event_rx.recv());
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
            handle.block_on(rx1).unwrap().expect("Failed to write Event");
            handle.block_on(rx2).unwrap().expect("Failed to write Event");
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
