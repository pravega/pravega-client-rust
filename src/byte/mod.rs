//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

//! The Byte API for writing and reading data to a segment in raw bytes.
//!
//! Byte streams are restricted to single-segment streams, but, unlike event streams, there is no event framing,
//! so a reader cannot distinguish separate writes for you, and, if needed, you must do so by
//! convention or by protocol in a layer above the reader.
//!
//! Note that sharing a stream between the byte stream API and the Event readers/writers will
//! CORRUPT YOUR DATA in an unrecoverable way.
///
/// # Examples
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_shared::ScopedSegment;
/// use std::io::{Write, Read};
///
/// #[tokio::main]
/// async fn main() {
///     // assuming Pravega controller is running at endpoint `localhost:9090`.
///     let config = ClientConfigBuilder::default()
///         .controller_uri("localhost:9090")
///         .build()
///         .expect("creating config");
///
///     let client_factory = ClientFactory::new(config);
///
///     // assuming scope:myscope, stream:mystream and segment with id 0 do exist.
///     let segment = ScopedSegment::from("myscope/mystream/0");
///
///     // create writer and write some data.
///     let mut byte_writer = client_factory.create_byte_writer(segment.clone());
///     let payload = "hello world".to_string().into_bytes();
///     byte_writer.write(&payload).expect("write");
///     byte_writer.flush().expect("flush");
///
///     // create reader and read from stream.
///     let mut byte_reader = client_factory.create_byte_reader(segment);
///     let mut buf: Vec<u8> = vec![0; 12];
///     let size = byte_reader.read(&mut buf).expect("read from byte stream");
/// }
/// ```
pub mod reader;
pub mod writer;
