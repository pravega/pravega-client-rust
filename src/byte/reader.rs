//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactory;
use crate::segment::metadata::SegmentMetadataClient;
use crate::segment::reader::PrefetchingAsyncSegmentReader;

use pravega_client_shared::ScopedSegment;

use std::convert::TryInto;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom};
use std::sync::Arc;
use uuid::Uuid;

/// A ByteReader enables reading raw bytes from a segment.
///
/// The ByteReader implements the [Read] and [Seek] trait in standard library.
///
/// Internally ByteReader uses a prefetching reader that prefetches data from the stream in the background.
/// The prefetched data is cached in memory so any sequential read should hit the cache.
/// Any [Seek] operation will invalidate the cache and causes cache miss, so frequent [Seek] and read operation
/// might not have good performance.
///
/// [`Read`]: https://doc.rust-lang.org/std/io/trait.Read.html
/// [`Seek`]: https://doc.rust-lang.org/stable/std/io/trait.Seek.html
///
/// # Examples
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_shared::ScopedSegment;
/// use std::io::Read;
///
/// #[tokio::main]
/// async fn main() {
///     // assuming Pravega controller is running at endpoint `localhost:9090`
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
///     let mut byte_reader = client_factory.create_byte_reader(segment);
///     let mut buf: Vec<u8> = vec![0; 4];
///     let size = byte_reader.read(&mut buf).expect("read from byte stream");
/// }
/// ```
pub struct ByteReader {
    reader_id: Uuid,
    reader: Option<PrefetchingAsyncSegmentReader>,
    reader_buffer_size: usize,
    metadata_client: SegmentMetadataClient,
    factory: ClientFactory,
}

impl Read for ByteReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let result = self
            .factory
            .get_runtime()
            .block_on(self.reader.as_mut().unwrap().read(buf));
        result.map_err(|e| Error::new(ErrorKind::Other, format!("Error: {:?}", e)))
    }
}

impl ByteReader {
    pub fn new(segment: ScopedSegment, factory: ClientFactory, buffer_size: usize) -> Self {
        let async_reader = factory
            .get_runtime()
            .block_on(factory.create_async_event_reader(segment.clone()));
        let async_reader_wrapper = PrefetchingAsyncSegmentReader::new(
            factory.get_runtime().handle().clone(),
            Arc::new(Box::new(async_reader)),
            0,
            buffer_size,
        );
        let metadata_client = factory
            .get_runtime()
            .block_on(factory.create_segment_metadata_client(segment));
        ByteReader {
            reader_id: Uuid::new_v4(),
            reader: Some(async_reader_wrapper),
            reader_buffer_size: buffer_size,
            metadata_client,
            factory,
        }
    }

    /// Returns the head of current readable data in the segment.
    pub fn current_head(&self) -> std::io::Result<u64> {
        self.factory
            .get_runtime()
            .block_on(self.metadata_client.fetch_current_starting_head())
            .map(|i| i as u64)
            .map_err(|e| Error::new(ErrorKind::Other, format!("{:?}", e)))
    }

    /// Returns the read offset.
    pub fn current_offset(&self) -> i64 {
        self.reader.as_ref().unwrap().offset
    }

    /// Returns the bytes that are available to read instantly without fetching from server.
    pub fn available(&self) -> usize {
        self.reader.as_ref().unwrap().available()
    }

    fn recreate_reader_wrapper(&mut self, offset: i64) {
        let internal_reader = self.reader.take().unwrap().extract_reader();
        let new_reader_wrapper = PrefetchingAsyncSegmentReader::new(
            self.factory.get_runtime().handle().clone(),
            internal_reader,
            offset,
            self.reader_buffer_size,
        );
        self.reader = Some(new_reader_wrapper);
    }
}

/// The Seek implementation for ByteStreamReader allows seeking to a byte offset from the beginning
/// of the stream or a byte offset relative to the current position in the stream.
/// If the stream has been truncated, the byte offset will be relative to the original beginning of the stream.
impl Seek for ByteReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(offset) => {
                let offset = offset.try_into().map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        format!("Overflowed when converting offset to i64: {:?}", e),
                    )
                })?;
                self.recreate_reader_wrapper(offset);
                Ok(offset as u64)
            }
            SeekFrom::Current(offset) => {
                let new_offset = self.reader.as_ref().unwrap().offset + offset;
                if new_offset < 0 {
                    Err(Error::new(
                        ErrorKind::InvalidInput,
                        "Cannot seek to a negative offset",
                    ))
                } else {
                    self.recreate_reader_wrapper(new_offset);
                    Ok(new_offset as u64)
                }
            }
            SeekFrom::End(offset) => {
                let tail = self
                    .factory
                    .get_runtime()
                    .block_on(self.metadata_client.fetch_current_segment_length())
                    .map_err(|e| Error::new(ErrorKind::Other, format!("{:?}", e)))?;
                if tail + offset < 0 {
                    Err(Error::new(
                        ErrorKind::InvalidInput,
                        "Cannot seek to a negative offset",
                    ))
                } else {
                    let new_offset = tail + offset;
                    self.recreate_reader_wrapper(new_offset);
                    Ok(new_offset as u64)
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::byte::writer::ByteWriter;
    use crate::util::create_stream;
    use pravega_client_config::connection_type::{ConnectionType, MockType};
    use pravega_client_config::ClientConfigBuilder;
    use pravega_client_shared::PravegaNodeUri;
    use std::io::Write;
    use tokio::runtime::Runtime;

    #[test]
    fn test_byte_stream_seek() {
        let rt = Runtime::new().unwrap();
        let (mut writer, mut reader) = create_reader_and_writer(&rt);

        // write 200 bytes
        let payload = vec![1; 200];
        writer.write(&payload).expect("write");
        writer.flush().expect("flush");

        // read 200 bytes from beginning
        let mut buf = vec![0; 200];
        let mut read = 0;
        while read != 200 {
            let r = reader.read(&mut buf).expect("read");
            read += r;
        }
        assert_eq!(read, 200);
        assert_eq!(buf, vec![1; 200]);

        // seek to head
        reader.seek(SeekFrom::Start(0)).expect("seek to head");
        assert_eq!(reader.current_offset(), 0);

        // seek to head with positive offset
        reader.seek(SeekFrom::Start(100)).expect("seek to head");
        assert_eq!(reader.current_offset(), 100);

        // seek to current with positive offset
        assert_eq!(reader.current_offset(), 100);
        reader.seek(SeekFrom::Current(100)).expect("seek to current");
        assert_eq!(reader.current_offset(), 200);

        // seek to current with negative offset
        reader.seek(SeekFrom::Current(-100)).expect("seek to current");
        assert_eq!(reader.current_offset(), 100);

        // seek to current invalid negative offset
        assert!(reader.seek(SeekFrom::Current(-200)).is_err());

        // seek to end
        reader.seek(SeekFrom::End(0)).expect("seek to end");
        assert_eq!(reader.current_offset(), 200);

        // seek to end with positive offset
        assert!(reader.seek(SeekFrom::End(1)).is_ok());

        // seek to end with negative offset
        reader.seek(SeekFrom::End(-100)).expect("seek to end");
        assert_eq!(reader.current_offset(), 100);

        // seek to end with invalid negative offset
        assert!(reader.seek(SeekFrom::End(-300)).is_err());
    }

    #[test]
    fn test_byte_stream_truncate() {
        let rt = Runtime::new().unwrap();
        let (mut writer, mut reader) = create_reader_and_writer(&rt);

        // write 200 bytes
        let payload = vec![1; 200];
        writer.write(&payload).expect("write");
        writer.flush().expect("flush");

        // truncate to offset 100
        rt.block_on(writer.truncate_data_before(100)).expect("truncate");

        // read truncated offset
        reader.seek(SeekFrom::Start(0)).expect("seek to head");
        let mut buf = vec![0; 100];
        assert!(reader.read(&mut buf).is_err());

        // read from current head
        let offset = reader.current_head().expect("get current head");
        reader.seek(SeekFrom::Start(offset)).expect("seek to new head");
        let mut buf = vec![0; 100];
        assert!(reader.read(&mut buf).is_ok());
        assert_eq!(buf, vec![1; 100]);
    }

    #[test]
    fn test_byte_stream_seal() {
        let rt = Runtime::new().unwrap();
        let (mut writer, mut reader) = create_reader_and_writer(&rt);

        // write 200 bytes
        let payload = vec![1; 200];
        writer.write(&payload).expect("write");
        writer.flush().expect("flush");

        // seal the segment
        rt.block_on(writer.seal()).expect("seal");

        // read sealed stream
        reader.seek(SeekFrom::Start(0)).expect("seek to new head");
        let mut buf = vec![0; 200];
        assert!(reader.read(&mut buf).is_ok());
        assert_eq!(buf, vec![1; 200]);

        let payload = vec![1; 200];
        let write_result = writer.write(&payload);
        let flush_result = writer.flush();
        assert!(write_result.is_err() || flush_result.is_err());
    }

    fn create_reader_and_writer(runtime: &Runtime) -> (ByteWriter, ByteReader) {
        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(MockType::Happy))
            .mock(true)
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091".to_string()))
            .build()
            .unwrap();
        let factory = ClientFactory::new(config);
        runtime.block_on(create_stream(&factory, "testScope", "testStream"));
        let segment = ScopedSegment::from("testScope/testStream/0.#epoch.0");
        let writer = factory.create_byte_writer(segment.clone());
        let reader = factory.create_byte_reader(segment);
        (writer, reader)
    }
}
