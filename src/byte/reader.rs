//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryAsync;
use crate::segment::metadata::SegmentMetadataClient;
use crate::segment::reader::PrefetchingAsyncSegmentReader;

use pravega_client_shared::{ScopedSegment, ScopedStream};

use std::convert::TryInto;
use std::io::{Error, ErrorKind, SeekFrom};
use std::sync::Arc;
use uuid::Uuid;

/// A ByteReader enables reading raw bytes from a segment.
///
/// The ByteReader provides an API similar to the [`Read`] and [`Seek`] traits in the standard library,
/// but where the methods are asynchronous.
///
/// Internally ByteReader uses a prefetching reader that prefetches data from the server in the background.
/// The prefetched data is cached in memory so any sequential reads should be able to hit the cache.
///
/// Any seek operation will invalidate the cache and causes cache miss, so frequent seek and read operations
/// might not have good performance.
///
/// You can also wrap ByteReader with [`BufReader`], but doing so will not increase performance further.
///
/// [`Read`]: https://doc.rust-lang.org/std/io/trait.Read.html
/// [`Seek`]: https://doc.rust-lang.org/stable/std/io/trait.Seek.html
/// [`BufReader`]: https://doc.rust-lang.org/std/io/struct.BufReader.html
///
/// # Examples
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactoryAsync;
/// use pravega_client_shared::ScopedStream;
/// use tokio::runtime::Handle;
///
/// #[tokio::main]
/// async fn main() {
///     // assuming Pravega controller is running at endpoint `localhost:9090`
///     let config = ClientConfigBuilder::default()
///         .controller_uri("localhost:9090")
///         .build()
///         .expect("creating config");
///
///     let handle = Handle::current();
///     let client_factory = ClientFactoryAsync::new(config, handle);
///
///     // assuming scope:myscope, stream:mystream exist.
///     // notice that this stream should be a fixed sized single segment stream
///     let stream = ScopedStream::from("myscope/mystream");
///
///     let mut byte_reader = client_factory.create_byte_reader(stream).await;
///     let mut buf: Vec<u8> = vec![0; 4];
///     let size = byte_reader.read(&mut buf).await.expect("read from byte stream");
/// }
/// ```
pub struct ByteReader {
    reader_id: Uuid,
    pub segment: ScopedSegment,
    reader: Option<PrefetchingAsyncSegmentReader>,
    reader_buffer_size: usize,
    metadata_client: SegmentMetadataClient,
    factory: ClientFactoryAsync,
}

impl ByteReader {
    pub(crate) async fn new(stream: ScopedStream, factory: ClientFactoryAsync, buffer_size: usize) -> Self {
        let segments = factory
            .controller_client()
            .get_head_segments(&stream)
            .await
            .expect("get head segments");
        assert_eq!(
            segments.len(),
            1,
            "Byte stream is configured with more than one segment"
        );
        let segment = segments.iter().next().unwrap().0.clone();
        let scoped_segment = ScopedSegment {
            scope: stream.scope.clone(),
            stream: stream.stream.clone(),
            segment,
        };
        let async_reader = factory.create_async_segment_reader(scoped_segment.clone()).await;
        let async_reader_wrapper = PrefetchingAsyncSegmentReader::new(
            factory.runtime_handle(),
            Arc::new(Box::new(async_reader)),
            0,
            buffer_size,
        );
        let metadata_client = factory
            .create_segment_metadata_client(scoped_segment.clone())
            .await;
        ByteReader {
            reader_id: Uuid::new_v4(),
            segment: scoped_segment,
            reader: Some(async_reader_wrapper),
            reader_buffer_size: buffer_size,
            metadata_client,
            factory,
        }
    }

    /// Read data asynchronously.
    ///
    /// ```ignore
    /// let mut byte_reader = client_factory.create_byte_reader(segment).await;
    /// let mut buf: Vec<u8> = vec![0; 4];
    /// let size = byte_reader.read(&mut buf).expect("read");
    /// ```
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.reader
            .as_mut()
            .unwrap()
            .read(buf)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("Error: {:?}", e)))
    }

    /// Return the head of current readable data in the segment asynchronously.
    ///
    /// The ByteReader is initialized to read from the segment at offset 0. However, it might
    /// encounter the SegmentIsTruncated error due to the segment has been truncated. In this case,
    /// application should call this method to get the current readable head and read from it.
    /// ```ignore
    /// let mut byte_reader = client_factory.create_byte_reader_async(segment).await;
    /// let offset = byte_reader.current_head().await.expect("get current head offset");
    /// ```
    pub async fn current_head(&self) -> std::io::Result<u64> {
        self.metadata_client
            .fetch_current_starting_head()
            .await
            .map(|i| i as u64)
            .map_err(|e| Error::new(ErrorKind::Other, format!("{:?}", e)))
    }

    /// Return the tail offset of the segment asynchronously.
    ///
    /// ```ignore
    /// let mut byte_reader = client_factory.create_byte_reader_async(segment).await;
    /// let offset = byte_reader.current_tail().await.expect("get current tail offset");
    /// ```
    pub async fn current_tail(&self) -> std::io::Result<u64> {
        self.metadata_client
            .fetch_current_segment_length()
            .await
            .map(|i| i as u64)
            .map_err(|e| Error::new(ErrorKind::Other, format!("{:?}", e)))
    }

    /// Return the current read offset.
    ///
    /// ```ignore
    /// let mut byte_reader = client_factory.create_byte_reader(segment);
    /// let offset = byte_reader.current_offset();
    /// ```
    pub fn current_offset(&self) -> u64 {
        self.reader.as_ref().unwrap().offset as u64
    }

    /// Return the bytes that are available to read instantly without fetching from server.
    ///
    /// ByteReader has a buffer internally. This method returns the size of remaining data in that buffer.
    /// ```ignore
    /// let mut byte_reader = client_factory.create_byte_reader(segment);
    /// let size = byte_reader.available();
    /// ```
    pub fn available(&self) -> usize {
        self.reader.as_ref().unwrap().available()
    }

    /// The seek method for ByteReader allows seeking to a byte offset from the beginning
    /// of the stream or a byte offset relative to the current position in the stream.
    /// If the stream has been truncated, the byte offset will be relative to the original beginning of the stream.
    pub async fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
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
                    .metadata_client
                    .fetch_current_segment_length()
                    .await
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

    fn recreate_reader_wrapper(&mut self, offset: i64) {
        let internal_reader = self.reader.take().unwrap().extract_reader();
        let new_reader_wrapper = PrefetchingAsyncSegmentReader::new(
            self.factory.runtime_handle(),
            internal_reader,
            offset,
            self.reader_buffer_size,
        );
        self.reader = Some(new_reader_wrapper);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::byte::writer::ByteWriter;
    use crate::client_factory::ClientFactory;
    use crate::util::create_stream;
    use pravega_client_config::connection_type::{ConnectionType, MockType};
    use pravega_client_config::ClientConfigBuilder;
    use pravega_client_shared::PravegaNodeUri;
    use tokio::runtime::Runtime;

    #[test]
    fn test_byte_seek() {
        let (mut writer, mut reader, factory) = create_reader_and_writer(Runtime::new().unwrap());
        let rt = factory.runtime();
        // write 200 bytes
        let payload = vec![1; 200];
        rt.block_on(writer.write(&payload)).expect("write");
        rt.block_on(writer.flush()).expect("flush");

        // read 200 bytes from beginning
        let mut buf = vec![0; 200];
        let mut read = 0;
        while read != 200 {
            let r = rt.block_on(reader.read(&mut buf)).expect("read");
            read += r;
        }
        assert_eq!(read, 200);
        assert_eq!(buf, vec![1; 200]);
        // seek to head
        rt.block_on(reader.seek(SeekFrom::Start(0)))
            .expect("seek to head");
        assert_eq!(reader.current_offset(), 0);

        // seek to head with positive offset
        rt.block_on(reader.seek(SeekFrom::Start(100)))
            .expect("seek to head");
        assert_eq!(reader.current_offset(), 100);

        // seek to current with positive offset
        assert_eq!(reader.current_offset(), 100);
        rt.block_on(reader.seek(SeekFrom::Current(100)))
            .expect("seek to current");
        assert_eq!(reader.current_offset(), 200);

        // seek to current with negative offset
        rt.block_on(reader.seek(SeekFrom::Current(-100)))
            .expect("seek to current");
        assert_eq!(reader.current_offset(), 100);

        // seek to current invalid negative offset
        assert!(rt.block_on(reader.seek(SeekFrom::Current(-200))).is_err());

        // seek to end
        rt.block_on(reader.seek(SeekFrom::End(0))).expect("seek to end");
        assert_eq!(reader.current_offset(), 200);

        // seek to end with positive offset
        assert!(rt.block_on(reader.seek(SeekFrom::End(1))).is_ok());

        // seek to end with negative offset
        rt.block_on(reader.seek(SeekFrom::End(-100)))
            .expect("seek to end");
        assert_eq!(reader.current_offset(), 100);

        // seek to end with invalid negative offset
        assert!(rt.block_on(reader.seek(SeekFrom::End(-300))).is_err());
    }

    #[test]
    fn test_byte_stream_truncate() {
        let (mut writer, mut reader, factory) = create_reader_and_writer(Runtime::new().unwrap());
        let rt = factory.runtime();
        // write 200 bytes
        let payload = vec![1; 200];
        rt.block_on(writer.write(&payload)).expect("write");
        rt.block_on(writer.flush()).expect("flush");

        // truncate to offset 100
        rt.block_on(writer.truncate_data_before(100)).expect("truncate");

        // read truncated offset
        rt.block_on(reader.seek(SeekFrom::Start(0)))
            .expect("seek to head");
        let mut buf = vec![0; 100];
        assert!(rt.block_on(reader.read(&mut buf)).is_err());

        // read from current head
        let offset = rt.block_on(reader.current_head()).expect("get current head");
        rt.block_on(reader.seek(SeekFrom::Start(offset)))
            .expect("seek to new head");
        let mut buf = vec![0; 100];
        assert!(rt.block_on(reader.read(&mut buf)).is_ok());
        assert_eq!(buf, vec![1; 100]);
    }

    #[test]
    fn test_byte_stream_seal() {
        const BYTE_SIZE: usize = 200;

        let (mut writer, mut reader, factory) = create_reader_and_writer(Runtime::new().unwrap());
        let rt = factory.runtime();

        // write 200 bytes
        let payload = vec![1; BYTE_SIZE];
        rt.block_on(writer.write(&payload)).expect("write");
        rt.block_on(writer.flush()).expect("flush");

        // seal the segment
        rt.block_on(writer.seal()).expect("seal");

        // read sealed stream
        rt.block_on(reader.seek(SeekFrom::Start(0)))
            .expect("seek to new head");
        let mut buf = vec![0; BYTE_SIZE];
        assert!(rt.block_on(reader.read(&mut buf)).is_ok());
        assert_eq!(buf, vec![1; BYTE_SIZE]);

        let payload = vec![1; BYTE_SIZE];
        let write_result = rt.block_on(writer.write(&payload));
        let flush_result = rt.block_on(writer.flush());
        assert!(write_result.is_err() || flush_result.is_err());
    }

    #[test]
    #[should_panic(expected = "Byte stream is configured with more than one segment")]
    fn test_invalid_stream_config() {
        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(MockType::Happy))
            .mock(true)
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091".to_string()))
            .build()
            .unwrap();
        let factory = ClientFactory::new(config);
        factory.runtime().block_on(create_stream(
            &factory,
            "testScopeInvalid",
            "testStreamInvalid",
            2,
        ));
        let stream = ScopedStream::from("testScopeInvalid/testStreamInvalid");
        factory.runtime().block_on(factory.create_byte_reader(stream));
    }

    fn create_reader_and_writer(runtime: Runtime) -> (ByteWriter, ByteReader, ClientFactory) {
        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(MockType::Happy))
            .mock(true)
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091".to_string()))
            .build()
            .unwrap();
        let factory = ClientFactory::new_with_runtime(config, runtime);
        factory
            .runtime()
            .block_on(create_stream(&factory, "testScope", "testStream", 1));
        let stream = ScopedStream::from("testScope/testStream");
        let writer = factory
            .runtime()
            .block_on(factory.create_byte_writer(stream.clone()));
        let reader = factory.runtime().block_on(factory.create_byte_reader(stream));
        (writer, reader, factory)
    }
}
