//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

cfg_if! {
    if #[cfg(feature = "python_binding")] {
        use std::time::Duration;
        use pravega_client::byte::{ByteReader, ByteWriter};
        use pyo3::types::PyByteArray;
        use std::io::{ Read, Seek, SeekFrom, Write};
        use pravega_client_shared::ScopedStream;
        use pravega_client::client_factory::ClientFactory;
        use pyo3::exceptions;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use pyo3::PyObjectProtocol;
        use tracing::{trace, info, error};
        use tokio::time::timeout;
    }
}

///
/// This represents a Stream writer for a given Stream.
/// Note: A python object of StreamWriter cannot be created directly without using the StreamManager.
///
#[cfg(feature = "python_binding")]
#[pyclass]
pub(crate) struct ByteStream {
    stream: ScopedStream,
    factory: ClientFactory,
    writer: ByteWriter,
    reader: ByteReader,
}

// The amount of time the python api will wait for the underlying write to be completed.
const TIMEOUT_IN_SECONDS: u64 = 120;

impl ByteStream {
    pub fn new(stream: ScopedStream, factory: ClientFactory) -> Self {
        let byte_writer = factory.create_byte_writer(stream.clone());
        let byte_reader = factory.create_byte_reader(stream.clone());
        ByteStream {
            stream,
            factory,
            writer: byte_writer,
            reader: byte_reader,
        }
    }
}

#[cfg(feature = "python_binding")]
#[pymethods]
impl ByteStream {
    ///
    /// Write a byte array into the Pravega Stream using a byte writer. The data is not persisted
    /// on the server side when this method returns, user should call a flush to ensure all data has
    /// been acknowledged by the server.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // lets assume the Pravega scope and stream are already created.
    /// byte_stream=manager.create_byte_stream("scope", "stream")
    ///
    /// byte_stream.write(b"bytes")
    /// byte_stream.flush();
    /// ```
    ///
    #[text_signature = "($self, byte_array)"]
    #[args(byte_array)]
    pub fn write(&mut self, byte_array: &[u8]) -> PyResult<usize> {
        trace!("Writing a byte array to stream {:?}", self.stream);
        match self.writer.write(byte_array) {
            Ok(bytes_written) => Ok(bytes_written),
            Err(e) => Err(exceptions::PyOSError::new_err(format!(
                "Error while writing into ByteStream {:?}",
                e
            ))),
        }
    }

    ///
    /// This is is used to flush all the data to be persisted on the Pravega server side.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // lets assume the Pravega scope and stream are already created.
    /// byte_stream=manager.create_byte_stream("scope", "stream")
    ///
    /// byte_stream.write(b"bytes")
    /// byte_stream.flush();
    /// ```
    ///
    #[text_signature = "($self)"]
    pub fn flush(&mut self) -> PyResult<()> {
        info!("Flush all data into Pravega Stream {:?}", self.stream);
        match self.writer.flush() {
            Ok(()) => Ok(()),
            Err(e) => Err(exceptions::PyOSError::new_err(format!(
                "Error while flushing data into ByteStream {:?}",
                e
            ))),
        }
    }

    ///
    /// This is used to read bytes from a Pravega Stream into the specified buffer.
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // lets assume the Pravega scope and stream are already created.
    /// byte_stream=manager.create_byte_stream("scope", "stream")
    ///
    /// byte_stream.write(b"bytes")
    /// byte_stream.flush();
    /// buf = bytearray(5)
    /// byte_stream.readinto(buf)
    /// buf
    /// bytearray(b'bytes')
    /// ```
    ///
    #[text_signature = "($self, byteArray)"]
    #[args(byteArray)]
    pub fn readinto(&mut self, buf: &PyByteArray) -> PyResult<usize> {
        trace!("Reading binary data from stream {:?} into buffer", buf);
        let destination_buffer = unsafe { buf.as_bytes_mut() };
        match self.reader.read(destination_buffer) {
            Ok(bytes_read) => Ok(bytes_read),
            Err(e) => Err(exceptions::PyOSError::new_err(format!(
                "Error while reading from ByteStream {:?}",
                e
            ))),
        }
    }

    /// Seek to a position within the stream.
    /// whence follows the same values as IOBase.seek (https://docs.python.org/3/library/io.html#io.IOBase.seek)
    /// where:
    /// ```bash
    /// 0: from start of the stream
    /// 1: from current stream position
    /// 2: from end of the stream
    /// ```
    ///
    pub fn seek(&mut self, position: isize, whence: Option<usize>) -> PyResult<u64> {
        let pos =
            match whence.unwrap_or(0) {
                0 => SeekFrom::Start(position as u64),
                1 => SeekFrom::Current(position as i64),
                2 => SeekFrom::End(position as i64),
                _ => return Err(pyo3::exceptions::PyValueError::new_err(
                    "whence should be one of 0: seek from start, 1: seek from current, or 2: seek from end",
                )),
            };
        match self.reader.seek(pos) {
            Ok(new_start_offset) => Ok(new_start_offset),
            Err(e) => Err(exceptions::PyValueError::new_err(format!(
                "Error while seeking to offset {:?}",
                e
            ))),
        }
    }

    ///
    /// Whether the buffer is seekable; here just for compatibility, it always returns True.
    ///
    pub fn seekable(&self) -> PyResult<bool> {
        Ok(true)
    }

    ///
    /// Get the current read position in the Stream.
    ///
    pub fn tell(&self) -> PyResult<u64> {
        Ok(self.reader.current_offset())
    }

    ///
    /// Get the current head offset of the Stream.
    ///
    pub fn current_head_offset(&self) -> PyResult<u64> {
        let res = self.reader.current_head();
        match res {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::PyOSError::new_err(format!(
                "Error while fetching the head offset of ByteStream {:?}",
                e
            ))),
        }
    }

    ///
    /// Get the current tail offset of the Stream.
    ///
    pub fn current_tail_offset(&self) -> PyResult<u64> {
        let res = self.reader.current_tail();
        match res {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::PyOSError::new_err(format!(
                "Error while fetching the tail offset of ByteStream {:?}",
                e
            ))),
        }
    }

    ///
    /// Truncate Stream at the specified offset. All data before that offset will be truncated
    /// from the Pravega Stream.
    ///
    #[text_signature = "($self, offset)"]
    #[args(offset)]
    pub fn truncate(&mut self, offset: i64) -> PyResult<()> {
        let truncate_future = self.writer.truncate_data_before(offset);

        let _guard = self.factory.runtime().enter();
        let timeout_fut = timeout(Duration::from_secs(TIMEOUT_IN_SECONDS), truncate_future);
        let res = self.factory.runtime().block_on(timeout_fut);
        match res {
            Ok(t) => match t {
                Ok(_) => Ok(()),
                Err(e) => Err(exceptions::PyOSError::new_err(format!(
                    "Error while truncating ByteStream {:?}",
                    e
                ))),
            },
            Err(e) => Err(exceptions::PyOSError::new_err(format!(
                "Truncation timed out, please check connectivity with Pravega. {:?}",
                e
            ))),
        }
    }

    /// Returns the string representation.
    fn to_str(&self) -> String {
        format!("Stream: {:?} ", self.stream)
    }
}

impl Drop for ByteStream {
    fn drop(&mut self) {
        info!("Drop invoked on ByteStream {:?}, invoking flush", self.stream);
        if let Err(e) = self.writer.flush() {
            error!("Error while flushing byteStream {:?}", e);
        }
    }
}

///
/// Refer https://docs.python.org/3/reference/datamodel.html#basic-customization
/// This function will be called by the repr() built-in function to compute the “official” string
/// representation of an Python object.
///
#[cfg(feature = "python_binding")]
#[pyproto]
impl PyObjectProtocol for ByteStream {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("ByteStream({})", self.to_str()))
    }
}
