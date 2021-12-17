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
        use pravega_client::event::reader::EventReader;
        use pravega_client::event::reader::EventReaderError;
        use pravega_client_shared::ScopedStream;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use pyo3::PyObjectProtocol;
        use pyo3::exceptions;
        use tracing::info;
        use std::sync::Arc;
        use pravega_client::event::reader::{Event, SegmentSlice};
        use pyo3::PyIterProtocol;
        use tokio::sync::Mutex;
        use tokio::runtime::Handle;
    }
}

///
/// This represents a Stream reader for a given Stream.
/// Note: A python object of StreamReader cannot be created directly without using the StreamManager.
///
#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)]
pub(crate) struct StreamReader {
    reader: Arc<Mutex<EventReader>>,
    runtime_handle: Handle,
    streams: Vec<ScopedStream>,
}

#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamReader {
    ///
    /// This method returns a Python Future which completes when a segment slice is acquired for consumption.
    /// A segment slice is data chunk received from a segment of a Pravega stream. It can contain one
    /// or more events and the user can iterate over the segment slice to read the events.
    /// If there are multiple segments in the stream then this API can return a segment slice of any
    /// segments in the stream. The reader ensures that events returned by the stream are in order.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // lets assume the Pravega scope and stream are already created.
    /// reader_group=manager.create_reader_group("rg1", "scope", "stream")
    /// reader=reader_group.create_reader("reader-1");
    /// slice=await reader.get_segment_slice_async()
    /// for event in slice:
    ///     print(event.data())
    ///```
    ///
    pub fn get_segment_slice_async<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let read = self.reader.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let slice_result = read.lock().await.acquire_segment().await;
            match slice_result {
                Ok(slice) => {
                    let slice_py: Slice = Slice { seg_slice: slice };
                    Ok(Python::with_gil(|py| slice_py.into_py(py)))
                }
                Err(e) => Err(exceptions::PyOSError::new_err(format!(
                    "Error while attempting to acquire segment {:?}",
                    e
                ))),
            }
        })
    }

    ///
    /// Set the reader offline.
    ///
    #[pyo3(text_signature = "($self)")]
    pub fn reader_offline(&self) -> PyResult<()> {
        match self.runtime_handle.block_on(self.reader_offline_async()) {
            Ok(_) => Ok(()),
            Err(e) => Err(exceptions::PyOSError::new_err(format!(
                "Error while attempting to acquire segment {:?}",
                e
            ))),
        }
    }

    ///
    /// Release the segment back.
    ///
    #[pyo3(text_signature = "($self, slice)")]
    pub fn release_segment(&self, slice: &mut Slice) -> PyResult<()> {
        info!("Release segment slice back");
        if let Some(s) = slice.get_set_to_none() {
            self.runtime_handle
                .block_on(self.release_segment_async(s))
                .map_err(|e| {
                    exceptions::PyOSError::new_err(format!(
                        "Error while attempting to acquire segment {:?}",
                        e
                    ))
                })?;
        }
        Ok(())
    }

    /// Returns the string representation.
    fn to_str(&self) -> String {
        format!("Streams: {:?} ", self.streams)
    }
}

impl StreamReader {
    // Helper method for to set reader_offline.
    async fn reader_offline_async(&self) -> Result<(), EventReaderError> {
        self.reader.lock().await.reader_offline().await
    }

    // Helper method for to release segment
    async fn release_segment_async(&self, slice: SegmentSlice) -> Result<(), EventReaderError> {
        self.reader.lock().await.release_segment(slice).await
    }
}

///
/// This represents a Stream reader for a given Stream.
/// Note: A python object of StreamReader cannot be created directly without using the StreamManager.
///
#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)]
pub(crate) struct EventData {
    offset_in_segment: i64,
    value: Vec<u8>,
}

#[cfg(feature = "python_binding")]
#[pymethods]
impl EventData {
    ///Return the data
    fn data(&self) -> &[u8] {
        self.value.as_slice()
    }
    /// Returns the string representation.
    fn to_str(&self) -> String {
        format!("offset {:?} data :{:?}", self.offset_in_segment, self.value)
    }
}

///
/// This represents a Stream reader for a given Stream.
/// Note: A python object of StreamReader cannot be created directly without using the StreamManager.
///
#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)]
pub(crate) struct Slice {
    seg_slice: Option<SegmentSlice>,
}

impl Slice {
    fn get_set_to_none(&mut self) -> Option<SegmentSlice> {
        self.seg_slice.take()
    }
}

#[pyproto]
impl PyIterProtocol for Slice {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<EventData> {
        if let Some(mut slice) = slf.seg_slice.take() {
            let next_event: Option<Event> = slice.next();
            slf.seg_slice = Some(slice);
            next_event.map(|e| EventData {
                offset_in_segment: e.offset_in_segment,
                value: e.value,
            })
        } else {
            info!("Empty Slice");
            None
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
impl PyObjectProtocol for StreamReader {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("StreamReader({})", self.to_str()))
    }
}

///
/// Refer https://docs.python.org/3/reference/datamodel.html#basic-customization
/// This function will be called by the repr() built-in function to compute the “official” string
/// representation of an Python object.
///
#[cfg(feature = "python_binding")]
#[pyproto]
impl PyObjectProtocol for EventData {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("EventData({})", self.to_str()))
    }
}
