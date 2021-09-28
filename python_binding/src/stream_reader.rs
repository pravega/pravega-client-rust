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
        use pravega_client_shared::ScopedStream;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use tracing::info;
        use std::sync::Arc;
        use pravega_client::event::reader::{Event, SegmentSlice};
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
    stream: ScopedStream,
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
    /// reader=manager.create_reader("scope", "stream");
    /// slice=await reader.get_segment_slice_async()
    /// for event in slice:
    ///     print(event.data())
    ///```
    ///
    pub fn get_segment_slice_async(&self) -> PyResult<PyObject> {
        // create a python future object.
        let (py_future, py_future_clone, event_loop): (PyObject, PyObject, PyObject) = {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let loop_event = StreamReader::get_loop(py)?;
            let fut: PyObject = loop_event.call_method0(py, "create_future")?;
            (fut.clone_ref(py), fut, loop_event)
        };
        let read = self.reader.clone();
        self.runtime_handle.spawn(async move {
            let slice_result = read.lock().await.acquire_segment().await;
            let slice_py: Slice = Slice {
                seg_slice: slice_result,
            };
            let gil = Python::acquire_gil();
            let py = gil.python();
            let py_container = PyCell::new(py, slice_py).unwrap();
            if let Err(e) = StreamReader::set_fut_result(event_loop, py_future, PyObject::from(py_container))
            {
                let gil = Python::acquire_gil();
                let py = gil.python();
                e.print(py);
            }
        });

        Ok(py_future_clone)
    }

    ///
    /// Set the reader offline.
    ///
    #[pyo3(text_signature = "($self)")]
    pub fn reader_offline(&self) -> PyResult<()> {
        self.runtime_handle.block_on(self.reader_offline_async());
        Ok(())
    }

    ///
    /// Release the segment back.
    ///
    #[pyo3(text_signature = "($self, slice)")]
    pub fn release_segment(&self, slice: &mut Slice) -> PyResult<()> {
        info!("Release segment slice back");
        if let Some(s) = slice.get_set_to_none() {
            self.runtime_handle.block_on(self.release_segment_async(s));
        }
        Ok(())
    }

    /// Returns the string representation.
    fn to_str(&self) -> String {
        format!("Stream: {:?} ", self.stream)
    }
}

impl StreamReader {
    //
    // This is used to set the mark the Python future as complete and set its result.
    // ref: https://docs.python.org/3/library/asyncio-future.html#asyncio.Future.set_result
    //
    fn set_fut_result(event_loop: PyObject, fut: PyObject, res: PyObject) -> PyResult<()> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let sr = fut.getattr(py, "set_result")?;
        // The future is set on the event loop.
        // ref :https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.call_soon_threadsafe
        // call_soon_threadsafe schedules the callback (setting the future to complete) to be called
        // in the next iteration of the event loop.
        event_loop.call_method1(py, "call_soon_threadsafe", (sr, res))?;

        Ok(())
    }

    //
    // Return the running event loop in the current OS thread.
    // https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.get_running_loop
    // This supported in Python 3.7 onwards.
    //
    fn get_loop(py: Python) -> PyResult<PyObject> {
        let asyncio = PyModule::import(py, "asyncio")?;
        let event_loop = asyncio.call0("get_running_loop")?;
        Ok(event_loop.into())
    }

    // Helper method for to set reader_offline.
    async fn reader_offline_async(&self) {
        self.reader.lock().await.reader_offline().await;
    }

    // Helper method for to release segment
    async fn release_segment_async(&self, slice: SegmentSlice) {
        self.reader.lock().await.release_segment(slice).await;
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

#[pymethods]
impl Slice {
    fn __iter__(slf: PyRef<Self>) -> PyResult<Py<Slice>> {
        Ok(slf.into())
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
#[pymethods]
impl StreamReader {
    fn __repr__(&self) -> String {
        format!("StreamReader({})", self.to_str())
    }
}

///
/// Refer https://docs.python.org/3/reference/datamodel.html#basic-customization
/// This function will be called by the repr() built-in function to compute the “official” string
/// representation of an Python object.
///
#[cfg(feature = "python_binding")]
#[pymethods]
impl EventData {
    fn __repr__(&self) -> String {
        format!("EventData({})", self.to_str())
    }
}
