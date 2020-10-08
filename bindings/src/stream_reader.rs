//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_client_rust::segment_slice::{Event, SegmentSlice};
use pyo3::PyIterProtocol;
cfg_if! {
    if #[cfg(feature = "python_binding")] {
        use pravega_client_rust::event_reader::EventReader;
        use pravega_rust_client_shared::ScopedStream;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use pyo3::PyObjectProtocol;
        use tokio::runtime::Handle;
        use log::info;
        use pyo3::types::PyString;
        use std::sync::Arc;
    }
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)]
///
/// This represents a Stream reader for a given Stream.
/// Note: A python object of StreamReader cannot be created directly without using the StreamManager.
///
pub(crate) struct StreamReader {
    reader: Arc<EventReader>,
    handle: Handle,
    stream: ScopedStream,
}

#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamReader {
    ///
    /// Return a Python Future which completes when a segment slice is acquired.
    ///
    pub fn get_segment_slice_async(&self) -> PyResult<PyObject> {
        // create a python future object.
        let (py_future, py_future_clone, event_loop): (PyObject, PyObject, PyObject) = {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let loop_ = StreamReader::get_loop(py)?;
            let fut: PyObject = loop_.call_method0(py, "create_future")?.into();
            (fut.clone_ref(py), fut, loop_.into())
        };
        let read = self.reader.clone();
        self.handle.spawn(async move {
            let slice = read.acquire_segment_test().await;
            if let Some(slice) = slice {
                info!("Segment slice acquired {:?}", slice);
                let gil = Python::acquire_gil();
                let py = gil.python();
                let r = PyString::new(py, slice.as_str());
                if let Err(e) = StreamReader::set_fut_result(event_loop, py_future, PyObject::from(r)) {
                    let gil = Python::acquire_gil();
                    let py = gil.python();
                    e.print(py);
                }
            } else {
                info!("No new slices to be acquired");
            }
        });

        Ok(py_future_clone)
    }

    /// Returns the facet string representation.
    fn to_str(&self) -> String {
        format!("Stream: {:?} ", self.stream)
    }
}

impl StreamReader {
    //
    // This is used to set the mark the Python future as complete and set its result.
    // ref: https://docs.python.org/3/library/asyncio-future.html#asyncio.Future.set_result
    //
    fn set_fut_result(loop_: PyObject, fut: PyObject, res: PyObject) -> PyResult<()> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let sr = fut.getattr(py, "set_result")?;
        // The future is set on the event loop.
        // ref :https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.call_soon_threadsafe
        // call_soon_threadsafe schedules the callback (setting the future to complete) to be called
        // in the next iteration of the event loop.
        loop_.call_method1(py, "call_soon_threadsafe", (sr, res))?;

        Ok(())
    }

    //
    // Return the running event loop in the current OS thread.
    // https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.get_running_loop
    // This supported in Python 3.7 onwards.
    //
    fn get_loop(py: Python) -> PyResult<PyObject> {
        let asyncio = PyModule::import(py, "asyncio")?;
        let loop_ = asyncio.call0("get_running_loop")?;
        Ok(loop_.into())
    }
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)]
///
/// This represents a Stream reader for a given Stream.
/// Note: A python object of StreamReader cannot be created directly without using the StreamManager.
///
pub(crate) struct EventData {
    offset_in_segment: i64,
    value: Vec<u8>,
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)]
///
/// This represents a Stream reader for a given Stream.
/// Note: A python object of StreamReader cannot be created directly without using the StreamManager.
///
pub(crate) struct Slice {
    seg_slice: SegmentSlice,
}

#[pyproto]
impl PyIterProtocol for Slice {
    fn __iter__(slf: PyRef<Self>) -> PyResult<Py<Slice>> {
        Ok(slf.into())
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<EventData> {
        let next_event: Option<Event> = slf.seg_slice.next();
        next_event.map(|e| EventData {
            offset_in_segment: e.offset_in_segment,
            value: e.value,
        })
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
