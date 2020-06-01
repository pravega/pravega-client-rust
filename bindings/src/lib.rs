//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

#[macro_use]
extern crate derive_new;

use pyo3::prelude::*;
mod stream_manager;
mod stream_writer;
use stream_manager::StreamManager;
use stream_writer::StreamWriter;

#[pymodule]
/// A Python module implemented in Rust.
fn pravega_client(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<StreamManager>()?;
    m.add_class::<StreamWriter>()?;
    Ok(())
}
