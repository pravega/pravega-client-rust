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
extern crate cfg_if;

mod stream_manager;
mod stream_writer;
cfg_if! {
    if #[cfg(feature = "python_binding")] {
        use pyo3::prelude::*;
        use stream_manager::StreamManager;
        #[macro_use]
        extern crate derive_new;
        use stream_writer::StreamWriter;
    }
}

#[cfg(feature = "python_binding")]
#[pymodule]
/// A Python module implemented in Rust.
fn pravega_client(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<StreamManager>()?;
    m.add_class::<StreamWriter>()?;
    Ok(())
}
