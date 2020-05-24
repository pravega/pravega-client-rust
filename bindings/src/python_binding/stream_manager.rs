//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pyo3::prelude::*;

#[pyclass]
pub(crate) struct StreamManager {
    num: i32,
    debug: bool,
    controller_ip: String,
}

#[pymethods]
impl StreamManager {
    #[new]
    fn new(num: i32) -> Self {
        StreamManager {
            num,
            debug: false,
            controller_ip: "localhost:9090".to_string(),
        }
    }

    pub fn create_scope(&self, scope: &str) -> PyResult<usize> {
        println!("creating scope {:?}", scope);
        Ok(self.num as usize)
    }
}
