//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_client_rust::client_factory::ClientFactory;
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::PyResult;
use std::net::SocketAddr;
use tokio::runtime::Runtime;

#[pyclass]
pub(crate) struct StreamManager {
    controller_ip: String,
    rt: Runtime,
    cf: ClientFactory,
}

#[pymethods]
impl StreamManager {
    #[new]
    fn new(controller_uri: String) -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("create runtime");
        let handle = runtime.handle().clone();
        let config = ClientConfigBuilder::default()
            .controller_uri(
                controller_uri
                    .parse::<SocketAddr>()
                    .expect("Parsing controller ip"),
            )
            .build()
            .expect("creating config");
        let client_factory = handle.block_on(ClientFactory::new(config.clone()));

        StreamManager {
            controller_ip: controller_uri,
            rt: runtime,
            cf: client_factory,
        }
    }

    pub fn create_scope(&self, scope: &str) -> PyResult<bool> {
        let handle = self.rt.handle().clone();
        println!("creating scope {:?}", scope);

        let controller = self.cf.get_controller_client();
        let scope_name = Scope::new(scope.to_string());
        // let stream_name = Stream::new("testStream".into());

        let scope_result = handle.block_on(controller.create_scope(&scope_name));
        println!("Scope Creation {:?}", scope_result);
        match scope_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }

    pub fn create_stream(&self, scope: &str, stream: &str, initial_segments: i32) -> PyResult<bool> {
        let handle = self.rt.handle().clone();
        println!(
            "creating stream {:?} under scope {:?} with segment count {:?}",
            stream, scope, initial_segments
        );
        let stream_cfg = StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: Scope::new(scope.to_string()),
                stream: Stream::new(stream.to_string()),
            },
            scaling: Scaling {
                scale_type: ScaleType::FixedNumSegments,
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: initial_segments,
            },
            retention: Retention {
                retention_type: RetentionType::None,
                retention_param: 0,
            },
        };
        let controller = self.cf.get_controller_client();

        let stream_result = handle.block_on(controller.create_stream(&stream_cfg));
        println!("Stream creation result {:?}", stream_result);
        match stream_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }
}
