//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::pravega_service::PravegaStandaloneServiceConfig;
use pravega_client_rust::client_factory::ClientFactory;
use pravega_controller_client::ControllerClient;
use pravega_rust_client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
use pravega_rust_client_shared::*;
use std::sync::Arc;
use tracing::info;

pub fn test_controller_apis(config: PravegaStandaloneServiceConfig) {
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .is_auth_enabled(config.auth())
        .is_tls_enabled(config.tls())
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config);

    let controller = client_factory.get_controller_client();
    let scope_name = Scope::from("testScope123".to_owned());
    let stream_name = Stream::from("testStream".to_owned());
    let handle = client_factory.get_runtime_handle();

    let scope_result = handle.block_on(controller.create_scope(&scope_name));
    info!("Response for create_scope is {:?}", scope_result);

    let stream_cfg = StreamConfiguration {
        scoped_stream: ScopedStream {
            scope: scope_name,
            stream: stream_name,
        },
        scaling: Scaling {
            scale_type: ScaleType::FixedNumSegments,
            target_rate: 0,
            scale_factor: 0,
            min_num_segments: 1,
        },
        retention: Retention {
            retention_type: RetentionType::None,
            retention_param: 0,
        },
    };

    let stream_result = handle.block_on(controller.create_stream(&stream_cfg));
    info!("Response for create_stream is {:?}", stream_result);

    handle.block_on(test_scale_stream(controller));
}

pub async fn test_scale_stream(controller: Arc<Box<dyn ControllerClient>>) {
    let scoped_stream = ScopedStream::new(
        Scope::from("testScope123".to_owned()),
        Stream::from("testStream".to_owned()),
    );

    let current_segments_result = controller.get_current_segments(&scoped_stream).await;
    info!(
        "Response for get_current_segments is {:?}",
        current_segments_result
    );
    assert!(current_segments_result.is_ok());
    assert_eq!(1, current_segments_result.unwrap().key_segment_map.len());

    let sealed_segments = [Segment::from(0)];

    let new_range = [(0.0, 0.5), (0.5, 1.0)];

    let scale_result = controller
        .scale_stream(&scoped_stream, &sealed_segments, &new_range)
        .await;
    info!("Response for scale_stream is {:?}", scale_result);
    assert!(scale_result.is_ok());

    let current_segments_result = controller.get_current_segments(&scoped_stream).await;
    info!(
        "Response for get_current_segments is {:?}",
        current_segments_result
    );
    assert_eq!(2, current_segments_result.unwrap().key_segment_map.len());
}
