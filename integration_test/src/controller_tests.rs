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
use pravega_client::client_factory::ClientFactory;
use pravega_client_config::{ClientConfigBuilder, MOCK_CONTROLLER_URI};
use pravega_client_shared::*;
use pravega_controller_client::paginator::{list_streams, list_streams_for_tag};
use pravega_controller_client::ControllerClient;
use std::sync::Arc;
use tracing::info;

const SCOPE: &str = "testScope123";

pub fn test_controller_apis(config: PravegaStandaloneServiceConfig) {
    let config = ClientConfigBuilder::default()
        .controller_uri(MOCK_CONTROLLER_URI)
        .is_auth_enabled(config.auth)
        .is_tls_enabled(config.tls)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config);

    let controller = client_factory.controller_client();
    let handle = client_factory.runtime();
    // Create a Scope that is used by all the tests.
    let scope_result = handle.block_on(controller.create_scope(&Scope::from(SCOPE.to_owned())));
    info!("Response for create_scope is {:?}", scope_result);
    // Invoke the tests.
    handle.block_on(test_scope_stream(controller));
    handle.block_on(test_stream_tags(controller));
    handle.block_on(test_scale_stream(controller));
}

pub async fn test_scope_stream(controller: &dyn ControllerClient) {
    let scopes = get_all_scopes(controller).await;
    let scope1 = Scope::from(SCOPE.to_string());
    let scope2 = Scope::from("_system".to_string());
    let scope3 = Scope::from("sc1".to_string());
    let scope4 = Scope::from("sc2".to_string());
    assert!(scopes.contains(&scope1));
    assert!(scopes.contains(&scope2));

    assert!(controller
        .check_scope_exists(&scope1)
        .await
        .expect("check scope exists"));
    assert!(!controller
        .check_scope_exists(&Scope::from("dummy".to_string()))
        .await
        .expect("check scope exists"));

    controller
        .create_scope(&scope3)
        .await
        .expect("Creating scope sc1");

    controller
        .create_scope(&scope4)
        .await
        .expect("Creating scope sc2");

    let scopes = get_all_scopes(controller).await;
    assert_eq!(scopes.len(), 4);
    assert!(scopes.contains(&scope1));
    assert!(scopes.contains(&scope2));
    assert!(scopes.contains(&scope3));
    assert!(scopes.contains(&scope4));

    let streams = get_all_streams(controller, &scope3).await;
    assert_eq!(streams.len(), 0);

    let st1 = ScopedStream {
        scope: scope3.clone(),
        stream: Stream::from("st1".to_string()),
    };
    assert!(!controller
        .check_stream_exists(&st1)
        .await
        .expect("Check stream exists"));
    let stream_cfg = StreamConfiguration {
        scoped_stream: st1.clone(),
        scaling: Default::default(),
        retention: Default::default(),
        tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
    };

    let _stream_result = controller.create_stream(&stream_cfg).await;
    assert!(controller
        .check_stream_exists(&st1)
        .await
        .expect("Check stream exists"));
    let streams = get_all_streams(controller, &scope3).await;
    // _Mark stream is also created.
    assert_eq!(streams.len(), 2);
    assert!(streams.contains(&st1));
}

pub async fn test_stream_tags(controller: &dyn ControllerClient) {
    let scope_name = Scope::from(SCOPE.to_string());
    let stream_name = Stream::from("testTags".to_owned());
    let scoped_stream1 = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name,
    };

    let stream_cfg = StreamConfiguration {
        scoped_stream: scoped_stream1.clone(),
        scaling: Scaling {
            scale_type: ScaleType::FixedNumSegments,
            target_rate: 0,
            scale_factor: 0,
            min_num_segments: 1,
        },
        retention: Default::default(),
        tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
    };

    let stream_result = controller.create_stream(&stream_cfg).await;
    info!("Response for create_stream is {:?}", stream_result);
    let config_result = controller
        .get_stream_configuration(&scoped_stream1)
        .await
        .unwrap();
    assert_eq!(config_result, stream_cfg);
    info!("Response of get Stream Configuration is {:?}", config_result);

    let tags = controller.get_stream_tags(&scoped_stream1).await.unwrap();
    assert_eq!(tags, stream_cfg.tags);
    info!("Response for getTags is {:?}", tags);

    let scoped_stream2 = ScopedStream {
        scope: scope_name.clone(),
        stream: Stream {
            name: "testTags2".to_string(),
        },
    };
    let stream_cfg = StreamConfiguration {
        scoped_stream: scoped_stream2.clone(),
        scaling: Scaling {
            scale_type: ScaleType::ByRateInEventsPerSec,
            target_rate: 10,
            scale_factor: 2,
            min_num_segments: 1,
        },
        retention: Retention {
            retention_type: RetentionType::Size,
            retention_param: 1024 * 1024,
        },
        tags: Some(vec!["tag2".to_string(), "tag3".to_string()]),
    };
    let stream_result = controller.create_stream(&stream_cfg).await;
    info!("Response for create_stream is {:?}", stream_result);
    let config_result = controller
        .get_stream_configuration(&scoped_stream2)
        .await
        .unwrap();
    assert_eq!(config_result, stream_cfg);
    info!("Response of get Stream Configuration is {:?}", config_result);

    let tags = controller.get_stream_tags(&scoped_stream2).await.unwrap();
    assert_eq!(tags, stream_cfg.tags);
    info!("Response for getTags is {:?}", tags);

    // Verify listStreams for the specified tag.
    let stream_list = get_all_streams_for_tag(controller, &scope_name, "tag2").await;

    assert_eq!(2, stream_list.len());
    assert!(stream_list.contains(&scoped_stream1));
    assert!(stream_list.contains(&scoped_stream2));

    let stream_list = get_all_streams_for_tag(controller, &scope_name, "tag1").await;

    assert_eq!(1, stream_list.len());
    assert!(stream_list.contains(&scoped_stream1));

    use futures::StreamExt;
    let stream = list_streams_for_tag(scope_name, "tag2".to_string(), controller);
    futures::pin_mut!(stream);
    let stream_list: Vec<ScopedStream> = stream
        .map(|str| str.unwrap())
        .collect::<Vec<ScopedStream>>()
        .await;
    assert_eq!(2, stream_list.len());
    assert!(stream_list.contains(&scoped_stream1));
    assert!(stream_list.contains(&scoped_stream2));
}

// Helper method to fetch all the streams for a tag.
async fn get_all_streams_for_tag(
    controller: &dyn ControllerClient,
    scope_name: &Scope,
    tag: &str,
) -> Vec<ScopedStream> {
    let mut result: Vec<ScopedStream> = Vec::new();

    let mut token = CToken::empty();
    while let Some((mut res, next_token)) = controller
        .list_streams_for_tag(scope_name, tag, &token)
        .await
        .unwrap()
    {
        result.append(&mut res);
        token = next_token;
    }
    result
}

// Helper method to fetch all the scopes.
async fn get_all_scopes(controller: &dyn ControllerClient) -> Vec<Scope> {
    let mut result: Vec<Scope> = Vec::new();

    let mut token = CToken::empty();
    while let Some((mut res, next_token)) = controller.list_scopes(&token).await.unwrap() {
        result.append(&mut res);
        token = next_token;
    }
    result
}

// Helper method to fetch all the streams for a tag.
async fn get_all_streams(controller: &dyn ControllerClient, scope_name: &Scope) -> Vec<ScopedStream> {
    let mut result: Vec<ScopedStream> = Vec::new();

    let mut token = CToken::empty();
    while let Some((mut res, next_token)) = controller.list_streams(scope_name, &token).await.unwrap() {
        result.append(&mut res);
        token = next_token;
    }
    result
}

pub async fn test_scale_stream(controller: &dyn ControllerClient) {
    let scoped_stream = ScopedStream {
        scope: Scope::from("testScope123".to_owned()),
        stream: Stream::from("testStreamScale".to_owned()),
    };
    let stream_cfg = StreamConfiguration {
        scoped_stream: scoped_stream.clone(),
        scaling: Scaling {
            scale_type: ScaleType::FixedNumSegments,
            target_rate: 0,
            scale_factor: 0,
            min_num_segments: 1,
        },
        retention: Default::default(),
        tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
    };
    let stream_result = controller.create_stream(&stream_cfg).await;
    info!("Response of create stream is {:?}", stream_result);

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

    let head_segments_result = controller.get_head_segments(&scoped_stream).await;
    info!("Response for get_head_segments is {:?}", head_segments_result);
    assert_eq!(1, head_segments_result.unwrap().len());
}
