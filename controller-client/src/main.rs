/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use pravega_controller_client::*;
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfigBuilder;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error + 'static>> {
    // start Pravega standalone before invoking this function.
    {
        let controller_addr = "127.0.0.1:9090"
            .parse::<SocketAddr>()
            .expect("parse to socketaddr");
        let config = ClientConfigBuilder::default()
            .controller_uri(controller_addr)
            .build()
            .expect("creating config");
        // start Pravega standalone before invoking this function.
        let controller_client = ControllerClientImpl::new(config);

        let controller_client =
            ControllerClientImpl::create_pooled_connection("http://[::1]:9090", 2).await?;
        // let client = create_connection("http://[::1]:9090").await;
        // let mut controller_client = ControllerClientImpl { channel: client };

        let scope_name = Scope::new("testScope123".into());
        let stream_name = Stream::new("testStream".into());

        let scope_result = controller_client.create_scope(&scope_name).await;
        println!("Response for create_scope is {:?}", scope_result);

        // test multiple requests without pooling
        // netstat indicates only one port is opened.
        let mut futures = FuturesUnordered::new();
        for _ in 0..10000_i32 {
            let scope_name = Scope::new("testScope".into());
            let c = &controller_client;
            futures.push(async move { c.create_scope(&scope_name).await });
        }

        while let Some(res) = futures.next().await {
            match res {
                Ok(resp) => println!("{:?}", resp),
                Err(e) => {
                    println!("Errant response; err = {:?}", e);
                }
            }
        }
    }

    // test multiple requests with pooling
    // netstat indicates multiple ports are opened.
    let controller_client = ControllerClientImpl::create_pooled_connection("http://[::1]:9090", 2).await?;
    let mut futures = FuturesUnordered::new();
    for _ in 0..10000_i32 {
        let scope_name = Scope::new("testScope".into());
        let c = &controller_client;
        futures.push(async move { c.create_scope(&scope_name).await });
    }

    while let Some(res) = futures.next().await {
        match res {
            Ok(resp) => println!("{:?}", resp),
            Err(e) => {
                println!("Errant response; err = {:?}", e);
            }
        }
    }

    // let stream_cfg = StreamConfiguration {
    //     scoped_stream: ScopedStream {
    //         scope: scope_name.clone(),
    //         stream: stream_name.clone(),
    //     },
    //     scaling: Scaling {
    //         scale_type: ScaleType::FixedNumSegments,
    //         target_rate: 0,
    //         scale_factor: 0,
    //         min_num_segments: 2,
    //     },
    //     retention: Retention {
    //         retention_type: RetentionType::None,
    //         retention_param: 0,
    //     },
    // };
    //
    // let stream_result = controller_client.create_stream(&stream_cfg).await;
    // println!("Response for create_stream is {:?}", stream_result);
    //
    // let segment_name = ScopedSegment {
    //     scope: scope_name.clone(),
    //     stream: stream_name.clone(),
    //     segment: Segment { number: 0 },
    // };
    //
    // let endpoint_result = controller_client.get_endpoint_for_segment(&segment_name).await;
    // println!("Response for get_endpoint_for_segment is {:?}", endpoint_result);
    //
    // let scoped_stream = ScopedStream::new(
    //     Scope::new("testScope123".into()),
    //     Stream::new("testStream".into()),
    // );
    //
    // let current_segments_result = controller_client.get_current_segments(&scoped_stream).await;
    // println!(
    //     "Response for get_current_segments is {:?}",
    //     current_segments_result
    // );
    //
    // let stream_config_modified = StreamConfiguration {
    //     scoped_stream: ScopedStream {
    //         scope: scope_name.clone(),
    //         stream: stream_name.clone(),
    //     },
    //     scaling: Scaling {
    //         scale_type: ScaleType::FixedNumSegments,
    //         target_rate: 0,
    //         scale_factor: 0,
    //         min_num_segments: 1,
    //     },
    //     retention: Retention {
    //         retention_type: RetentionType::Size,
    //         retention_param: 100_000,
    //     },
    // };
    // let result_update_config = controller_client.update_stream(&stream_config_modified).await;
    // println!("Response for update_stream is {:?}", result_update_config);
    //
    // let result_truncate = controller_client
    //     .truncate_stream(&pravega_rust_client_shared::StreamCut::new(
    //         scoped_stream.clone(),
    //         HashMap::new(),
    //     ))
    //     .await;
    // println!("Response for truncate stream is {:?}", result_truncate);
    //
    // let create_txn_result = controller_client
    //     .create_transaction(&scoped_stream, Duration::from_secs(100))
    //     .await;
    // println!("Response for create transaction is {:?}", create_txn_result);
    //
    // let txn = create_txn_result.unwrap().tx_id;
    // let ping_txn_result = controller_client
    //     .ping_transaction(&scoped_stream, txn, Duration::from_secs(10))
    //     .await;
    // println!("Response for ping transaction is {:?}", ping_txn_result);
    //
    // let commit_txn_result = controller_client
    //     .commit_transaction(&scoped_stream, txn, WriterId(100), Timestamp(123))
    //     .await;
    // println!("Response for commit transaction is {:?}", commit_txn_result);
    //
    // let txn_state_result = controller_client
    //     .check_transaction_status(&scoped_stream, txn)
    //     .await;
    // println!("Response of check txn status is {:?}", txn_state_result);
    //
    // let create_txn_result1 = controller_client
    //     .create_transaction(&scoped_stream, Duration::from_secs(100))
    //     .await;
    // println!("Response for create transaction is {:?}", create_txn_result1);
    //
    // let txn1 = create_txn_result1.unwrap().tx_id;
    // let txn_state_result1 = controller_client
    //     .check_transaction_status(&scoped_stream, txn1)
    //     .await;
    // println!("Response of transaction status is {:?}", txn_state_result1);
    //
    // let abort_txn_result = controller_client.abort_transaction(&scoped_stream, txn1).await;
    // println!("Response for abort transaction is {:?}", abort_txn_result);
    //
    // let seal_result = controller_client.seal_stream(&scoped_stream).await;
    // println!("Response for seal stream is {:?}", seal_result);
    //
    // let delete_result = controller_client.delete_stream(&scoped_stream).await;
    // println!("Response for delete stream is {:?}", delete_result);
    //
    // let scope_name_1 = Scope::new("testScope456".into());
    // let scope_result = controller_client.create_scope(&scope_name_1).await;
    // println!("Response for create_scope is {:?}", scope_result);
    //
    // let delete_scope_result = controller_client.delete_scope(&scope_name_1).await;
    // println!("Response for delete scope is {:?}", delete_scope_result);
    Ok(())
}
