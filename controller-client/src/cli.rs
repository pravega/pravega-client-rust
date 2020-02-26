/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use pravega_controller_client::*;
use pravega_rust_client_shared::*;
use tokio::runtime::Runtime;

#[macro_use]
extern crate clap;
use clap::App;

fn main() {
    let mut rt = Runtime::new().unwrap();

    // Load the yaml.
    let yml = load_yaml!("cli.yml");
    let m = App::from(yml).get_matches();

    // fetch the controller URI.
    let controller_uri = m.value_of("controller_uri").unwrap_or("http://[::1]:9090");

    // create a controller client.
    let client = rt.block_on(create_connection(controller_uri));
    let mut controller_client = ControllerClientImpl { channel: client };

    match m.subcommand() {
        ("create-scope", Some(sub_cmd)) => {
            let scope_name = sub_cmd.value_of("param1").unwrap();
            let scope_result = rt.block_on(controller_client.create_scope(&Scope::new(scope_name.into())));
            println!("Scope creation status {:?}", scope_result);
        }
        ("delete-scope", Some(sub_cmd)) => {
            let scope_name = sub_cmd.value_of("param1").unwrap();
            let scope_result = rt.block_on(controller_client.delete_scope(&Scope::new(scope_name.into())));
            println!("Scope deletion status {:?}", scope_result);
        }
        ("create-stream", Some(sub_cmd)) => {
            let scope_name = sub_cmd.value_of("param1").unwrap();
            let stream_name = sub_cmd.value_of("param2").unwrap();
            let segment_count = sub_cmd.value_of("param3").unwrap();

            let stream_cfg = StreamConfiguration {
                scoped_stream: ScopedStream {
                    scope: Scope::new(scope_name.into()),
                    stream: Stream::new(stream_name.into()),
                },
                scaling: Scaling {
                    scale_type: ScaleType::FixedNumSegments,
                    target_rate: 0,
                    scale_factor: 0,
                    min_num_segments: segment_count.parse::<i32>().unwrap(),
                },
                retention: Retention {
                    retention_type: RetentionType::None,
                    retention_param: 0,
                },
            };
            let result = rt.block_on(controller_client.create_stream(&stream_cfg));
            println!("Stream creation status {:?}", result);
        }
        ("seal-stream", Some(sub_cmd)) => {
            let scope_name = sub_cmd.value_of("param1").unwrap();
            let stream_name = sub_cmd.value_of("param2").unwrap();

            let scoped_stream =
                ScopedStream::new(Scope::new(scope_name.into()), Stream::new(stream_name.into()));

            let result = rt.block_on(controller_client.seal_stream(&scoped_stream));
            println!("Seal stream status {:?}", result);
        }
        ("delete-stream", Some(sub_cmd)) => {
            let scope_name = sub_cmd.value_of("param1").unwrap();
            let stream_name = sub_cmd.value_of("param2").unwrap();

            let scoped_stream =
                ScopedStream::new(Scope::new(scope_name.into()), Stream::new(stream_name.into()));

            let result = rt.block_on(controller_client.delete_stream(&scoped_stream));
            println!("Delete stream status {:?}", result);
        }
        _ => println!("No sub-command / invalid sub-command used"), // Either no subcommand or one not tested for...
    };
}
