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
use pravega_wire_protocol::client_config::ClientConfigBuilder;
use std::net::SocketAddr;
use structopt::StructOpt;
use tokio::runtime::Runtime;

#[derive(StructOpt, Debug)]
enum Command {
    /// Create Scope    
    CreateScope {
        #[structopt(help = "controller-cli create-scope scope-name")]
        scope_name: String,
    },
    /// Delete Scope    
    DeleteScope {
        #[structopt(help = "controller-cli delete-scope scope-name")]
        scope_name: String,
    },
    /// Create Stream with a fixed segment count.
    CreateStream {
        #[structopt(help = "Scope Name")]
        scope_name: String,
        #[structopt(help = "Stream Name")]
        stream_name: String,
        #[structopt(help = "Segment Count")]
        segment_count: i32,
    },
    /// Seal a Stream.
    SealStream {
        #[structopt(help = "Scope Name")]
        scope_name: String,
        #[structopt(help = "Stream Name")]
        stream_name: String,
    },
    /// Delete a Stream.
    DeleteStream {
        #[structopt(help = "Scope Name")]
        scope_name: String,
        #[structopt(help = "Stream Name")]
        stream_name: String,
    },
}

#[derive(StructOpt, Debug)]
#[structopt(
    name = "Controller CLI",
    about = "Command line used to perform operations on the Pravega controller",
    version = "0.1"
)]
struct Opt {
    /// Used to configure controller grpc, default uri http://127.0.0.1:9090
    #[structopt(short = "uri", long, default_value = "127.0.0.1:9090")]
    controller_uri: String,

    #[structopt(subcommand)] // Note that we mark a field as a subcommand
    cmd: Command,
}

fn main() {
    let opt = Opt::from_args();
    let mut rt = Runtime::new().unwrap();
    let controller_addr = opt
        .controller_uri
        .parse::<SocketAddr>()
        .expect("parse to socketaddr");
    let config = ClientConfigBuilder::default()
        .controller_uri(controller_addr)
        .build()
        .expect("creating config");
    // create a controller client.
    let controller_client = ControllerClientImpl::new(config, rt.handle().clone());
    match opt.cmd {
        Command::CreateScope { scope_name } => {
            let scope_result = rt.block_on(controller_client.create_scope(&Scope::from(scope_name)));
            println!("Scope creation status {:?}", scope_result);
        }
        Command::DeleteScope { scope_name } => {
            let scope_result = rt.block_on(controller_client.delete_scope(&Scope::from(scope_name)));
            println!("Scope deletion status {:?}", scope_result);
        }
        Command::CreateStream {
            scope_name,
            stream_name,
            segment_count,
        } => {
            let stream_cfg = StreamConfiguration {
                scoped_stream: ScopedStream {
                    scope: scope_name.into(),
                    stream: stream_name.into(),
                },
                scaling: Scaling {
                    scale_type: ScaleType::FixedNumSegments,
                    target_rate: 0,
                    scale_factor: 0,
                    min_num_segments: segment_count,
                },
                retention: Retention {
                    retention_type: RetentionType::None,
                    retention_param: 0,
                },
            };
            let result = rt.block_on(controller_client.create_stream(&stream_cfg));
            println!("Stream creation status {:?}", result);
        }
        Command::SealStream {
            scope_name,
            stream_name,
        } => {
            let scoped_stream = ScopedStream::new(scope_name.into(), stream_name.into());
            let result = rt.block_on(controller_client.seal_stream(&scoped_stream));
            println!("Seal stream status {:?}", result);
        }
        Command::DeleteStream {
            scope_name,
            stream_name,
        } => {
            let scoped_stream = ScopedStream::new(scope_name.into(), stream_name.into());
            let result = rt.block_on(controller_client.delete_stream(&scoped_stream));
            println!("Delete stream status {:?}", result);
        }
    }
}
