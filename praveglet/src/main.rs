/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use pravega_client::client_factory::ClientFactory;
use pravega_client_config::ClientConfigBuilder;
use pravega_client_shared::{PravegaNodeUri, ScaleType, Scaling, Scope, ScopedStream, StreamConfiguration};
use std::io::{self, BufRead};
use structopt::StructOpt;
use tracing::info;

#[derive(StructOpt, Debug)]
enum Command {
    /// Create Scope    
    CreateScope {
        #[structopt(help = "controller-cli create-scope scope-name")]
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
        #[structopt(help = "tag", value_name = "Tag,", use_delimiter = true, min_values = 0)]
        tags: Vec<String>,
    },
    /// Write events from STDIN.
    Write {
        #[structopt(help = "Scope Name")]
        scope_name: String,
        #[structopt(help = "Stream Name")]
        stream_name: String,
    },
    /// List Scopes
    ListScopes,
    /// List Streams under a scope
    ListStreams {
        #[structopt(help = "Scope Name")]
        scope_name: String,
    },
    /// List Streams for a tag, under a scope
    ListStreamsForTag {
        #[structopt(help = "Scope Name")]
        scope_name: String,
        #[structopt(help = "Tag Name")]
        tag: String,
    },
}

#[derive(StructOpt, Debug)]
#[structopt(
    name = "Praveglet",
    about = "Utility to interact with Pravega",
    version = "0.0.1"
)]
struct Opt {
    /// To enable TLS use uri of the format tls://ip:port
    #[structopt(short = "uri", long, default_value = "tcp://127.0.0.1:9090")]
    controller_uri: String,

    /// Enable authorization, default is false
    #[structopt(short = "auth", long)]
    enable_auth: bool,

    #[structopt(subcommand)]
    cmd: Command,
}

///
/// To enable logs set the env variable with RUST_LOG.
///  export RUST_LOG=info
///
fn main() {
    let _ = tracing_subscriber::fmt::try_init();
    let opt = Opt::from_args();
    let controller_addr = opt.controller_uri;
    let config = ClientConfigBuilder::default()
        .controller_uri(PravegaNodeUri::from(controller_addr))
        .is_auth_enabled(opt.enable_auth)
        .build()
        .expect("creating config");

    let client_factory = ClientFactory::new(config);
    // create a controller client.
    let controller_client = client_factory.controller_client();
    let rt = client_factory.runtime();
    match opt.cmd {
        Command::CreateScope { scope_name } => {
            let scope_result = rt.block_on(controller_client.create_scope(&Scope::from(scope_name)));
            info!("Scope creation status {:?}", scope_result);
        }
        Command::CreateStream {
            scope_name,
            stream_name,
            segment_count,
            tags,
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
                retention: Default::default(),
                tags: if tags.is_empty() { None } else { Some(tags) },
            };
            let result = rt.block_on(controller_client.create_stream(&stream_cfg));
            info!("Stream creation status {:?}", result);
        }
        Command::Write {
            scope_name,
            stream_name,
        } => {
            info!(
                "Attempting to read from FIFO and writing it to Scope {:?} Stream {:?}",
                scope_name, stream_name
            );
            // create event stream writer
            let stream = ScopedStream::new(scope_name.into(), stream_name.into());
            let mut event_writer = client_factory.create_event_writer(stream);
            info!("Event writer created");
            rt.block_on(async {
                let stdin = io::stdin();
                let mut event_count = 0;
                for line in stdin.lock().lines() {
                    let line = line.expect("Could not read line from standard in");
                    let result = event_writer.write_event(line.into_bytes()).await;
                    assert!(result.await.is_ok());
                    event_count += 1;
                }
                info!("Wrote {:?} events", event_count);
            });
            info!("Writes operation completed");
        }
        Command::ListScopes => {
            use futures::future;
            use futures::stream::StreamExt;
            use pravega_controller_client::paginator::list_scopes;

            let stream = list_scopes(controller_client);
            info!("Listing scopes");
            rt.block_on(stream.for_each(|scope| {
                if scope.is_ok() {
                    println!("{:?}", scope.unwrap());
                } else {
                    println!("Error while fetching data from Controller. Details: {:?}", scope);
                }
                future::ready(())
            }));
        }
        Command::ListStreams { scope_name } => {
            use futures::future;
            use futures::stream::StreamExt;
            use pravega_controller_client::paginator::list_streams;

            let scope = Scope::from(scope_name.clone());
            let stream = list_streams(scope, controller_client);
            info!("Listing streams under scope {:?}", scope_name);
            rt.block_on(stream.for_each(|stream| {
                if stream.is_ok() {
                    println!("{:?}", stream.unwrap());
                } else {
                    println!("Error while fetching data from Controller. Details: {:?}", stream);
                }
                future::ready(())
            }));
        }
        Command::ListStreamsForTag { scope_name, tag } => {
            use futures::future;
            use futures::stream::StreamExt;
            use pravega_controller_client::paginator::list_streams_for_tag;

            let scope = Scope::from(scope_name.clone());
            let stream = list_streams_for_tag(scope, tag.clone(), controller_client);
            info!("Listing streams with tag {:?} under scope {:?}", tag, scope_name);
            rt.block_on(stream.for_each(|stream| {
                if stream.is_ok() {
                    println!("{:?}", stream.unwrap());
                } else {
                    println!("Error while fetching data from Controller. Details: {:?}", stream);
                }
                future::ready(())
            }));
        }
    }
}
