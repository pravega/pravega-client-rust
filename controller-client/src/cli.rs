/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use pravega_client_config::ClientConfigBuilder;
use pravega_client_shared::*;
use pravega_controller_client::*;
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
        #[structopt(help = "tag", value_name = "Tag,", use_delimiter = true, min_values = 0)]
        tags: Vec<String>,
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
    name = "Controller CLI",
    about = "Command line used to perform operations on the Pravega controller",
    version = "0.2"
)]
struct Opt {
    /// Used to configure controller grpc, default uri tcp://127.0.0.1:9090
    #[structopt(short = "uri", long, default_value = "tcp://127.0.0.1:9090")]
    controller_uri: String,

    #[structopt(subcommand)] // Note that we mark a field as a subcommand
    cmd: Command,
}

fn main() {
    let opt = Opt::from_args();
    let rt = Runtime::new().unwrap();
    let controller_addr = opt.controller_uri;
    let config = ClientConfigBuilder::default()
        .controller_uri(PravegaNodeUri::from(controller_addr))
        .build()
        .expect("creating config");
    // create a controller client.
    let controller_client = ControllerClientImpl::new(config, rt.handle());
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
        Command::ListScopes => {
            use futures::future;
            use futures::stream::StreamExt;
            use pravega_controller_client::paginator::list_scopes;

            let stream = list_scopes(&controller_client);
            println!("Listing scopes");
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
            let stream = list_streams(scope, &controller_client);
            println!("Listing streams under scope {:?}", scope_name);
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
            let stream = list_streams_for_tag(scope, tag.clone(), &controller_client);
            println!("Listing streams with tag {:?} under scope {:?}", tag, scope_name);
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
