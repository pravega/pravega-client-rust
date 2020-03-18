/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use pravega_client_rust::raw_client::*;
use pravega_wire_protocol::client_config::ClientConfigBuilder;
use pravega_wire_protocol::commands::*;
use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryImpl};
use pravega_wire_protocol::connection_pool::ConnectionPoolImpl;
use pravega_wire_protocol::wire_commands::Requests;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use structopt::StructOpt;
use tokio::runtime::Runtime;

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(0);

#[derive(StructOpt, Debug)]
enum Command {
    /// Send Hello to Server.
    Hello {
        #[structopt(short = "h", long, default_value = "9")]
        high_version: i32,
        #[structopt(short = "l", long, default_value = "5")]
        low_version: i32,
    },
    /// Create a segment with the defined configuration.
    CreateSegment {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(long)]
        target_rate: i32,
        /// ScaleType, 0 = NoScale, 1 = ByRateInKbytesPerSec 2 = ByRateInEventsPerSec
        #[structopt(long)]
        scale_type: u8,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    SetupAppend {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "w", long)]
        writer_id: u128,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Add an event to a segment. It will create the writer if it does not exist.
    ConditionalAppend {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "w", long)]
        writer_id: u128,
        #[structopt(long)]
        event_number: i64,
        #[structopt(long)]
        expected_offset: i64,
        #[structopt(short = "e", long)]
        event: String,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Read from one segment.
    ReadSegment {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "o", long)]
        offset: i64,
        #[structopt(short = "len", long)]
        suggested_length: i32,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Get segment attribute.
    GetSegmentAttribute {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "id", long)]
        attribute_id: u128,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Update segment attribute.
    UpdateSegmentAttribute {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "id", long)]
        attribute_id: u128,
        #[structopt(long)]
        new_value: i64,
        /// The default value is i64::min_value(). which means to add a new attribute.
        #[structopt(long, default_value = "-9223372036854775808")]
        expected_value: i64,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Get stream segment info.
    GetStreamSegmentInfo {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Create table segment.
    CreateTableSegment {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Update segment policy.
    UpdateSegmentPolicy {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(long)]
        target_rate: i32,
        /// ScaleType, 0 = NoScale, 1 = ByRateInKbytesPerSec 2 = ByRateInEventsPerSec
        #[structopt(long)]
        scale_type: u8,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Merge two segments.
    MergeSegments {
        #[structopt(short = "s", long)]
        source: String,
        #[structopt(short = "t", long)]
        target: String,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Merge two table segments.
    MergeTableSegments {
        #[structopt(short = "s", long)]
        source: String,
        #[structopt(short = "t", long)]
        target: String,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Seal segment.
    SealSegment {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Seal table segment.
    SealTableSegment {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Truncate segment.
    TruncateSegment {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "t", long)]
        truncation_offset: i64,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Delete segment.
    DeleteSegment {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Delete table segment.
    DeleteTableSegment {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(long)]
        must_be_empty: bool,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Update table entries, for the convenience, it only allows update one entry at one time.
    UpdateTableEntries {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(short = "k", long)]
        key: String,
        /// The default value is i64::min_value(). which means to add a new attribute.
        #[structopt(long, default_value = "-9223372036854775808")]
        key_version: i64,
        #[structopt(short = "v", long)]
        value: String,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Remove table keys.
    RemoveTableKeys {
        #[structopt(short = "s", long)]
        segment: String,
        /// The data of the table key.
        #[structopt(short = "k", long)]
        table_keys: Vec<String>,
        /// The version of the table key, the length should be the same.
        #[structopt(short = "v", long)]
        keys_version: Vec<i64>,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Read table.
    ReadTable {
        #[structopt(short = "s", long)]
        segment: String,
        /// The data of the table key.
        #[structopt(short = "k", long)]
        table_keys: Vec<String>,
        /// The version of the table key, the length should be the same.
        #[structopt(short = "v", long)]
        keys_version: Vec<i64>,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Read table keys.
    ReadTableKeys {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(long)]
        suggested_key_count: i32,
        /// this is used to indicate the point from which the next keys should be fetch.
        /// use comma to separate the num.
        #[structopt(long, default_value = "")]
        continuation_token: String,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Read table entries.
    ReadTableEntries {
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(long)]
        suggested_entry_count: i32,
        /// this is used to indicate the point from which the next entry should be fetch.
        #[structopt(long, default_value = "")]
        continuation_token: String,
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
}

#[derive(StructOpt, Debug)]
#[structopt(
    name = "server-cli",
    about = "Command line used to perform operations on the Pravega SegmentStore",
    version = "0.1"
)]
struct Opt {
    /// Used to configure controller grpc, default uri 127.0.1.1:6000
    #[structopt(short = "uri", long, default_value = "127.0.1.1:6000")]
    server_uri: String,
    /// The type of the wire command.
    #[structopt(subcommand)] // Note that we mark a field as a subcommand
    cmd: Command,
}

fn main() {
    let opt = Opt::from_args();
    let mut rt = Runtime::new().unwrap();
    let cf = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
    let config = ClientConfigBuilder::default()
        .build()
        .expect("build client config");
    let pool = ConnectionPoolImpl::new(cf, config);
    let endpoint = opt
        .server_uri
        .clone()
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");
    let raw_client = rt.block_on(RawClientImpl::new(&pool, endpoint));
    match opt.cmd {
        Command::Hello {
            high_version,
            low_version,
        } => {
            let request = Requests::Hello(HelloCommand {
                high_version,
                low_version,
            });
            let reply = rt.block_on(raw_client.send_request(request)).expect("send hello");
            println!("{:?}", reply);
        }
        Command::CreateSegment {
            segment,
            target_rate,
            scale_type,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::CreateSegment(CreateSegmentCommand {
                request_id: id,
                segment,
                target_rate,
                scale_type,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("send create segment");
            println!("{:?}", reply);
        }
        Command::SetupAppend {
            segment,
            writer_id,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            // we must do setupAppend before the ConditionalAppend.
            let request = Requests::SetupAppend(SetupAppendCommand {
                request_id: id,
                writer_id,
                segment,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("setupAppend");
            println!("{:?}", reply)
        }
        Command::ConditionalAppend {
            segment,
            writer_id,
            event_number,
            expected_offset,
            event,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            // we must do setupAppend before the ConditionalAppend.
            let request = Requests::SetupAppend(SetupAppendCommand {
                request_id: id,
                writer_id,
                segment,
                delegation_token,
            });
            rt.block_on(raw_client.send_request(request))
                .expect("setupAppend");

            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let data = event.into_bytes();
            let data_event = EventCommand { data };
            let request = Requests::ConditionalAppend(ConditionalAppendCommand {
                writer_id,
                event_number,
                expected_offset,
                event: data_event,
                request_id: id,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("Conditional append");
            println!("{:?}", reply);
        }
        Command::ReadSegment {
            segment,
            offset,
            suggested_length,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::ReadSegment(ReadSegmentCommand {
                segment,
                offset,
                suggested_length,
                delegation_token,
                request_id: id,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("read segment");
            println!("{:?}", reply);
        }
        Command::GetSegmentAttribute {
            segment,
            attribute_id,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::GetSegmentAttribute(GetSegmentAttributeCommand {
                request_id: id,
                segment_name: segment,
                attribute_id,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("get segment attribute");
            println!("{:?}", reply);
        }
        Command::UpdateSegmentAttribute {
            segment,
            attribute_id,
            new_value,
            expected_value,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::UpdateSegmentAttribute(UpdateSegmentAttributeCommand {
                request_id: id,
                segment_name: segment,
                attribute_id,
                new_value,
                expected_value,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("update segment attribute");
            println!("{:?}", reply);
        }
        Command::GetStreamSegmentInfo {
            segment,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::GetStreamSegmentInfo(GetStreamSegmentInfoCommand {
                request_id: id,
                segment_name: segment,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("get stream segment info");
            println!("{:?}", reply);
        }
        Command::CreateTableSegment {
            segment,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::CreateTableSegment(CreateTableSegmentCommand {
                request_id: id,
                segment,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("create table segment");
            println!("{:?}", reply);
        }
        Command::UpdateSegmentPolicy {
            segment,
            target_rate,
            scale_type,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::UpdateSegmentPolicy(UpdateSegmentPolicyCommand {
                request_id: id,
                segment,
                target_rate,
                scale_type,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("update segment policy");
            println!("{:?}", reply);
        }
        Command::MergeSegments {
            source,
            target,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::MergeSegments(MergeSegmentsCommand {
                request_id: id,
                target,
                source,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("merge segment");
            println!("{:?}", reply);
        }
        Command::MergeTableSegments {
            source,
            target,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::MergeTableSegments(MergeTableSegmentsCommand {
                request_id: id,
                target,
                source,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("merge table segment");
            println!("{:?}", reply);
        }
        Command::SealSegment {
            segment,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::SealSegment(SealSegmentCommand {
                request_id: id,
                segment,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("seal segment");
            println!("{:?}", reply);
        }
        Command::SealTableSegment {
            segment,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::SealTableSegment(SealTableSegmentCommand {
                request_id: id,
                segment,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("seal table segment");
            println!("{:?}", reply);
        }
        Command::TruncateSegment {
            segment,
            truncation_offset,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::TruncateSegment(TruncateSegmentCommand {
                request_id: id,
                segment,
                truncation_offset,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("truncate segment");
            println!("{:?}", reply);
        }
        Command::DeleteSegment {
            segment,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::DeleteSegment(DeleteSegmentCommand {
                request_id: id,
                segment,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("delete segment");
            println!("{:?}", reply);
        }
        Command::DeleteTableSegment {
            segment,
            must_be_empty,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let request = Requests::DeleteTableSegment(DeleteTableSegmentCommand {
                request_id: id,
                segment,
                must_be_empty,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("delete table segment");
            println!("{:?}", reply);
        }
        Command::UpdateTableEntries {
            segment,
            key,
            key_version,
            value,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let mut entries = Vec::new();
            entries.push((
                TableKey::new(key.into_bytes(), key_version),
                TableValue::new(value.into_bytes()),
            ));
            let table = TableEntries { entries };
            let request = Requests::UpdateTableEntries(UpdateTableEntriesCommand {
                request_id: id,
                segment,
                table_entries: table,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("update table entries");
            println!("{:?}", reply);
        }
        Command::RemoveTableKeys {
            segment,
            table_keys,
            keys_version,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let mut keys = Vec::new();
            let size = table_keys.len();
            for i in 0..size {
                let key = table_keys[i].clone();
                let key_verion = keys_version[i];
                keys.push(TableKey::new(key.into_bytes(), key_verion));
            }
            let request = Requests::RemoveTableKeys(RemoveTableKeysCommand {
                request_id: id,
                segment,
                keys,
                delegation_token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("remove table keys");
            println!("{:?}", reply);
        }
        Command::ReadTable {
            segment,
            table_keys,
            keys_version,
            delegation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let mut keys = Vec::new();
            let size = table_keys.len();
            for i in 0..size {
                let key = table_keys[i].clone();
                let key_verion = keys_version[i];
                keys.push(TableKey::new(key.into_bytes(), key_verion));
            }
            let request = Requests::ReadTable(ReadTableCommand {
                request_id: id,
                segment,
                keys,
                delegation_token,
            });
            let reply = rt.block_on(raw_client.send_request(request)).expect("read table");
            println!("{:?}", reply);
        }
        Command::ReadTableKeys {
            segment,
            delegation_token,
            suggested_key_count,
            continuation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let token = if continuation_token.is_empty() {
                Vec::new()
            } else {
                continuation_token
                    .split(',')
                    .map(|s| s.parse::<u8>().unwrap())
                    .collect()
            };
            let request = Requests::ReadTableKeys(ReadTableKeysCommand {
                request_id: id,
                segment,
                delegation_token,
                suggested_key_count,
                continuation_token: token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("read table keys");
            println!("{:?}", reply);
        }
        Command::ReadTableEntries {
            segment,
            delegation_token,
            suggested_entry_count,
            continuation_token,
        } => {
            let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
            let token = if continuation_token.is_empty() {
                Vec::new()
            } else {
                continuation_token
                    .split(',')
                    .map(|s| s.parse::<u8>().unwrap())
                    .collect()
            };
            let request = Requests::ReadTableEntries(ReadTableEntriesCommand {
                request_id: id,
                segment,
                delegation_token,
                suggested_entry_count,
                continuation_token: token,
            });
            let reply = rt
                .block_on(raw_client.send_request(request))
                .expect("read table entries");
            println!("{:?}", reply);
        }
    }
}
