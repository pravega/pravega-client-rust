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
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_rust_client_config::{connection_type::ConnectionType, ClientConfigBuilder};
use pravega_rust_client_shared::PravegaNodeUri;
use pravega_wire_protocol::commands::*;
use pravega_wire_protocol::connection_factory::{
    ConnectionFactory, ConnectionFactoryConfig, SegmentConnectionManager,
};
use pravega_wire_protocol::wire_commands::Requests;
use std::sync::atomic::{AtomicUsize, Ordering};
use structopt::StructOpt;
use tokio::time::Duration;

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
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// Desire rate.
        #[structopt(long)]
        target_rate: i32,
        /// ScaleType, 0 = NoScale, 1 = ByRateInKbytesPerSec 2 = ByRateInEventsPerSec
        #[structopt(long)]
        scale_type: u8,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Setup an Append to the given segment.
    SetupAppend {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The writer id.
        #[structopt(short = "w", long)]
        writer_id: u128,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Add an event to a segment. It will create the writer if it does not exist.
    ConditionalAppend {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The writer id.
        #[structopt(short = "w", long)]
        writer_id: u128,
        /// The number of the event.
        #[structopt(long)]
        event_number: i64,
        /// The start offset of the event.
        #[structopt(long)]
        expected_offset: i64,
        /// The content of event.
        #[structopt(short = "e", long)]
        event: String,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Read some data from one segment.
    ReadSegment {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The start offset to read.
        #[structopt(short = "o", long)]
        offset: i64,
        /// The suggested_length of data.
        #[structopt(short = "len", long)]
        suggested_length: i32,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Get segment attribute of the given segment.
    GetSegmentAttribute {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The id of the attribute.
        #[structopt(short = "id", long)]
        attribute_id: u128,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Update segment attribute of the given segment.
    UpdateSegmentAttribute {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The id of the attribute.
        #[structopt(short = "id", long)]
        attribute_id: u128,
        /// The new value of the attribute.
        #[structopt(long)]
        new_value: i64,
        /// The old value of the attribute, default value is i64::min_value(). which means to add a new attribute.
        #[structopt(long, default_value = "-9223372036854775808")]
        expected_value: i64,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Get stream segment info.
    GetStreamSegmentInfo {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Create table segment.
    CreateTableSegment {
        /// The table segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Update segment policy.
    UpdateSegmentPolicy {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// Desired rate.
        #[structopt(long)]
        target_rate: i32,
        /// ScaleType, 0 = NoScale, 1 = ByRateInKbytesPerSec 2 = ByRateInEventsPerSec
        #[structopt(long)]
        scale_type: u8,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Merge two segments.
    MergeSegments {
        /// The source name of segment.
        #[structopt(short = "s", long)]
        source: String,
        /// The target name of segment.
        #[structopt(short = "t", long)]
        target: String,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Merge two table segments.
    MergeTableSegments {
        /// The source name of table segment.
        #[structopt(short = "s", long)]
        source: String,
        /// The target name of table segment.
        #[structopt(short = "t", long)]
        target: String,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Seal segment.
    SealSegment {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Seal table segment.
    SealTableSegment {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Truncate segment.
    TruncateSegment {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The truncation offset.
        #[structopt(short = "t", long)]
        truncation_offset: i64,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Delete segment.
    DeleteSegment {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Delete table segment.
    DeleteTableSegment {
        /// The table segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// If true, the segment only allows to delete if it is empty.
        #[structopt(long)]
        must_be_empty: bool,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Update table entries, for the convenience, it only allows update one entry at one time.
    UpdateTableEntries {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The key.
        #[structopt(short = "k", long)]
        key: String,
        /// The key version, default value is i64::min_value(). which means to add a new table entry.
        #[structopt(long, default_value = "-9223372036854775808")]
        key_version: i64,
        /// The value.
        #[structopt(short = "v", long)]
        value: String,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
        /// The expected offset to update.
        #[structopt(short = "t", long, default_value = "-1")]
        table_segment_offset: i64,
    },
    /// Remove table keys.
    RemoveTableKeys {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The data of the table key.
        #[structopt(short = "k", long)]
        table_keys: Vec<String>,
        /// The version of the table key, the length should be the same as keys array..
        #[structopt(short = "v", long)]
        keys_version: Vec<i64>,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
        /// The expected offset to update.
        #[structopt(short = "t", long, default_value = "-1")]
        table_segment_offset: i64,
    },
    /// Read table.
    ReadTable {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        /// The data of the table key.
        #[structopt(short = "k", long)]
        table_keys: Vec<String>,
        /// The version of the table key, the length should be the same.
        #[structopt(short = "v", long)]
        keys_version: Vec<i64>,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Read table keys.
    ReadTableKeys {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(long)]
        suggested_key_count: i32,
        /// This is used to indicate the point from which the next keys should be fetch.
        /// use comma to separate the number.
        #[structopt(long, default_value = "")]
        continuation_token: String,
        /// The delegation token, default value is null.
        #[structopt(short = "d", long, default_value = "")]
        delegation_token: String,
    },
    /// Read table entries.
    ReadTableEntries {
        /// The segment name.
        #[structopt(short = "s", long)]
        segment: String,
        #[structopt(long)]
        suggested_entry_count: i32,
        /// This is used to indicate the point from which the next keys should be fetch.
        /// use comma to separate the number.
        #[structopt(long, default_value = "")]
        continuation_token: String,
        /// The delegation token, default value is null.
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

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    let config = ClientConfigBuilder::default()
        .connection_type(ConnectionType::Tokio)
        .build()
        .expect("build client config");
    let cf = ConnectionFactory::create(ConnectionFactoryConfig::from(&config));
    let manager = SegmentConnectionManager::new(cf, config.max_connections_in_pool);
    let pool = ConnectionPool::new(manager);
    let endpoint = opt.server_uri;
    let raw_client = RawClientImpl::new(&pool, PravegaNodeUri::from(endpoint), Duration::from_secs(3600));
    match opt.cmd {
        Command::Hello {
            high_version,
            low_version,
        } => {
            let request = Requests::Hello(HelloCommand {
                high_version,
                low_version,
            });
            let reply = raw_client.send_request(&request).await.expect("send hello");
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
            let reply = raw_client
                .send_request(&request)
                .await
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
            let reply = raw_client.send_request(&request).await.expect("setupAppend");
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
            raw_client.send_request(&request).await.expect("setupAppend");

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
            let reply = raw_client
                .send_request(&request)
                .await
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
            let reply = raw_client.send_request(&request).await.expect("read segment");
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
            let reply = raw_client
                .send_request(&request)
                .await
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
            let reply = raw_client
                .send_request(&request)
                .await
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
            let reply = raw_client
                .send_request(&request)
                .await
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
            let reply = raw_client
                .send_request(&request)
                .await
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
            let reply = raw_client
                .send_request(&request)
                .await
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
            let reply = raw_client.send_request(&request).await.expect("merge segment");
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
            let reply = raw_client
                .send_request(&request)
                .await
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
            let reply = raw_client.send_request(&request).await.expect("seal segment");
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
            let reply = raw_client
                .send_request(&request)
                .await
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
            let reply = raw_client.send_request(&request).await.expect("truncate segment");
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
            let reply = raw_client.send_request(&request).await.expect("delete segment");
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
            let reply = raw_client
                .send_request(&request)
                .await
                .expect("delete table segment");
            println!("{:?}", reply);
        }
        Command::UpdateTableEntries {
            segment,
            key,
            key_version,
            value,
            delegation_token,
            table_segment_offset,
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
                table_segment_offset,
            });
            let reply = raw_client
                .send_request(&request)
                .await
                .expect("update table entries");
            println!("{:?}", reply);
        }
        Command::RemoveTableKeys {
            segment,
            table_keys,
            keys_version,
            delegation_token,
            table_segment_offset,
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
                table_segment_offset,
            });
            let reply = raw_client
                .send_request(&request)
                .await
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
            let reply = raw_client.send_request(&request).await.expect("read table");
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
            let reply = raw_client.send_request(&request).await.expect("read table keys");
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
            let reply = raw_client
                .send_request(&request)
                .await
                .expect("read table entries");
            println!("{:?}", reply);
        }
    }
}
