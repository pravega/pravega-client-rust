/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#[derive(StructOpt, Debug)]
enum Command {
    /// Send Hello to Server.
    Hello {
        #[structopt(short, long, default_value = 9)]
        high_version: i32,
        #[structopt(short, long, default_value = 5)]
        low_version: i32,
    },
    /// Create a segment with the defined configuration.
    CreateSegment {
        #[structopt(short, long)]
        segment: String,
        #[structopt(short, long)]
        target_rate: i32,
        /// ScaleType, 0 = NoScale, 1 = ByRateInKbytesPerSec 2 = ByRateInEventsPerSec
        #[structopt(short, long)]
        scale_type: u8,
        #[structopt(short, long, default_value = "")]
        delegation_token: String,
    },
    /// SetupAppend on an existing stream.
    SetupAppend {
        #[structopt(short, long)]
        writer_id: u128,
        #[structopt(short, long)]
        segment: String,
        #[structopt(short, long, default_value = "")]
        delegation_token: String,
    },
    /// Add an event to a segment.
    ConditionalAppend {
        #[structopt(short, long)]
        writer_id: u128,
        #[structopt(short, long)]
        event_number: i64,
        #[structopt(short, long)]
        expected_offset: i64,
        #[structopt(short, long)]
        event: String,
    },
    /// Read from one segment.
    ReadSegment {
        #[structopt(short, long)]
        segment: String,
        #[structopt(short, long)]
        offset: i64,
        #[structopt(short, long)]
        suggestion_length: i64,
        #[structopt(short, long, default_value = "")]
        delegation_token: String,
    },
    GetSegmentAttribute {
        #[structopt(short, long)]
        segment: String,
        #[structopt(short, long)]
        attribute_id: u128,
        #[structopt(short, long, default_value = "")]
        delegation_token: String,
    },
    UpdateSegmentAttribute {
        #[structopt(short = "s")]
        segment: String,
        attribute_id: u128,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    GetStreamSegmentInfo {
        #[structopt(short = "s")]
        segment: String,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    CreateTableSegment {
        #[structopt(short = "s")]
        segment: String,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    UpdateSegmentPolicy {
        #[structopt(short = "s")]
        segment: String,
        target_rate: i32,
        scale_type: u8,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    MergeSegments {
        #[structopt(short = "s")]
        source: String,
        #[structopt(short = "t")]
        target: String,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    MergeTableSegments {
        #[structopt(short = "s")]
        source: String,
        #[structopt(short = "t")]
        target: String,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    SealSegment {
        #[structopt(short = "s")]
        segment: String,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    SealTableSegment {
        #[structopt(short = "s")]
        segment: String,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    TruncateSegment {
        #[structopt(short = "s")]
        segment: String,
        truncation_offset: i64,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    DeleteSegment {
        #[structopt(short = "s")]
        segment: String,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    DeleteTableSegment {
        #[structopt(short = "s")]
        segment: String,
        must_be_empty: bool,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    UpdateTableEntries {
        #[structopt(short = "s")]
        segment: String,
        #[structopt(parse(from_os_str))]
        table_entries: Vec<String>,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    RemoveTableKeys {
        #[structopt(short = "s")]
        segment: String,
        table_keys: Vec<String>,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    ReadTable {
        #[structopt(short = "s")]
        segment: String,
        #[structopt(parse(from_os_str))]
        table_keys: Vec<String>,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    ReadTableKeys {
        #[structopt(short = "s")]
        segment: String,
        #[structopt(parse(from_os_str))]
        table_keys: Vec<String>,
        #[structopt(short = "d", default_value = "")]
        delegation_token: String,
    },
    ReadTableEntries {

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

