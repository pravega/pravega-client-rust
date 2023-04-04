// Copyright Pravega Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#![allow(clippy::result_large_err)] //https://github.com/pravega/pravega-client-rust/issues/413
#[macro_use]
extern crate derive_new;

pub mod stream_manager;
pub mod stream_reader;
pub mod stream_reader_group;
pub mod stream_writer;
pub mod stream_writer_transactional;
pub mod transaction;
pub mod util;

use neon::prelude::*;
use stream_manager::{StreamManager, StreamRetentionPolicy, StreamScalingPolicy};
use stream_reader::{EventData, Slice, StreamReader};
use stream_reader_group::{StreamCut, StreamReaderGroup};
use stream_writer::StreamWriter;
use stream_writer_transactional::StreamTxnWriter;
use transaction::StreamTransaction;

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("StreamManagerNew", StreamManager::js_new)?;
    cx.export_function("StreamManagerCreateScope", StreamManager::js_create_scope)?;
    cx.export_function("StreamManagerDeleteScope", StreamManager::js_delete_scope)?;
    cx.export_function("StreamManagerListScopes", StreamManager::js_list_scopes)?;
    cx.export_function("StreamRetentionPolicyNone", StreamRetentionPolicy::js_none)?;
    cx.export_function("StreamRetentionPolicyBySize", StreamRetentionPolicy::js_by_size)?;
    cx.export_function("StreamRetentionPolicyByTime", StreamRetentionPolicy::js_by_time)?;
    cx.export_function(
        "StreamScalingPolicyFixed",
        StreamScalingPolicy::js_fixed_scaling_policy,
    )?;
    cx.export_function(
        "StreamScalingPolicyByDataRate",
        StreamScalingPolicy::js_auto_scaling_policy_by_data_rate,
    )?;
    cx.export_function(
        "StreamScalingPolicyByEventRate",
        StreamScalingPolicy::js_auto_scaling_policy_by_event_rate,
    )?;
    cx.export_function(
        "StreamManagerCreateStreamWithPolicy",
        StreamManager::js_create_stream_with_policy,
    )?;
    cx.export_function(
        "StreamManagerUpdateStreamWithPolicy",
        StreamManager::js_update_stream_with_policy,
    )?;
    cx.export_function("StreamManagerGetStreamTags", StreamManager::js_get_stream_tags)?;
    cx.export_function("StreamManagerSealStream", StreamManager::js_seal_stream)?;
    cx.export_function("StreamManagerDeleteStream", StreamManager::js_delete_stream)?;
    cx.export_function("StreamManagerListStreams", StreamManager::js_list_streams)?;
    cx.export_function("StreamManagerToString", StreamManager::js_to_str)?;

    cx.export_function("StreamRetentionStreamCutHead", StreamCut::js_head)?;
    cx.export_function("StreamRetentionStreamCutTail", StreamCut::js_tail)?;
    cx.export_function(
        "StreamManagerCreateReaderGroup",
        StreamManager::js_create_reader_group,
    )?;
    cx.export_function(
        "StreamManagerDeleteReaderGroup",
        StreamManager::js_delete_reader_group,
    )?;
    cx.export_function(
        "StreamReaderGroupCreateReader",
        StreamReaderGroup::js_create_reader,
    )?;
    cx.export_function(
        "StreamReaderGroupReaderOffline",
        StreamReaderGroup::js_reader_offline,
    )?;
    cx.export_function("StreamReaderGroupToString", StreamReaderGroup::js_to_str)?;

    cx.export_function("EventDataData", EventData::js_data)?;
    cx.export_function("EventDataOffset", EventData::js_offset)?;
    cx.export_function("EventDataToString", EventData::js_to_str)?;
    cx.export_function("SliceNext", Slice::js_next)?;
    cx.export_function("StreamReaderGetSegementSlice", StreamReader::js_get_segment_slice)?;
    cx.export_function("StreamReaderReaderOffline", StreamReader::js_reader_offline)?;
    cx.export_function("StreamReaderReleaseSegment", StreamReader::js_release_segment)?;
    cx.export_function("StreamReaderToString", StreamReader::js_to_str)?;

    cx.export_function("StreamManagerCreateWriter", StreamManager::js_create_writer)?;
    cx.export_function("StreamWriterWriteEventBytes", StreamWriter::js_write_event_bytes)?;
    cx.export_function("StreamWriterFlush", StreamWriter::js_flush)?;
    cx.export_function("StreamWriterToString", StreamWriter::js_to_str)?;

    cx.export_function(
        "StreamManagerCreateTxnWriter",
        StreamManager::js_create_transaction_writer,
    )?;
    cx.export_function("StreamTransactionGetTxnId", StreamTransaction::js_get_txn_id)?;
    cx.export_function("StreamTransactionIsOpen", StreamTransaction::js_is_open)?;
    cx.export_function(
        "StreamTransactionWriteEventBytes",
        StreamTransaction::js_write_event_bytes,
    )?;
    cx.export_function(
        "StreamTransactionCommitTimestamp",
        StreamTransaction::js_commit_timestamp,
    )?;
    cx.export_function("StreamTransactionAbort", StreamTransaction::js_abort)?;
    cx.export_function("StreamTransactionToString", StreamTransaction::js_to_str)?;
    cx.export_function("StreamTxnWriterBeginTxn", StreamTxnWriter::js_begin_txn)?;
    cx.export_function("StreamTxnWriterGetTxn", StreamTxnWriter::js_get_txn)?;
    cx.export_function("StreamTxnWriterToString", StreamTxnWriter::js_to_str)?;

    Ok(())
}
