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

pub mod stream_manager;
pub mod util;

use neon::prelude::*;
use stream_manager::{StreamManager, StreamRetentionPolicy, StreamScalingPolicy};

fn hello(mut cx: FunctionContext) -> JsResult<JsNumber> {
    let result = 42;
    Ok(cx.number(result))
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("hello", hello)?;
    cx.export_function("StreamManagerNew", StreamManager::js_new)?;
    cx.export_function("StreamManagerCreateScope", StreamManager::js_create_scope)?;
    cx.export_function("StreamManagerDeleteScope", StreamManager::js_delete_scope)?;
    cx.export_function("StreamManagerListScopes", StreamManager::js_list_scopes)?;
    cx.export_function("StreamRetentionPolicyNone", StreamRetentionPolicy::js_none)?;
    cx.export_function("StreamRetentionPolicyBySize", StreamRetentionPolicy::js_by_size)?;
    cx.export_function("StreamRetentionPolicyByTime", StreamRetentionPolicy::js_by_time)?;
    cx.export_function("StreamScalingPolicyFixed", StreamScalingPolicy::js_fixed_scaling_policy)?;
    cx.export_function("StreamScalingPolicyByDataRate", StreamScalingPolicy::js_auto_scaling_policy_by_data_rate)?;
    cx.export_function("StreamScalingPolicyByEventRate", StreamScalingPolicy::js_auto_scaling_policy_by_event_rate)?;
    cx.export_function("StreamManagerCreateStreamWithPolicy", StreamManager::js_create_stream_with_policy)?;
    cx.export_function("StreamManagerUpdateStreamWithPolicy", StreamManager::js_update_stream_with_policy)?;
    cx.export_function("StreamManagerGetStreamTags", StreamManager::js_get_stream_tags)?;
    cx.export_function("StreamManagerSealStream", StreamManager::js_seal_stream)?;
    cx.export_function("StreamManagerDeleteStream", StreamManager::js_delete_stream)?;
    cx.export_function("StreamManagerListStreams", StreamManager::js_list_streams)?;
    cx.export_function("StreamManagerToString", StreamManager::js_to_str)?;
    Ok(())
}
