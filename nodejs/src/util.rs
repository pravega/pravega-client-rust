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

use neon::prelude::*;

pub fn js_log<'a, C: Context<'a>>(cx: &mut C, s: String) -> NeonResult<()> {
    let global = cx.global();
    let console = global.get(cx, "console")?.downcast_or_throw::<JsObject, _>(cx)?;
    let log = console.get(cx, "log")?.downcast_or_throw::<JsFunction, _>(cx)?;
    let null = cx.null();

    let args: Vec<Handle<JsString>> = vec![cx.string(s)];
    log.call(cx, null, args)?;

    Ok(())
}
