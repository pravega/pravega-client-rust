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
    let console = global.get::<JsObject, _, _>(cx, "console")?;
    let log = console.get::<JsFunction, _, _>(cx, "log")?;

    log.call_with(cx).arg(cx.string(s)).exec(cx)?;

    Ok(())
}
