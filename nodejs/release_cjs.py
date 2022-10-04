# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This helper script will swap all the import from esm to cjs
# to support legacy commonjs.
# This is a single-way operation, change it manually back to esm
# if you need to rebuild for esm.
import os
ts_files = [file for file in os.listdir("./") if file.endswith(".ts")]
for filename in ts_files:
    data = ''
    with open(filename, 'r') as f:
        data = f.read().replace("from './native_esm.js';", "from './native_cjs.js';")
    with open(filename, 'w') as f:
        f.write(data)

# It will also add correct module type to the commonjs distribution,
# e.g. "type": "commonjs" to ./dist/cjs/package.json
# so that `require` will work properly in the legacy environment.
os.makedirs('./dist/cjs')
with open('./dist/cjs/package.json', 'w') as f:
    f.write('{"type": "commonjs"}')
