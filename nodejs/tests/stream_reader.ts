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

import { StreamManager } from '../stream_manager.js';
import { StreamCut } from '../stream_reader_group.js';

const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);

const stream_reader_group = stream_manager.create_reader_group(StreamCut.head(), 'rg1', 'scope1', 'stream1');
const stream_reader = stream_reader_group.create_reader('r1');

try {
    const seg_slice = await stream_reader.get_segment_slice();
    const dec = new TextDecoder('utf-8');
    for (const event of seg_slice) {
        const raw_value = event.data();
        console.log(`Event at ${event.offset()} reads ${dec.decode(raw_value)}`);
    }
} catch (e) {
    console.log(e);
} finally {
    stream_reader_group.reader_offline('r1');
}
