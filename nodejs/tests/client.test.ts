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

// https://stackoverflow.com/questions/47277887/node-experimental-modules-requested-module-does-not-provide-an-export-named
import chai from 'chai';
const { assert } = chai;

import { StreamCut, StreamManager } from '../stream_manager';

const SCOPE = 'scope1';
const STREAM = 'stream1';
const DATA = 'Hello World!';

describe('Basic test on manager, reader, and writer', () => {
    it('main', async () => {
        const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);
        if (!(await stream_manager.list_scopes()).includes(SCOPE)) {
            stream_manager.create_scope(SCOPE);
        }
        if ((await stream_manager.list_streams(SCOPE)).includes(STREAM)) {
            stream_manager.seal_stream(SCOPE, STREAM);
            stream_manager.delete_stream(SCOPE, STREAM);
        }
        stream_manager.create_stream(SCOPE, STREAM);

        const stream_writer_1 = stream_manager.create_writer(SCOPE, STREAM);
        await stream_writer_1.write_event(DATA);
        await stream_writer_1.write_event(DATA, 'routing_key');
        const enc = new TextEncoder();
        const stream_writer_2 = stream_manager.create_writer(SCOPE, STREAM);
        stream_writer_2.write_event_bytes(enc.encode(DATA));
        stream_writer_2.write_event_bytes(enc.encode(DATA), 'routing_key');
        await stream_writer_2.flush();

        const reader_group_name = Math.random().toString(36).slice(2, 10);
        const reader_name = Math.random().toString(36).slice(2, 10);
        const stream_reader_group = stream_manager.create_reader_group(
            StreamCut.head(),
            reader_group_name,
            SCOPE,
            STREAM
        );
        const stream_reader = stream_reader_group.create_reader(reader_name);

        const seg_slice = await stream_reader.get_segment_slice();
        const dec = new TextDecoder('utf-8');
        const events = [...seg_slice].map(event => dec.decode(event.data()));
        assert.equal(events.length, 4);
        events.map(event => assert.equal(event, DATA));
        // for (const event of seg_slice) {
        //     const raw_value = event.data();
        //     console.log(`Event at ${event.offset()} reads ${dec.decode(raw_value)}`);
        // }
        stream_reader.reader_offline();

        stream_manager.seal_stream(SCOPE, STREAM);
        stream_manager.delete_stream(SCOPE, STREAM);
        stream_manager.delete_scope(SCOPE);
    });
});
