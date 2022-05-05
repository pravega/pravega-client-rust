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

import { StreamManager, StreamRetentionPolicy, StreamScalingPolicy, StreamCut } from '../stream_manager';
import { Event } from '../stream_reader';

const SCOPE = 'scope1';
const STREAM = 'stream1';
const DATA = 'Hello World!';

describe('Tests on StreamReader', () => {
    it('Write event and read', async () => {
        const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);
        if (!(await stream_manager.list_scopes()).includes(SCOPE)) {
            stream_manager.create_scope(SCOPE);
        }
        if ((await stream_manager.list_streams(SCOPE)).includes(STREAM)) {
            stream_manager.seal_stream(SCOPE, STREAM);
            stream_manager.delete_stream(SCOPE, STREAM);
        }
        stream_manager.create_stream(SCOPE, STREAM);

        const stream_writer = stream_manager.create_writer(SCOPE, STREAM);
        await stream_writer.write_event(DATA);
        await stream_writer.write_event(DATA, 'routing_key');

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
        assert.equal(events.length, 2);
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

    it('Write event with inflight and read', async () => {
        const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);
        if (!(await stream_manager.list_scopes()).includes(SCOPE)) {
            stream_manager.create_scope(SCOPE);
        }
        if ((await stream_manager.list_streams(SCOPE)).includes(STREAM)) {
            stream_manager.seal_stream(SCOPE, STREAM);
            stream_manager.delete_stream(SCOPE, STREAM);
        }
        stream_manager.create_stream(SCOPE, STREAM);

        const stream_writer = stream_manager.create_writer(SCOPE, STREAM);
        [...Array(10).keys()].map(_ => {
            stream_writer.write_event(DATA);
        });
        await stream_writer.flush();

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
        assert.equal(events.length, 10);
        events.map(event => assert.equal(event, DATA));
        stream_reader.reader_offline();

        stream_manager.seal_stream(SCOPE, STREAM);
        stream_manager.delete_stream(SCOPE, STREAM);
        stream_manager.delete_scope(SCOPE);
    });

    // This test verifies data reading a Pravega stream with multiple readers.
    // It also invokes release Segment after consuming the first segment slice and marks the first
    // reader as offline. This test verifies if we are able to read all the 100 elements.
    it('Multiple reader', async () => {
        const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);
        if (!(await stream_manager.list_scopes()).includes(SCOPE)) {
            stream_manager.create_scope(SCOPE);
        }
        if ((await stream_manager.list_streams(SCOPE)).includes(STREAM)) {
            stream_manager.seal_stream(SCOPE, STREAM);
            stream_manager.delete_stream(SCOPE, STREAM);
        }
        stream_manager.create_stream(
            SCOPE,
            STREAM,
            StreamRetentionPolicy.none(),
            StreamScalingPolicy.fixed_scaling_policy(2)
        );

        // write 100 events
        const stream_writer = stream_manager.create_writer(SCOPE, STREAM);
        [...Array(100)].map(_ => {
            stream_writer.write_event(DATA);
        });
        await stream_writer.flush();

        const reader_group_name = Math.random().toString(36).slice(2, 10);
        const stream_reader_group = stream_manager.create_reader_group(
            StreamCut.head(),
            reader_group_name,
            SCOPE,
            STREAM
        );

        // consume the just 1 event from the first segment slice.
        let count = 0;
        const stream_reader_1 = stream_reader_group.create_reader('r1');
        const seg_slice_1 = await stream_reader_1.get_segment_slice();
        const dec = new TextDecoder('utf-8');
        const event = dec.decode((seg_slice_1.next().value as Event).data());
        assert.equal(event, DATA);
        count += 1;

        // release the partially read segment slice.
        stream_reader_1.release_segment(seg_slice_1);
        stream_reader_1.reader_offline();

        // consume the rest 99 events
        const stream_reader_2 = stream_reader_group.create_reader('r2');
        const seg_slice_2 = await stream_reader_2.get_segment_slice();
        const events = [...seg_slice_2].map(event => dec.decode(event.data()));
        count += events.length;
        events.map(event => assert.equal(event, DATA));
        while (count !== 100) {
            const seg_slice_2 = await stream_reader_2.get_segment_slice();
            const events = [...seg_slice_2].map(event => dec.decode(event.data()));
            count += events.length;
            events.map(event => assert.equal(event, DATA));
        }
        stream_reader_2.reader_offline();

        assert.equal(count, 100);

        stream_manager.seal_stream(SCOPE, STREAM);
        stream_manager.delete_stream(SCOPE, STREAM);
        stream_manager.delete_scope(SCOPE);
    });

    it('Large events', async () => {
        const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);
        if (!(await stream_manager.list_scopes()).includes(SCOPE)) {
            stream_manager.create_scope(SCOPE);
        }
        if ((await stream_manager.list_streams(SCOPE)).includes(STREAM)) {
            stream_manager.seal_stream(SCOPE, STREAM);
            stream_manager.delete_stream(SCOPE, STREAM);
        }
        stream_manager.create_stream(SCOPE, STREAM);

        // write 1000 large events
        const enc = new TextEncoder();
        const buf = enc.encode('a'.repeat(100000));
        const stream_writer = stream_manager.create_writer(SCOPE, STREAM);
        [...Array(1000)].map(_ => {
            stream_writer.write_event_bytes(buf);
        });
        await stream_writer.flush();

        const reader_group_name = Math.random().toString(36).slice(2, 10);
        const reader_name = Math.random().toString(36).slice(2, 10);
        const stream_reader_group = stream_manager.create_reader_group(
            StreamCut.head(),
            reader_group_name,
            SCOPE,
            STREAM
        );
        const stream_reader = stream_reader_group.create_reader(reader_name);

        // read 1000 large events
        let count = 0;
        while (count !== 1000) {
            const seg_slice = await stream_reader.get_segment_slice();
            const events = [...seg_slice];
            count += events.length;
            stream_reader.release_segment(seg_slice);
        }
        assert.equal(count, 1000);

        // read another round to make sure no more events in the stream
        const seg_slice = await stream_reader.get_segment_slice();
        const events = [...seg_slice];
        assert.equal(events.length, 0);
        stream_reader_group.reader_offline(reader_name);

        stream_manager.seal_stream(SCOPE, STREAM);
        stream_manager.delete_stream(SCOPE, STREAM);
        stream_manager.delete_scope(SCOPE);
    });
});
