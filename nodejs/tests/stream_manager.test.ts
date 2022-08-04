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

import { StreamManager, StreamRetentionPolicy, StreamScalingPolicy } from '../stream_manager.js';

describe('Tests on StreamManager', () => {
    it('Create and delete scope and streams', async () => {
        const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);

        // create scope and stream
        assert.equal(stream_manager.create_scope('scope1'), true);
        assert.equal(
            stream_manager.create_stream(
                'scope1',
                'stream1',
                StreamRetentionPolicy.none(),
                StreamScalingPolicy.fixed_scaling_policy(1)
            ),
            true
        );
        assert.equal(stream_manager.create_stream('scope1', 'stream2withoutpolicy'), true);

        // assert list scope and stream
        assert.deepEqual(await stream_manager.list_scopes(), ['scope1', '_system']);
        assert.deepEqual(await stream_manager.list_streams('scope1'), [
            'stream2withoutpolicy',
            '_MARKstream2withoutpolicy',
            '_MARKstream1',
            'stream1',
        ]);

        // delete stream and scope
        assert.equal(stream_manager.seal_stream('scope1', 'stream1'), true);
        assert.equal(stream_manager.delete_stream('scope1', 'stream1'), true);
        assert.equal(stream_manager.seal_stream('scope1', 'stream2withoutpolicy'), true);
        assert.equal(stream_manager.delete_stream('scope1', 'stream2withoutpolicy'), true);
        assert.deepEqual(await stream_manager.list_streams('scope1'), []);
        assert.equal(stream_manager.delete_scope('scope1'), true);
        assert.deepEqual(await stream_manager.list_scopes(), ['_system']);
    });

    it('Add tags', async () => {
        const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);
        assert.equal(stream_manager.create_scope('scope1'), true);
        assert.equal(stream_manager.create_stream('scope1', 'stream2withoutpolicy'), true);

        // update stream with tags and get tags
        assert.equal(
            stream_manager.update_stream(
                'scope1',
                'stream2withoutpolicy',
                StreamRetentionPolicy.none(),
                StreamScalingPolicy.fixed_scaling_policy(1),
                ['a', 'bb', 'ccc']
            ),
            true
        );

        assert.deepEqual(stream_manager.get_stream_tags('scope1', 'stream2withoutpolicy'), ['bb', 'a', 'ccc']);

        assert.equal(stream_manager.seal_stream('scope1', 'stream2withoutpolicy'), true);
        assert.equal(stream_manager.delete_stream('scope1', 'stream2withoutpolicy'), true);
        assert.equal(stream_manager.delete_scope('scope1'), true);
    });
});
