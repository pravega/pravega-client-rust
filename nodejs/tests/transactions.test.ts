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
import chaiAsPromised from 'chai-as-promised';
chai.use(chaiAsPromised);
const { assert, expect } = chai;

import { StreamManager } from '../stream_manager';

const SCOPE = 'scope1';
const STREAM = 'stream1';
const DATA = 'Hello World!';

describe('Tests on StreamReader', () => {
    it('Write transaction', async () => {
        const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);
        if (!(await stream_manager.list_scopes()).includes(SCOPE)) {
            stream_manager.create_scope(SCOPE);
        }
        if ((await stream_manager.list_streams(SCOPE)).includes(STREAM)) {
            stream_manager.seal_stream(SCOPE, STREAM);
            stream_manager.delete_stream(SCOPE, STREAM);
        }
        stream_manager.create_stream(SCOPE, STREAM);

        const stream_txn_writer = stream_manager.create_transaction_writer(SCOPE, STREAM, BigInt(1));
        const txn = await stream_txn_writer.begin_txn();
        assert.isAbove(Number(txn.get_txn_id()), 0);
        await txn.write_event(DATA);
        await txn.write_event(DATA);
        assert.isTrue(await txn.is_open());
        await txn.commit();
        assert.isFalse(await txn.is_open());

        const txn_old = await stream_txn_writer.begin_txn();
        await txn_old.write_event(DATA);
        const txn_id = txn_old.get_txn_id();
        const txn_new = await stream_txn_writer.get_txn(txn_id);
        assert.equal(txn_new.get_txn_id(), txn_id);
        // txn_new.write_event(DATA);
        // txn_new.abort();

        stream_manager.seal_stream(SCOPE, STREAM);
        stream_manager.delete_stream(SCOPE, STREAM);
        stream_manager.delete_scope(SCOPE);
    });

    // Test cases when transactions are already committed or aborted.
    it('Transaction error', async () => {
        const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);
        if (!(await stream_manager.list_scopes()).includes(SCOPE)) {
            stream_manager.create_scope(SCOPE);
        }
        if ((await stream_manager.list_streams(SCOPE)).includes(STREAM)) {
            stream_manager.seal_stream(SCOPE, STREAM);
            stream_manager.delete_stream(SCOPE, STREAM);
        }
        stream_manager.create_stream(SCOPE, STREAM);

        const stream_txn_writer = stream_manager.create_transaction_writer(SCOPE, STREAM, BigInt(1));
        const txn1 = await stream_txn_writer.begin_txn();
        await txn1.write_event(DATA);
        await txn1.write_event(DATA);
        await txn1.commit();

        expect(txn1.write_event(DATA)).to.be.rejectedWith(/Transaction (\w|-){36} already closed/);
        expect(txn1.abort()).to.be.rejectedWith(/Transaction (\w|-){36} already closed/);
        expect(txn1.commit()).to.be.rejectedWith(/Transaction (\w|-){36} already closed/);

        const txn2 = await stream_txn_writer.begin_txn();
        await txn2.write_event(DATA);
        await txn2.write_event(DATA);
        await txn2.abort();

        expect(txn2.write_event(DATA)).to.be.rejectedWith(/Transaction (\w|-){36} already closed/);
        expect(txn2.abort()).to.be.rejectedWith(/Transaction (\w|-){36} already closed/);
        expect(txn2.commit()).to.be.rejectedWith(/Transaction (\w|-){36} already closed/);

        stream_manager.seal_stream(SCOPE, STREAM);
        stream_manager.delete_stream(SCOPE, STREAM);
        stream_manager.delete_scope(SCOPE);
    });
});
