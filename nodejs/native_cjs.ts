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

const { find } = require('@mapbox/node-pre-gyp');
const { resolve, join } = require('path');

export const {
    StreamManagerNew,
    StreamManagerCreateScope,
    StreamManagerDeleteScope,
    StreamManagerListScopes,
    StreamRetentionPolicyNone,
    StreamRetentionPolicyBySize,
    StreamRetentionPolicyByTime,
    StreamScalingPolicyFixed,
    StreamScalingPolicyByDataRate,
    StreamScalingPolicyByEventRate,
    StreamManagerCreateStreamWithPolicy,
    StreamManagerUpdateStreamWithPolicy,
    StreamManagerGetStreamTags,
    StreamManagerSealStream,
    StreamManagerDeleteStream,
    StreamManagerListStreams,
    StreamRetentionStreamCutHead,
    StreamRetentionStreamCutTail,
    StreamManagerCreateReaderGroup,
    StreamManagerDeleteReaderGroup,
    StreamManagerCreateWriter,
    StreamManagerToString,
    StreamReaderGroupCreateReader,
    StreamReaderGroupReaderOffline,
    StreamReaderGroupToString,
    EventDataData,
    EventDataOffset,
    EventDataToString,
    SliceNext,
    StreamReaderGetSegementSlice,
    StreamReaderReaderOffline,
    StreamReaderReleaseSegment,
    StreamReaderToString,
    StreamWriterWriteEventBytes,
    StreamWriterFlush,
    StreamWriterToString,
    StreamManagerCreateTxnWriter,
    StreamTransactionGetTxnId,
    StreamTransactionIsOpen,
    StreamTransactionWriteEventBytes,
    StreamTransactionCommitTimestamp,
    StreamTransactionAbort,
    StreamTransactionToString,
    StreamTxnWriterBeginTxn,
    StreamTxnWriterGetTxn,
    StreamTxnWriterToString,
    // the file will be run in ./dist/esm/, so popd.
} = require(find(resolve(join(__dirname, process.env.PRAVEGA_NODEJS_DEV ? '' : '../..', './package.json'))));
