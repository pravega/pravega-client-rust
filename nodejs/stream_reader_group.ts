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

import {
    StreamReaderGroupCreateReader,
    StreamReaderGroupReaderOffline,
    StreamReaderGroupToString,
} from './native_esm.js';
import { StreamReader } from './stream_reader.js';

/**
 * A reader group is a collection of readers that collectively read all the events in the
 * stream. The events are distributed among the readers in the group such that each event goes
 * to only one reader.
 *
 * Note: A StreamReaderGroup cannot be created directly without using the StreamManager.
 */
export interface StreamReaderGroup {
    /**
     * Creates (or recreates) a new reader that is part of a StreamReaderGroup. The reader
     * will join the group and the members of the group will automatically rebalance among
     * themselves.
     *
     * @param reader_name A unique name (within the group) for this reader.
     * @returns The StreamReader
     */
    create_reader: (reader_name: string) => StreamReader;

    /**
     * Invoked when a reader that was added to the group is no longer consuming events. This will
     * cause the events that were going to that reader to be redistributed among the other
     * readers. Events after the lastPosition provided will be (re)read by other readers in the
     * StreamReaderGroup.
     *
     * @param reader_name The name of the reader that is offline.
     */
    reader_offline: (reader_name: string) => void;

    toString: () => string;
}

/**
 * Returns a wrapped StreamReaderGroup that helps users to call Rust code.
 *
 * Note: A StreamReaderGroup cannot be created directly without using the StreamManager.
 */
export const StreamReaderGroup = (stream_reader_group): StreamReaderGroup => {
    const create_reader = (reader_name: string): StreamReader =>
        StreamReader(StreamReaderGroupCreateReader.call(stream_reader_group, reader_name));
    const reader_offline = (reader_name: string): void =>
        StreamReaderGroupReaderOffline.call(stream_reader_group, reader_name);
    const toString = (): string => StreamReaderGroupToString.call(stream_reader_group);

    return { create_reader, reader_offline, toString };
};
