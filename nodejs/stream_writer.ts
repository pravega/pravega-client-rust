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

// One line destructuring a CommonJs module is not possible. Break into two lines.
import node_pre_gyp from '@mapbox/node-pre-gyp';
const { find } = node_pre_gyp;
import { resolve, join, dirname } from 'path';
import { fileURLToPath } from 'url';

// Native modules are not currently supported with ES module imports.
// https://nodejs.org/api/esm.html#esm_no_native_module_loading
import { createRequire } from 'module';
const require = createRequire(import.meta.url);

// __dirname is not defined in ES module scope, so get it manaully.
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const {
    StreamWriterWriteEventBytes,
    StreamWriterFlush,
    StreamWriterToString,
    // the file will be run in ./dist, so popd.
} = require(find(resolve(join(__dirname, process.env.PRAVEGA_NODEJS_DEV ? '' : '..', './package.json'))));

/**
 * A writer for a stream.
 * Note: A StreamWriter cannot be created directly without using the StreamManager.
 */
export interface StreamWriter {
    /**
     * Write an event into the Pravega Stream. The events that are written will appear
     * in the Stream exactly once. The event of type string is converted into bytes with `UTF-8` encoding.
     *
     * Note that the implementation provides retry logic to handle connection failures and service host
     * failures. Internal retries will not violate the exactly once semantic so it is better to rely on them
     * than to wrap this with custom retry logic.
     *
     * @param event String that want to be written to the Pravega Stream.
     * @param routing_key (optional) The user specified routing key.
     * @returns Promise<undefined>
     */
    write_event: (event: string, routing_key?: string) => Promise<undefined>;

    /**
     * Write a byte array into the Pravega Stream. This is similar to `write_event(...)` api except
     * that the the event to be written is a byte array.
     *
     * Note that the implementation provides retry logic to handle connection failures and service host
     * failures. Internal retries will not violate the exactly once semantic so it is better to rely on them
     * than to wrap this with custom retry logic.
     *
     * @param buf Byte array that want to be written to the Pravega Stream.
     * @param routing_key (optional) The user specified routing key.
     * @returns Promise<undefined>
     */
    write_event_bytes: (buf: Uint8Array, routing_key?: string) => Promise<undefined>;

    /**
     * Flush all the inflight events into Pravega Stream.
     * This will ensure all the inflight events are completely persisted on the Pravega Stream.
     *
     * Note that `write_event(...)` and `write_event_bytes(...)` api send the payload to a background task
     * called reactor to process, so `Promise.resolve` of these methods only means the payload has been sent
     * to the reactor. If the `max_inflight_events` is not 0 when creating the writer, there may be some
     * events that are not persisted on the Pravega Stream.
     *
     * @returns Promise<undefined>
     */
    flush: () => Promise<undefined>;

    toString: () => string;
}

export const StreamWriter = (stream_writer): StreamWriter => {
    const enc = new TextEncoder(); // string -> Uint8Array

    const write_event = async (event: string, routing_key?: string): Promise<undefined> =>
        await StreamWriterWriteEventBytes.call(stream_writer, enc.encode(event), routing_key);
    const write_event_bytes = async (buf: Uint8Array, routing_key?: string): Promise<undefined> =>
        await StreamWriterWriteEventBytes.call(stream_writer, buf, routing_key);
    const flush = (): Promise<undefined> => StreamWriterFlush.call(stream_writer);
    const toString = (): string => StreamWriterToString.call(stream_writer);

    return { write_event, write_event_bytes, flush, toString };
};
