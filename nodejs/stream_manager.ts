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
    StreamManagerCreateWriter,
    StreamManagerToString,
    // the file will be run in ./dist, so popd.
} = require(find(resolve(join(__dirname, process.env.PRAVEGA_NODEJS_DEV ? '' : '..', './package.json'))));

import { StreamReaderGroup } from './stream_reader_group.js';
import { StreamWriter } from './stream_writer.js';

/**
 * Pravega allows users to store data in Tier 2 as long as there is storage capacity available.
 * But sometimes, users may not be interested to keep all the historical data related to a Stream.
 * Instead, there are use-cases in which it may be useful to retain just a fraction of a Stream's data.
 * For this reason, Streams can be configured with `StreamRetentionPolicy`.
 */
export interface StreamRetentionPolicy {}

export const StreamRetentionPolicy = {
    /**
     * Every event is retained in the Stream. No deletion.
     */
    none: (): StreamRetentionPolicy => StreamRetentionPolicyNone(),
    /**
     * Set retention based on how many data in the Stream before it is deleted.
     */
    by_size: (size_in_bytes: number): StreamRetentionPolicy => StreamRetentionPolicyBySize(size_in_bytes),
    /**
     * Set retention based on how long the data is kept in the Stream before it is deleted.
     */
    by_time: (time_in_millis: number): StreamRetentionPolicy => StreamRetentionPolicyByTime(time_in_millis),
};

/**
 * A policy that specifies how the number of segments in a stream should scale over time.
 */
export interface StreamScalingPolicy {}

export const StreamScalingPolicy = {
    /**
     * No scaling, there will only ever be initial_segmentsat any given time.
     */
    fixed_scaling_policy: (initial_segments: number): StreamScalingPolicy => StreamScalingPolicyFixed(initial_segments),
    /**
     * Scale based on the rate in bytes specified in target_rate_kbytes_per_sec.
     */
    auto_scaling_policy_by_data_rate: (
        target_rate_kbytes_per_sec: number,
        scale_factor: number,
        initial_segments: number
    ): StreamScalingPolicy => StreamScalingPolicyByDataRate(target_rate_kbytes_per_sec, scale_factor, initial_segments),
    /**
     * Scale based on the rate in events specified in target_events_per_sec.
     */
    auto_scaling_policy_by_event_rate: (
        target_events_per_sec: number,
        scale_factor: number,
        initial_segments: number
    ): StreamScalingPolicy => StreamScalingPolicyByEventRate(target_events_per_sec, scale_factor, initial_segments),
};

/**
 * Represent a consistent position in the stream.
 * Only `head` and `tail` are supported now.
 */
export interface StreamCut {}

export const StreamCut = {
    head: (): StreamCut => StreamRetentionStreamCutHead(),
    tail: (): StreamCut => StreamRetentionStreamCutTail(),
};

/**
 * Create a StreamManager by providing a controller uri.
 *
 * ```typescript
 * const stream_manager = StreamManger('tcp://127.0.0.1:9090', false, false, true);
 * ```
 *
 * Optionally enable tls support using tls:// scheme.
 *
 * ```typescript
 * const stream_manager = StreamManger('tls://127.0.0.1:9090', false, false, true);
 * ```
 */
export interface StreamManager {
    /**
     * Create a Pravega scope.
     *
     * @param scope_name The scope name.
     * @returns The scope creation result. `false` indicates that the scope exists before creation.
     */
    create_scope: (scope_name: string) => boolean;

    /**
     * Delete a Pravega scope.
     *
     * @param scope_name The scope name.
     * @returns The scope deletion result. `false` indicates that the scope does not exist before deletion.
     */
    delete_scope: (scope_name: string) => boolean;

    /**
     * List all scopes in Pravega.
     *
     * @returns All scope names.
     */
    list_scopes: () => string[];

    /**
     * Create a stream with or without specific policy in Pravega.
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @param retention_policy The retention policy. Default will be StreamRetentionPolicy.none()
     * @param scaling_policy The scaling policy. Default will be StreamScalingPolicy.fixed_scaling_policy(1)
     * @param tags The stream tags.
     * @returns The stream creation result. `false` indicates that the stream exists before creation.
     */
    create_stream: (
        scope_name: string,
        stream_name: string,
        retention_policy?: StreamRetentionPolicy,
        scaling_policy?: StreamScalingPolicy,
        tags?: string[]
    ) => boolean;

    /**
     * Update a Pravega stream with new policies and tags.
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @param retention_policy The retention policy. Default will be StreamRetentionPolicy.none()
     * @param scaling_policy The scaling policy. Default will be StreamScalingPolicy.fixed_scaling_policy(1)
     * @param tags The stream tags.
     * @returns The stream update result.
     */
    update_stream: (
        scope_name: string,
        stream_name: string,
        retention_policy?: StreamRetentionPolicy,
        scaling_policy?: StreamScalingPolicy,
        tags?: string[]
    ) => boolean;

    /**
     * Get tags of a Pravega stream.
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @returns The stream tags.
     */
    get_stream_tags: (scope_name: string, stream_name: string) => string[];

    /**
     * Seal a Pravega stream. SEAL BEFORE DELETE!
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @returns The seal result.
     */
    seal_stream: (scope_name: string, stream_name: string) => boolean;

    /**
     * Deleta a Pravega stream. SEAL BEFORE DELETE!
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @returns The deletion result.
     */
    delete_stream: (scope_name: string, stream_name: string) => boolean;

    /**
     * List all streams in the specified Pravega scope.
     *
     * @param scope_name The scope name.
     * @returns All stream names in this scope.
     */
    list_streams: (scope_name: string) => string[];

    /**
     * Create a ReaderGroup for a given Stream.
     *
     * @param stream_cut The offset you would like to read from.
     * @param reader_group_name The reader group name.
     * @param scope_name The scope name.
     * @param streams All stream names in this scope.
     * @returns A StreamReaderGroup.
     * @todo An optional element cannot follow a rest element. `...args: [...stream: string[], stream_cut?: StreamCut]`
     */
    create_reader_group: (
        stream_cut: StreamCut,
        reader_group_name: string,
        scope_name: string,
        ...streams: string[]
    ) => StreamReaderGroup;

    /**
     * Create a Writer for a given Stream.
     *
     * By default the max inflight events is configured for 0. The users can change this value
     * to ensure there are multiple inflight events at any given point in time and can use the
     * `flush()` API on the writer to wait until all the events are persisted.
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @param max_inflight_events How many event writes that are not persisted on the Pravega Stream
     *  when `write_event(...)` and `write_event_bytes(...)` are returned.
     * @returns A StreamWriter.
     */
    create_writer: (scope_name: string, stream_name: string, max_inflight_events?: number) => StreamWriter;

    /**
     * A detailed view of the StreamManager.
     *
     * @returns String representation of the StreamManager.
     */
    toString: () => string;
}

export const StreamManager = (
    controller_uri: string,
    auth_enabled: boolean = false,
    tls_enabled: boolean = false,
    disable_cert_verification: boolean = true
): StreamManager => {
    // The internal rust StreamManager object. Should not be accessed directly.
    const stream_manager = StreamManagerNew(controller_uri, auth_enabled, tls_enabled, disable_cert_verification);

    const create_scope = (scope_name: string): boolean => StreamManagerCreateScope.call(stream_manager, scope_name);
    const delete_scope = (scope_name: string): boolean => StreamManagerDeleteScope.call(stream_manager, scope_name);
    const list_scopes = (): string[] => StreamManagerListScopes.call(stream_manager);
    const create_stream = (
        scope_name: string,
        stream_name: string,
        retention_policy: StreamRetentionPolicy = StreamRetentionPolicy.none(),
        scaling_policy: StreamScalingPolicy = StreamScalingPolicy.fixed_scaling_policy(1),
        tags: string[] = []
    ): boolean =>
        StreamManagerCreateStreamWithPolicy.call(
            stream_manager,
            scope_name,
            stream_name,
            retention_policy,
            scaling_policy,
            tags
        );
    const update_stream = (
        scope_name: string,
        stream_name: string,
        retention_policy: StreamRetentionPolicy = StreamRetentionPolicy.none(),
        scaling_policy: StreamScalingPolicy = StreamScalingPolicy.fixed_scaling_policy(1),
        tags: string[] = []
    ): boolean =>
        StreamManagerUpdateStreamWithPolicy.call(
            stream_manager,
            scope_name,
            stream_name,
            retention_policy,
            scaling_policy,
            tags
        );
    const get_stream_tags = (scope_name: string, stream_name: string): string[] =>
        StreamManagerGetStreamTags.call(stream_manager, scope_name, stream_name);
    const seal_stream = (scope_name: string, stream_name: string): boolean =>
        StreamManagerSealStream.call(stream_manager, scope_name, stream_name);
    const delete_stream = (scope_name: string, stream_name: string): boolean =>
        StreamManagerDeleteStream.call(stream_manager, scope_name, stream_name);
    const list_streams = (scope_name: string): string[] => StreamManagerListStreams.call(stream_manager, scope_name);
    const create_reader_group = (
        stream_cut: StreamCut,
        reader_group_name: string,
        scope_name: string,
        ...streams: string[]
    ): StreamReaderGroup =>
        StreamReaderGroup(
            StreamManagerCreateReaderGroup.call(stream_manager, reader_group_name, scope_name, streams, stream_cut)
        );
    const create_writer = (scope_name: string, stream_name: string, max_inflight_events: number = 0): StreamWriter =>
        StreamWriter(StreamManagerCreateWriter.call(stream_manager, scope_name, stream_name, max_inflight_events));
    const toString = (): string => StreamManagerToString.call(stream_manager);

    return {
        create_scope,
        delete_scope,
        list_scopes,
        create_stream,
        update_stream,
        get_stream_tags,
        seal_stream,
        delete_stream,
        list_streams,
        create_reader_group,
        create_writer,
        toString,
    };
};
