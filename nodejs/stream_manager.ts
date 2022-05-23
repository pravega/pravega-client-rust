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

import { StreamCut, StreamReaderGroup } from './stream_reader_group.js';
import { StreamWriter } from './stream_writer.js';

// Native modules are not currently supported with ES module imports.
// https://nodejs.org/api/esm.html#esm_no_native_module_loading
import { createRequire } from 'module';
const require = createRequire(import.meta.url);

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
    StreamManagerCreateReaderGroup,
    StreamManagerCreateWriter,
    StreamManagerToString,
} = require('./index.node');

export interface StreamRetentionPolicy {}

export const StreamRetentionPolicy = {
    none: (): StreamRetentionPolicy => StreamRetentionPolicyNone(),
    by_size: (size_in_bytes: number): StreamRetentionPolicy => StreamRetentionPolicyBySize(size_in_bytes),
    by_time: (time_in_millis: number): StreamRetentionPolicy => StreamRetentionPolicyByTime(time_in_millis),
};

export interface StreamScalingPolicy {}

export const StreamScalingPolicy = {
    fixed_scaling_policy: (initial_segments: number): StreamScalingPolicy => StreamScalingPolicyFixed(initial_segments),
    auto_scaling_policy_by_data_rate: (
        target_rate_kbytes_per_sec: number,
        scale_factor: number,
        initial_segments: number
    ): StreamScalingPolicy => StreamScalingPolicyByDataRate(target_rate_kbytes_per_sec, scale_factor, initial_segments),
    auto_scaling_policy_by_event_rate: (
        target_events_per_sec: number,
        scale_factor: number,
        initial_segments: number
    ): StreamScalingPolicy => StreamScalingPolicyByEventRate(target_events_per_sec, scale_factor, initial_segments),
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
export const StreamManager = (
    controller_uri: string,
    auth_enabled: boolean = false,
    tls_enabled: boolean = false,
    disable_cert_verification: boolean = true
) => {
    const stream_manager = StreamManagerNew(controller_uri, auth_enabled, tls_enabled, disable_cert_verification);

    /**
     * Create a Pravega scope.
     *
     * @param scope_name The scope name.
     * @returns The scope creation result. `false` indicates that the scope exists before creation.
     */
    const create_scope = (scope_name: string): boolean => StreamManagerCreateScope.call(stream_manager, scope_name);

    /**
     * Delete a Pravega scope.
     *
     * @param scope_name The scope name.
     * @returns The scope deletion result. `false` indicates that the scope does not exist before deletion.
     */
    const delete_scope = (scope_name: string): boolean => StreamManagerDeleteScope.call(stream_manager, scope_name);

    /**
     * List all scopes in Pravega.
     *
     * @returns All scope names.
     */
    const list_scopes = (): string[] => StreamManagerListScopes.call(stream_manager);

    /**
     * Create a stream with or without specific policy in Pravega.
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @param scaling_policy The scaling policy.
     * @param retention_policy The retention policy.
     * @param tags The stream tags.
     * @returns The stream creation result. `false` indicates that the stream exists before creation.
     */
    const create_stream = (
        scope_name: string,
        stream_name: string,
        scaling_policy: StreamScalingPolicy = StreamRetentionPolicy.none(),
        retention_policy: StreamRetentionPolicy = StreamScalingPolicy.fixed_scaling_policy(1),
        tags: string[] = []
    ): boolean =>
        StreamManagerCreateStreamWithPolicy.call(
            stream_manager,
            scope_name,
            stream_name,
            scaling_policy,
            retention_policy,
            tags
        );

    /**
     * Update a Pravega stream with new policies and tags.
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @param scaling_policy The scaling policy.
     * @param retention_policy The retention policy.
     * @param tags The stream tags.
     * @returns The stream update result.
     */
    const update_stream = (
        scope_name: string,
        stream_name: string,
        scaling_policy: StreamScalingPolicy = StreamRetentionPolicy.none(),
        retention_policy: StreamRetentionPolicy = StreamScalingPolicy.fixed_scaling_policy(1),
        tags: string[] = []
    ): boolean =>
        StreamManagerUpdateStreamWithPolicy.call(
            stream_manager,
            scope_name,
            stream_name,
            scaling_policy,
            retention_policy,
            tags
        );

    /**
     * Get tags of a Pravega stream.
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @returns The stream tags.
     */
    const get_stream_tags = (scope_name: string, stream_name: string): string[] =>
        StreamManagerGetStreamTags.call(stream_manager, scope_name, stream_name);

    /**
     * Seal a Pravega stream. SEAL BEFORE DELETE!
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @returns The seal result.
     */
    const seal_stream = (scope_name: string, stream_name: string): boolean =>
        StreamManagerSealStream.call(stream_manager, scope_name, stream_name);

    /**
     * Deleta a Pravega stream. SEAL BEFORE DELETE!
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @returns The deletion result.
     */
    const delete_stream = (scope_name: string, stream_name: string): boolean =>
        StreamManagerDeleteStream.call(stream_manager, scope_name, stream_name);

    /**
     * List all scopes in Pravega.
     *
     * @param scope_name The scope name.
     * @returns All stream names in this scope.
     */
    const list_streams = (scope_name: string): string[] => StreamManagerListStreams.call(stream_manager, scope_name);

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
    const create_reader_group = (
        stream_cut: StreamCut,
        reader_group_name: string,
        scope_name: string,
        ...streams: string[]
    ): StreamReaderGroup =>
        StreamReaderGroup(
            StreamManagerCreateReaderGroup.call(stream_manager, reader_group_name, scope_name, streams, stream_cut)
        );

    /**
     * Create a Writer for a given Stream.
     *
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @returns A StreamWriter.
     */
    const create_writer = (scope_name: string, stream_name: string): StreamWriter =>
        StreamWriter(StreamManagerCreateWriter.call(stream_manager, scope_name, stream_name));

    /**
     * A detailed view of a StreamManager.
     *
     * @returns String representation of the StreamManager.
     */
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
