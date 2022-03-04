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
} = require('./index.node');

export class StreamRetentionPolicy {
    static none = (): StreamRetentionPolicy => StreamRetentionPolicyNone();
    static by_size = (size_in_bytes: number): StreamRetentionPolicy => StreamRetentionPolicyBySize(size_in_bytes);
    static by_time = (time_in_millis: number): StreamRetentionPolicy => StreamRetentionPolicyByTime(time_in_millis);
}

export class StreamScalingPolicy {
    static fixed_scaling_policy = (initial_segments: number): StreamScalingPolicy =>
        StreamScalingPolicyFixed(initial_segments);
    static auto_scaling_policy_by_data_rate = (
        target_rate_kbytes_per_sec: number,
        scale_factor: number,
        initial_segments: number
    ): StreamScalingPolicy => StreamScalingPolicyByDataRate(target_rate_kbytes_per_sec, scale_factor, initial_segments);
    static auto_scaling_policy_by_event_rate = (
        target_events_per_sec: number,
        scale_factor: number,
        initial_segments: number
    ): StreamScalingPolicy => StreamScalingPolicyByEventRate(target_events_per_sec, scale_factor, initial_segments);
}

/**
 * Create a StreamManager by providing a controller uri.
 *
 * ```typescript
 * const stream_manager = new StreamManger('tcp://127.0.0.1:9090', false, false, true);
 * ```
 *
 * Optionally enable tls support using tls:// scheme.
 *
 * ```typescript
 * const stream_manager = new StreamManger('tls://127.0.0.1:9090', false, false, true);
 * ```
 */
export class StreamManger {
    // TODO: represent this object other than `StreamManger { StreamManger: [External: 676ee80] }`
    StreamManger: StreamManger;

    constructor(
        controller_uri: string,
        auth_enabled: boolean = false,
        tls_enabled: boolean = false,
        disable_cert_verification: boolean = true
    ) {
        this.StreamManger = StreamManagerNew(controller_uri, auth_enabled, tls_enabled, disable_cert_verification);
    }

    /**
     * Create a Pravega scope.
     *
     * @param scope_name The scope name.
     * @returns The scope creation result. `false` indicates that the scope exists before creation.
     */
    create_scope(scope_name: string): boolean {
        return StreamManagerCreateScope.call(this.StreamManger, scope_name);
    }

    /**
     * Delete a Pravega scope.
     *
     * @param scope_name The scope name.
     * @returns The scope deletion result.
     */
    delete_scope(scope_name: string): boolean {
        return StreamManagerDeleteScope.call(this.StreamManger, scope_name);
    }

    /**
     * List all scopes in Pravega.
     *
     * @returns All scope names.
     */
    list_scopes(): string[] {
        return StreamManagerListScopes.call(this.StreamManger);
    }

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
    create_stream(
        scope_name: string,
        stream_name: string,
        scaling_policy: StreamScalingPolicy = StreamRetentionPolicy.none(),
        retention_policy: StreamRetentionPolicy = StreamScalingPolicy.fixed_scaling_policy(1),
        tags: string[] = []
    ): boolean {
        return StreamManagerCreateStreamWithPolicy.call(
            this.StreamManger,
            scope_name,
            stream_name,
            scaling_policy,
            retention_policy,
            tags
        );
    }

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
    update_stream(
        scope_name: string,
        stream_name: string,
        scaling_policy: StreamScalingPolicy = StreamRetentionPolicy.none(),
        retention_policy: StreamRetentionPolicy = StreamScalingPolicy.fixed_scaling_policy(1),
        tags: string[] = []
    ): boolean {
        return StreamManagerUpdateStreamWithPolicy.call(
            this.StreamManger,
            scope_name,
            stream_name,
            scaling_policy,
            retention_policy,
            tags
        );
    }

    /**
     * Get tags of a Pravega stream.
     * 
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @returns The stream tags. 
     */
    get_stream_tags(scope_name: string, stream_name: string): string[] {
        return StreamManagerGetStreamTags.call(this.StreamManger, scope_name, stream_name);
    }

    /**
     * Seal a Pravega stream. SEAL BEFORE DELETE!
     * 
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @returns The seal result.
     */
    seal_stream(scope_name: string, stream_name: string): boolean {
        return StreamManagerSealStream.call(this.StreamManger, scope_name, stream_name);
    }

    /**
     * Deleta a Pravega stream. SEAL BEFORE DELETE!
     * 
     * @param scope_name The scope name.
     * @param stream_name The stream name.
     * @returns The deletion result.
     */
    delete_stream(scope_name: string, stream_name: string): boolean {
        return StreamManagerDeleteStream.call(this.StreamManger, scope_name, stream_name);
    }

    /**
     * List all scopes in Pravega.
     * 
     * @param scope_name The scope name.
     * @returns All stream names in this scope.
     */
    list_streams(scope_name: string): string[] {
        return StreamManagerListStreams.call(this.StreamManger, scope_name);
    }
}
