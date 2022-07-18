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
    EventDataData,
    EventDataOffset,
    EventDataToString,
    SliceNext,
    StreamReaderGetSegementSlice,
    StreamReaderReaderOffline,
    StreamReaderReleaseSegment,
    StreamReaderToString,
} from './native_esm.js';

/**
 * This represents an event that was read from a Pravega Segment and the offset at which the event
 * was read from.
 */
export interface Event {
    /**
     * Return the event data as `ArrayBuffer`.
     *
     * @returns ArrayBuffer that contains raw data.
     */
    data: () => ArrayBuffer;

    /**
     * Return the event offset in the segment.
     *
     * @returns offset
     */
    offset: () => number;

    toString: () => string;
}

/**
 * Returns a wrapped Event that helps users to get raw data and/or offset.
 *
 * Note: A Event cannot be created directly without using the Slice.
 */
const Event = (event): Event => {
    const data = (): ArrayBuffer => EventDataData.call(event);
    const offset = (): number => EventDataOffset.call(event);
    const toString = (): string => EventDataToString.call(event);
    return { data, offset, toString };
};

/**
 * This represents a segment slice which can be used to read events from a Pravega segment as an iterator.
 *
 * Individual events can be read from the Slice using the following snippets.
 * ```javascript
 * const seg_slice: Slice = await stream_reader.get_segment_slice();
 * for (const event of seg_slice) {
 *     const raw_value: ArrayBuffer = event.data();
 *     // do your things
 * }
 * ```
 */
export interface Slice extends IterableIterator<Event> {
    /**
     * The internal rust object used to release segment. Should **not** be used in user code!
     */
    readonly internal_slice;
}

/**
 * Returns a wrapped Slice that helps users to iterate.
 *
 * Note: A Slice cannot be created directly without using the StreamReader.
 */
const Slice = (slice): Slice => {
    return {
        internal_slice: slice,
        next: (): IteratorResult<Event> => {
            let event: Event;
            try {
                event = Event(SliceNext(slice));
            } catch (e) {
                return {
                    done: true,
                    value: null,
                };
            }
            return {
                done: false,
                value: event,
            };
        },
        [Symbol.iterator]: function () {
            return this;
        },
    };
};

/**
 * A reader for a stream.
 *
 * Note: A StreamReader cannot be created directly without using the StreamReaderGroup.
 */
export interface StreamReader {
    /**
     * This function returns a SegmentSlice from the SegmentStore(s).
     * Individual events can be read from the Slice with the following snippets.
     * ```javascript
     * const seg_slice: Slice = await stream_reader.get_segment_slice();
     * for (const event of seg_slice) {
     *     const raw_value: ArrayBuffer = event.data();
     *     // do your things
     * }
     * ```
     *
     * Invoking this function multiple times ensure multiple SegmentSlices corresponding
     * to different Segments of the stream are received. In-case we receive data for an already
     * acquired SegmentSlice, this method waits until SegmentSlice is completely consumed before
     * returning the data.
     *
     * @returns Slice in Promise.
     */
    get_segment_slice: () => Promise<Slice>;

    /**
     * Mark the reader as offline.
     *
     * This will ensure the segments owned by this reader is distributed to other readers in the ReaderGroup.
     */
    reader_offline: () => void;

    /**
     * Release a partially read segment slice back to event reader.
     */
    release_segment: (slice: Slice) => void;

    toString: () => string;
}

/**
 * Returns a wrapped StreamReader that helps users to call Rust code.
 *
 * Note: A StreamReader cannot be created directly without using the StreamReaderGroup.
 */
export const StreamReader = (stream_reader): StreamReader => {
    const get_segment_slice = async (): Promise<Slice> => Slice(await StreamReaderGetSegementSlice.call(stream_reader));
    const reader_offline = (): void => StreamReaderReaderOffline.call(stream_reader);
    const release_segment = (slice: Slice): void =>
        StreamReaderReleaseSegment.call(stream_reader, slice.internal_slice);
    const toString = (): string => StreamReaderToString.call(stream_reader);

    return { get_segment_slice, reader_offline, release_segment, toString };
};
