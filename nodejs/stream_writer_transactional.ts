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
    StreamTransactionGetTxnId,
    StreamTransactionIsOpen,
    StreamTransactionWriteEventBytes,
    StreamTransactionCommitTimestamp,
    StreamTransactionAbort,
    StreamTransactionToString,
    StreamTxnWriterBeginTxn,
    StreamTxnWriterGetTxn,
    StreamTxnWriterToString,
} from './native_esm.js';

/**
 * This represents a transaction on a given Stream.
 */
export interface Transaction {
    /**
     * Return the transaction id in unsigned integer 128 format.
     */
    get_txn_id: () => BigInt;

    /**
     * Check if the transaction is an OPEN state.
     */
    is_open: () => Promise<boolean>;

    /**
     * Write an event into the Pravega Stream.
     * The event of type string is converted into bytes with `UTF-8` encoding.
     *
     */
    write_event: (event: string, routing_key?: string) => Promise<void>;

    /**
     * Write a byte array into the Pravega Stream. This is similar to `write_event(...)`
     * api except that the the event to be written is a byte array.
     */
    write_event_bytes: (buf: Uint8Array, routing_key?: string) => Promise<void>;

    /**
     * Commit the Transaction.
     * This Causes all messages previously written to the transaction to go into the stream contiguously.
     * This operation will either fully succeed making all events consumable or fully fail such that none of them are.
     * There may be some time delay before readers see the events after this call has returned.
     */
    commit: () => Promise<void>;

    /**
     * Commit the Transaction and the associated timestamp.
     * This Causes all messages previously written to the transaction to go into the stream contiguously.
     * This operation will either fully succeed making all events consumable or fully fail such that none of them are.
     * There may be some time delay before readers see the events after this call has returned.
     */
    commit_timestamp: (timestamp: number) => Promise<void>;

    /**
     * Abort the Transaction.
     * Drops the transaction, causing all events written to it to be deleted.
     */
    abort: () => Promise<void>;

    /**
     * Returns the string representation of the transaction including its id and scoped stream.
     */
    toString: () => string;
}

/**
 * Returns a wrapped StreamWriter that helps users to call Rust code.
 *
 * Note: A Transaction cannot be created directly without using the StreamTxnWriter.
 */
export const Transaction = (transaction): Transaction => {
    const enc = new TextEncoder(); // A `string` to `Uint8Array` serializer.

    const get_txn_id = (): BigInt => BigInt(StreamTransactionGetTxnId.call(transaction));
    const is_open = (): Promise<boolean> => StreamTransactionIsOpen.call(transaction);
    const write_event = (event: string, routing_key?: string): Promise<void> =>
        StreamTransactionWriteEventBytes.call(transaction, enc.encode(event), routing_key);
    const write_event_bytes = (buf: Uint8Array, routing_key?: string): Promise<void> =>
        StreamTransactionWriteEventBytes.call(transaction, buf, routing_key);
    const commit = (): Promise<void> => StreamTransactionCommitTimestamp.call(transaction, Number.MIN_SAFE_INTEGER);
    const commit_timestamp = (timestamp: number): Promise<void> =>
        StreamTransactionCommitTimestamp.call(transaction, timestamp);
    const abort = (): Promise<void> => StreamTransactionAbort.call(transaction);
    const toString = (): string => StreamTransactionToString.call(transaction);

    return { get_txn_id, is_open, write_event, write_event_bytes, commit, commit_timestamp, abort, toString };
};

/**
 * This represents a Transaction writer for a given Stream.
 */
export interface StreamTxnWriter {
    /**
     * Create a new transaction.
     * This returns a StreamTransaction which can be perform writes on the created transaction.
     * It can also be used to perform commit() and abort() operations on the created transaction.
     */
    begin_txn: () => Promise<Transaction>;

    /**
     * Get a StreamTransaction for a given transaction id.
     */
    get_txn: (txn_id: BigInt) => Promise<Transaction>;

    /**
     * Returns the string representation the writer with its scoped stream.
     */
    toString: () => string;
}

/**
 * Returns a wrapped StreamTxnWriter that helps users to call Rust code.
 *
 * Note: A StreamTxnWriter cannot be created directly without using the StreamManager.
 */
export const StreamTxnWriter = (stream_txn_writer): StreamTxnWriter => {
    const begin_txn = async (): Promise<Transaction> =>
        Transaction(await StreamTxnWriterBeginTxn.call(stream_txn_writer));
    const get_txn = async (txn_id: BigInt): Promise<Transaction> =>
        Transaction(await StreamTxnWriterGetTxn.call(stream_txn_writer, txn_id.toString()));
    const toString = (): string => StreamTxnWriterToString.call(stream_txn_writer);

    return { begin_txn, get_txn, toString };
};
