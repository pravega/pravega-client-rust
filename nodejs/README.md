# Pravega Nodejs Client

This project provides a way to interact with [Pravega](https://cncf.pravega.io) via a Nodejs client.

Pravega is an open source distributed storage service implementing Streams. It offers Stream as the main primitive for the foundation of reliable storage systems: a high-performance, durable, elastic, and unlimited append-only byte stream with strict ordering and consistency.

## Install

The client library can be installed using npm or yarn.

```shell
npm install @pravega/pravega
#or
yarn add @pravega/pravega
```

After the package is downloaded from the registry, a `node-pre-gyp install` will be triggered to pull the underlying Rust Node addon binary from the Github releases.

Note your os and architecture matters. Only `Windows`, `MacOS`, and `linux` with `x86_64` architecture come with a pre-built binary. If this fails or your platform is not supported, you need to build the native Node addon by pulling the repo and execute several commands to get the binary. They are stated below in the development section.

## Example

After an `npm init`, add `"type": "module",` to your `package.json` so `node` can load ECMAScript modules correctly. We do provide a commonJS distribution for legacy compatibility, so feel free to use `require()`.

```javascript
import { StreamCut, StreamManager } from '@pravega/pravega';

const SCOPE = 'scope1';
const STREAM = 'stream1';
const DATA = 'Hello World!';

// Assume Pravega controller is listening at 127.0.0.1:9090
const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);
// Assume the scope and stream don't exist.
stream_manager.create_scope(SCOPE);
// This will create a stream with only 1 segment.
stream_manager.create_stream(SCOPE, STREAM);

// Write event as string.
const stream_writer_1 = stream_manager.create_writer(SCOPE, STREAM);
await stream_writer_1.write_event(DATA);
await stream_writer_1.write_event(DATA, 'routing_key');
// Write event as bytes.
const enc = new TextEncoder();
const stream_writer_2 = stream_manager.create_writer(SCOPE, STREAM);
stream_writer_2.write_event_bytes(enc.encode(DATA));
stream_writer_2.write_event_bytes(enc.encode(DATA), 'routing_key');
// You can also write them in parallel and await flush.
await stream_writer_2.flush();

// Write events as a transaction.
const stream_txn_writer = stream_manager.create_transaction_writer(SCOPE, STREAM, BigInt(1));
const txn = await stream_txn_writer.begin_txn();
await txn.write_event(DATA);
await txn.write_event_bytes(enc.encode(DATA), 'routing_key');
// You may commit or abort the transaction.
// The previous events aren't preserved in Pravega until next operation.
await txn.commit();

// Create a reader group and a reader.
const reader_group_name = Math.random().toString(36).slice(2, 10);
const reader_name = Math.random().toString(36).slice(2, 10);
const stream_reader_group = stream_manager.create_reader_group(
    StreamCut.head(),
    reader_group_name,
    SCOPE,
    STREAM
);
const stream_reader = stream_reader_group.create_reader(reader_name);

// One `get_segment_slice()` call per segment.
const seg_slice = await stream_reader.get_segment_slice();
const dec = new TextDecoder('utf-8');
for (const event of seg_slice) {
    const raw_bytes = event.data();
    console.log(`Event at ${event.offset()} reads ${dec.decode(raw_bytes)}`);
}
// Release the current slice so other reader can lock and read this slice.
stream_reader.release_segment(seg_slice);
stream_reader.reader_offline();

// Clean up.
stream_manager.seal_stream(SCOPE, STREAM);
stream_manager.delete_stream(SCOPE, STREAM);
stream_manager.delete_scope(SCOPE);
```

With a [`pravega-standalone`](https://cncf.pravega.io/docs/latest/deployment/run-local/) running locally, you can see these outputs after running it:

```shell
$ node --version
v16.15.0
$ node index.js
Event at 0 reads Hello World!
Event at 20 reads Hello World!
Event at 40 reads Hello World!
Event at 60 reads Hello World!
Event at 80 reads Hello World!
Event at 100 reads Hello World!
```

## Supported APIs

A full API reference may be found [here](https://pravega.github.io/pravega-client-rust/nodejs/index.html)

| API details                   | Java | RUST | Python 3.6, 3.7, 3.8, 3.9 | NodeJs |
|-------------------------------|------|------|---------------------------|--------|
| EventWriter                   | X    | X    | X                         | X      |
| EventReader                   | X    | X    | X[1]                      | X      |
| ReaderGroup                   | X    | X    | X                         | X      |
| TxnWriter                     | X    | X    | X                         | -      |
| Transaction                   | X    | X    | X                         | -      |
| ByteStreamWriter              | X    | X    | X                         | -      |
| ByteStreamReader              | X    | X    | X                         | -      |
| StateSynchronizer             | X    | -    | -                         | -      |
| TableSynchronizer             | -    | X    | -                         | -      |
| KeyValueTable                 | X    | X    | -                         | -      |
| StreamManager#create_scope    | X    | X    | X                         | X      |
| StreamManager#delete_scope    | X    | X    | X                         | X      |
| StreamManager#list_scopes     | X    | X    | -                         | X      |
| StreamManager#create_stream   | X    | X    | X                         | X      |
| StreamManager#update_stream   | X    | X    | X                         | X      |
| StreamManager#get_stream_tags | X[2] | X[2] | X[2]                      | X[2]   |
| StreamManager#seal_stream     | X    | X    | X                         | X      |
| StreamManager#delete_stream   | X    | X    | X                         | X      |
| StreamManager#list_streams    | X    | X    | -                         | X      |

1. StreamReader provides an Async Python binding. It requires at least Python 3.6+.
2. This requires PRAVEGA 0.10.x, enabled as part of PR https://github.com/pravega/pravega-client-rust/pull/281

## Development

To build or test this binding locally, Rust toolchain must be installed and `cargo build` can be executed without any problems in the parent project.

Then you need to install Nodejs related packages via `npm i` in this folder.

### Tests

1. `npm run build-debug` to build a debug addon.
2. `/path/to/pravega/bin/pravega-standalone`
3. `npm test`

### Local build and install

1. `npm run release-native` to build a release addon.
2. `npm run release-js` to build a release dist.
3. `npm pack` to pack a local npm package.
4. `npm i pravega-x.y.z.tgz` in your project and use it.
