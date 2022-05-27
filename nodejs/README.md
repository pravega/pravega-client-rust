# Pravega Nodejs client.

This project provides a way to interact with [Pravega](http://pravega.io) with a Nodejs client.

Pravega is an open source distributed storage service implementing Streams. It offers Stream as the main primitive for the foundation of reliable storage systems: a high-performance, durable, elastic, and unlimited append-only byte stream with strict ordering and consistency.

Example usage can be found in `./tests` and you may run it with `node --loader ts-node/esm tests/stream_manager.ts`.

## Supported APIs

| API details                   | Java | RUST | Python 3.6, 3.7, 3.8, 3.9 | NodeJs  |
|-------------------------------|------|------|---------------------------|---------|
| EventWriter                   | X    | X    | X                         | X[3]    |
| EventReader                   | X    | X    | X[1]                      | X[3]    |
| ReaderGroup                   | X    | X    | X                         | X[3]    |
| TxnWriter                     | X    | X    | X                         | -       |
| Transaction                   | X    | X    | X                         | -       |
| ByteStreamWriter              | X    | X    | X                         | -       |
| ByteStreamReader              | X    | X    | X                         | -       |
| StateSynchronizer             | X    | -    | -                         | -       |
| TableSynchronizer             | -    | X    | -                         | -       |
| KeyValueTable                 | X    | X    | -                         | -       |
| StreamManager#create_scope    | X    | X    | X                         | X[3]    |
| StreamManager#delete_scope    | X    | X    | X                         | X[3]    |
| StreamManager#list_scopes     | X    | X    | -                         | X[3]    |
| StreamManager#create_stream   | X    | X    | X                         | X[3]    |
| StreamManager#update_stream   | X    | X    | X                         | X[3]    |
| StreamManager#get_stream_tags | X[2] | X[2] | X[2]                      | X[2][3] |
| StreamManager#seal_stream     | X    | X    | X                         | X[3]    |
| StreamManager#delete_stream   | X    | X    | X                         | X[3]    |
| StreamManager#list_streams    | X    | X    | -                         | X[3]    |

1. StreamReader provides an Async Python binding. It requires at least Python 3.6+.
2. This requires PRAVEGA 0.10.x, enabled as part of PR https://github.com/pravega/pravega-client-rust/pull/281
3. Local npm and cargo build is required via `npm run build-debug` or `npm run install`.
