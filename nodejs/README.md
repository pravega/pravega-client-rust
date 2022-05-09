# Pravega Nodejs client.

This project provides a way to interact with [Pravega](http://pravega.io) with a Nodejs client.

Pravega is an open source distributed storage service implementing Streams. It offers Stream as the main primitive for the foundation of reliable storage systems: a high-performance, durable, elastic, and unlimited append-only byte stream with strict ordering and consistency.

This project supports interaction with Pravega for Nodejs versions 16.

Only `StreamManager` is ready for use now. Example usage can be found in `./tests` and you may run it with `node --loader ts-node/esm tests/stream_manager.ts`.

Everything else is WORKING IN PROGRESS!
