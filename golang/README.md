# Pravega Golang client.

This project provides a way to interact with [Pravega](http://pravega.io) using Golang client.

Pravega is an open source distributed storage service implementing Streams. It offers Stream as the main primitive for
the foundation of reliable storage systems: a high-performance, durable, elastic, and unlimited append-only byte stream
with strict ordering and consistency.

The current POC attempts to use [async-ffi crate](https://crates.io/crates/async-ffi) to convert the Rust Futures into a FFI-compatible struct.
The idea is use a poller which blocks the go-routine until the future is completed. The writer part uses
a slightly different approach. More details will be added based on the performance results.


## Build
```
# build dynamic library & generate header
cargo build

# test go code
go build examples/go/main.go
./main

# test c++ code
cd examples/c++
make
./main
```
