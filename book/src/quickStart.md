# Quick Start

This chapter demonstrates how to use Pravega Rust client to communicate to a standalone Pravega server.

## Running a standalone Pravega server
Navigate to the [Pravega Release](https://github.com/pravega/pravega/releases) page and download
a Pravega release. Note that `ByteStreamClient` requires Pravega 0.9.0+.

For example in a Linux environment, after downloading and decompressing `pravega-0.9.0.tgz`, we can start 
a minimal Pravega server by calling
```
./pravega-0.9.0/bin/pravega-standalone
```

It spins up a Pravega standalone server that listens to `localhost:9090` by default.

## Build a simple application

### Prerequisites
Make sure you have Rust installed first, check out the [official website](https://www.rust-lang.org/tools/install) of how to
install Rust.

### Creating a new project

Create a new Rust project called `my_app`
```
cargo new my_app --bin
```

in the `Cargo.toml` file, add the following as dependencies
```
[dependencies]
pravega-client = "0.1"
pravega-client-config = "0.1"
pravega-client-shared = "0.1"
tokio = "1"
```

### A simple app that writes and reads events
Check out the [event write and read example](https://github.com/pravega/pravega-client-rust/tree/master/examples).
