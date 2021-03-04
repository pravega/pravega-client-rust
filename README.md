![CIbuild](https://github.com/pravega/pravega-client-rust/workflows/CIbuild/badge.svg)
[![codecov](https://codecov.io/gh/pravega/pravega-client-rust/branch/master/graph/badge.svg?token=XEjqMkINCV)](https://codecov.io/gh/pravega/pravega-client-rust)

# Rust client for Pravega

This is a native Rust client for [Pravega](https://www.pravega.io/). 

Note: Pravega 0.9.0+ is required.

## Status

Up to date status can be seen on [the wiki](https://github.com/pravega/pravega-client-rust/wiki/Design-plan).

## Goals

The goal is to allow for clients to be written in Rust, as well as provide a common implementation for clients in higher level languages including Python and nodejs. 

See the wiki for the [status of each language](https://github.com/pravega/pravega-client-rust/wiki/Supported-APIs).

## Approach

The approach is to write a common native implementation of the internals of the client in Rust. Then use a C ABI to provide an interface for other languages to link against.

Finally for each supported language the low level API is translated into a high level API that is idiomatic for the language.

## Book

Check out the Pravega Rust client [book](https://pravega.github.io/pravega-client-rust/) for more details.