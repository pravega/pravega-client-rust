![CIbuild](https://github.com/pravega/pravega-client-rust/workflows/CIbuild/badge.svg)
[![codecov](https://codecov.io/gh/pravega/pravega-client-rust/branch/master/graph/badge.svg?token=XEjqMkINCV)](https://codecov.io/gh/pravega/pravega-client-rust)

# Native client for Pravega

This is an experimental repository with the goal of supporting multiple new languages.

## Goals

The goal is to allow multiple bindings for multiple languages with a common implementation.

The initial targets are to support C# and Python.

## Approach

The approach is to write a common native implementation of the internals of the client in Rust. Then use a C ABI to provide a interface for other languages to link against.
Finally for each supported language the low level API is translated into a high level API that is idiomatic for the language.

