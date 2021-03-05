![CIbuild](https://github.com/pravega/pravega-client-rust/workflows/CIbuild/badge.svg)
[![codecov](https://codecov.io/gh/pravega/pravega-client-rust/branch/master/graph/badge.svg?token=XEjqMkINCV)](https://codecov.io/gh/pravega/pravega-client-rust)

# Pravega client.

This project provides a way to interact with [Pravega](http://pravega.io).

Pravega is an open source distributed storage service implementing Streams. It offers Stream as the main primitive for 
the foundation of reliable storage systems: a high-performance, durable, elastic, and unlimited append-only byte stream 
with strict ordering and consistency.

This project supports interaction with Pravega for Python versions 3.8+. For a quick tutorial on the Python bindings 
visit https://pravega.github.io/pravega-client-rust/Python/PythonBindings.html

## Usage

You can install it using pip.
```shell
pip install pravega
```
The users can also choose to generate the bindings using the commands specified at [PythonBinding](./PythonBinding.md) .
