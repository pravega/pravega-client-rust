![CIbuild](https://github.com/pravega/pravega-client-rust/workflows/CIbuild/badge.svg)
[![codecov](https://codecov.io/gh/pravega/pravega-client-rust/branch/master/graph/badge.svg?token=XEjqMkINCV)](https://codecov.io/gh/pravega/pravega-client-rust)

# Pravega Python client.

This project provides a way to interact with [Pravega](http://pravega.io) using Python client.

Pravega is an open source distributed storage service implementing Streams. It offers Stream as the main primitive for 
the foundation of reliable storage systems: a high-performance, durable, elastic, and unlimited append-only byte stream 
with strict ordering and consistency.

This project supports interaction with Pravega for Python versions 3.8+. For a quick tutorial on the Python bindings 
visit the [book](https://pravega.github.io/pravega-client-rust/Python/PythonBindings.html).

Also check out the Pravega Python client [API documents](https://pravega.github.io/pravega-client-rust/python/pravega_client.html).
## Install

The client library can be installed using pip.
```shell
pip install pravega
```
The users can also choose to generate the bindings using the commands specified at [PythonBinding](./PythonBinding.md) .

## Example
### Write events
```python
import pravega_client
# assuming Pravega controller is listening at 127.0.0.1:9090
stream_manager = pravega_client.StreamManager("tcp://127.0.0.1:9090")

scope_result = stream_manager.create_scope("scope_foo")
self.assertEqual(True, scope_result, "Scope creation status")

stream_result = stream_manager.create_stream("scope_foo", "stream_bar", 1) # initially stream contains 1 segment
self.assertEqual(True, stream_result, "Stream creation status")

writer = stream_manager.create_writer("scope_foo","stream_bar")
writer.write_event("hello world")
```
### Read events
```python
import pravega_client
# assuming Pravega controller is listening at 127.0.0.1:9090
stream_manager = pravega_client.StreamManager("tcp://127.0.0.1:9090")

reader_group = stream_manager.create_reader_group("my_reader_group", "scope_foo", "stream_bar")

reader = reader_group.create_reader("my_reader");

# acquire a segment slice to read
slice = await reader.get_segment_slice_async()
for event in slice:
    print(event.data())
    
# after calling release segment, data in this segment slice will not be read again by
# readers in the same reader group.
reader.release_segment(slice)

# remember to mark the finished reader as offline.
reader.reader_offline()
```