# Event Stream Client

## Event Stream Writer

An EventStreamWriter can be used to write events into the Pravega Stream.

The concept of Event is nothing but some serialized bytes with an added 8 bytes header.
The events that are written will appear in the Stream exactly once. 
An optional routing key can be specified by the user. Events with the same routing key will be sent
to the same Segment in Pravega. A strict ordering is guaranteed within a single segment. 

[API docs](./doc/pravega_client_rust/event_stream_writer/struct.EventStreamWriter.html).

## Event Stream Reader
An EventStreamReader can be used to read events from the Pravega Stream.

Every EventStreamReader belongs to a ReaderGroup.
Multiple EventStreamReaders within a ReaderGroup can read from a Stream with coordination to achieve
maximum read performance and to avoid duplicate reads. EventStreamReaders communicate with each other
within a ReaderGroup using a so called TableSynchronizer. It conditionally writes the state of readers
to Pravega server and fetches any updates from it. 
For the details how it works please check blog [state synchronizer](https://blog.pravega.io/2019/02/15/exploring-state-synchronizer/).


[API docs](./doc/pravega_client_rust/event_reader/struct.EventReader.html)
