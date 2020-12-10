# Event Stream Client

## Event Stream Writer

An EventStreamWriter can be used to write events into the Pravega Stream.

An Event is a discrete item that can be read and processed independently.
Events are written atomically and will appear in the Stream exactly once. 
An optional routing key can be specified by the user. Events with the same routing key are guaranteed to be read back in the order they were written. While events with different routing keys may be read and processed in parallel. 

[API docs](./doc/pravega_client_rust/event_stream_writer/struct.EventStreamWriter.html).

## Event Stream Reader

An EventStreamReader can be used to read events from the Pravega Stream.

Every EventStreamReader belongs to a ReaderGroup.
All the EventStreamReaders within a ReaderGroup work together to from a Stream. This allows 
maximum read performance without duplicate reads. EventStreamReaders internally coordinate with each other
within a ReaderGroup using a so called TableSynchronizer.

For the details how TableSynchronizer works please check related blog [state synchronizer](https://blog.pravega.io/2019/02/15/exploring-state-synchronizer/).


[API docs](./doc/pravega_client_rust/event_reader/struct.EventReader.html)
