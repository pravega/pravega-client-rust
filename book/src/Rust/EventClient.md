# Event Client

## Event Writer

An EventWriter can be used to write events into the Pravega Stream.

An Event is a discrete item that can be read and processed independently.
Events are written atomically and will appear in the Stream exactly once. 
An optional routing key can be specified by the user. 
Events with the same routing key are guaranteed to be read back in the order they were written, 
while events with different routing keys may be read and processed in parallel.

[API docs](../doc/pravega_client/event/writer/struct.EventWriter.html).

## Event Reader

An EventReader can be used to read events from the Pravega Stream.

Every EventReader belongs to a ReaderGroup.
All the EventReaders within a ReaderGroup work together to from a Stream. This allows 
maximum read performance without duplicate reads. EventReaders internally coordinate with each other
within a ReaderGroup using a so-called Synchronizer.

For the details how Synchronizer works please check related blog [state synchronizer](https://blog.pravega.io/2019/02/15/exploring-state-synchronizer/).


[API docs](../doc/pravega_client/event/reader/struct.EventReader.html)

## Transactional Event Writer

Pravega transaction provides a mechanism for writing many events atomically.
A Transaction is unbounded in size but is bounded in time. If it has not been committed within a time window
specified at the time of its creation, it will be automatically aborted.

For more details please refer [API docs](../doc/pravega_client/event/transactional_writer/struct.TransactionalEventWriter.html).
