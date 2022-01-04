# Python Binding

The below section provides a Pravega Python API tutorial for the Rust based Pravega Native client. This tutorial is a 
companion to [Pravega Python API](../python/pravega_client.html)

Also, check out the [repo](https://github.com/pravega/pravega-client-rust/tree/master/python).

## StreamManager

A StreamManger is used to create a Scopes, Streams, Writers and Readers. It can also be used to perform
operations on the stream which include sealing a stream and deleting streams among others.

A StreamManager can be created by using a Controller URI. The below example snippet 

 ```python
import pravega_client
manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
# stream manager can be used to create scopes, streams, writers and readers against Pravega.
manager.create_scope("scope")
manager.create_stream("scope", "stream", 1)
 ```

A StreamWriter can be used create a Stream writer. The below snippet is shows a sample pattern.

```python
import pravega_client
manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
# create a writer against an already created Pravega scope and Stream.
writer=manager.create_writer("scope", "stream")
```
A transactional Writer can be created using the StreamManager. The below example snippet shows a sample pattern.

```python
 import pravega_client
 manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
# create a transactional writer against an already created Pravega scope and Stream.
 writer=manager.create_transaction_writer("scope", "stream", "123")
 ```

A ReaderGroup can be created using the StreamManager. Individual readers can be created using this ReaderGroup object.

```python
import pravega_client
manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
# create a ReaderGroup rg1 against an already created Pravega scope and Stream.
event.reader_group=manager.create_reader_group("rg1", "scope", "stream")
``` 

## StreamWriter

A StreamWriter object created using the StreamManager can be used to write events into the Pravega Stream. The events 
that are written will appear in the Stream exactly once. The event of type String is converted into bytes with `UTF-8` encoding.
The user can optionally specify the routing key. The StreamWriter can also write a byte array into the Pravega Stream 
with/without a routing key.

Note that the implementation provides retry logic to handle connection failures and service host failures. Internal 
retries will not violate the exactly-once semantic, so it is better to rely on them than to wrap this with custom retry logic.

```python
import pravega_client
manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
# assuming the Pravega scope and stream are already created.
writer=manager.create_writer("scope", "stream")
# write into Pravega stream without specifying the routing key.
writer.write_event("e1")
# write into Pravega stream by specifying the routing key.
writer.write_event("e2", "key1") 
e="eventData"                                                   
# convert the event object to a byte array.
e_bytes=e.encode("utf-8")                                       
# write into Pravega stream without specifying the routing key.    
writer.write_event_bytes(e_bytes)                                    
# write into Pravega stream by specifying the routing key.       
writer.write_event_bytes(e_bytes, "key1")    
```

## TransactionWriter
A TransactionWriter created using a StreamManager  writes Events to an Event stream transactionally. All events that are 
written as part of a transaction can be committed atomically by calling commit(). This will result in either all of those 
events going into the stream or none of them and the commit call failing with an exception.

Prior to committing a transaction, the events written to it cannot be read or otherwise seen by readers.

```python
import pravega_client                                         
manager = pravega_client.StreamManager("tcp://127.0.0.1:9090")        
w1 = stream_manager.create_transaction_writer(scope,"testTxn", 1)
# Begin a Transaction
txn1 = w1.begin_txn()
txn1.write_event("test event1")
txn1.write_event("test event2")
self.assertTrue(txn1.is_open(), "Transaction is open")
# Commit a Transaction
txn1.commit()
                      
txn2 = w1.begin_txn()
txn2.write_event("test event1")
txn2.write_event("test event2")
self.assertTrue(txn2.is_open(), "Transaction is open")
# Abort a transaction, none of the events will be written into the Stream.
txn2.abort()
```

## EventReader

One or more EventReaders can be created from the ReaderGroup object created using the StreamManager. Every reader has 
an async method call `reader.get_segment_slice_async()` that returns a Python Future which completes when a segment slice 
is acquired for consumption.  It can contain one or more events and the user can iterate over the segment slice to read 
the events. If there are multiple segments in the stream then this API can return a segment slice of any segments in the
stream. The reader ensures that events returned by the stream are in order.

If multiple readers are created under the same reader group then the readers co-ordinate amongst themselves and divide 
the responsibility of reading from different segments in the Pravega stream.

A sample example snippet is shown below.

```python
import pravega_client
manager = pravega_client.StreamManager("tcp://127.0.0.1:9090")
# assuming the Pravega scope and stream are already created.
reader_group = manager.create_reader_group("rg1", "scope", "stream")
reader = reader_group.create_reader("reader_id");
slice = await reader.get_segment_slice_async()
for event in slice:
    print(event.data())
```
