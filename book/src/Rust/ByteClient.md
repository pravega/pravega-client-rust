# Byte Client

Unlike EventWriter, ByteWriter writes raw bytes into Pravega Stream
without adding any headers or encoding. This means that the data stored in the Stream is a continuous
stream of bytes. Because of that, data written by ByteWriter can only be read by
ByteReader. 

The Byte Client is useful in cases where a raw stream of bytes is desirable such as video streaming.

## Example walkthrough
### Create a ClientFactory
Applications should use `ClientFactory` to initialize components. The client doesn't expose
the underlying `new` method to users.

```rust
// assuming pravega controller is listening at localhost:9090
let config = ClientConfigBuilder::default()
    .controller_uri("localhost:9090")
    .build()
    .expect("creating config");

let client_factory = ClientFactory::new(config);
```
### Create a Byte Writer
Assuming a new stream `mystream` has been created under scope `myscope` and
it contains a segment whose segmentId is 0.
```rust
let segment = ScopedSegment::from("myscope/mystream/0");
let mut byte_writer = client_factory.create_byte_writer(segment);
```
### Seek to tail
Sometimes applications use Byte Writer to write to a segment that 
contains some preexisting data. In this case, we need to find out
the tail offset of the segment first.
```rust
byte_writer.seek_to_tail();
```
### Write some data
It doesn't mean the data is persisted on the server side
when write method returns `Ok()`, user should call `writer.flush()` to ensure
all data has been acknowledged by the server.
``` rust
let payload = "hello world".to_string().into_bytes();
byte_writer.write(&payload).expect("write");
byte_writer.flush().expect("flush");
```

### Truncate the segment
`ByteWriter` can also truncate the segment. Truncation means that the
data prior to some offset is not needed anymore. Truncated data cannot be read. 
Applications use truncation to save space on the server.
```rust
byte_writer.truncate_data_before(4).await.expect("truncate segment");
```

### Seal the segment
Sealing a segment is basically to make a segment read-only.
```rust
byte_writer.seal().await.expect("seal segment");
```

### Create a Byte Reader
Create a `ByteReader` to read the same segment we just write.
```rust
let segment = ScopedSegment::from("myscope/mystream/0");
let mut byte_reader = client_factory.create_byte_reader(segment);
```

### Read from an offset
`ByteReader` can seek to any offset in the segment and read from it. It also 
provides a useful method to show the current head of the readable offset. Remember that 
the truncated data is not readable anymore, so the current head of the readable offset
is the truncation offset.

`read` method will block until some data are fetched from the server. If returned `size` is 0,
then it reaches the end of segment and no more data could be read from this offset.
```rust
let offset = byte_reader.current_head().await.expect("get current head offset");
let mut buf: Vec<u8> = vec![0; 4];
let size = byte_reader.read(&mut buf).await.expect("read from byte stream");
```

### Put it together
```rust
use pravega_client_config::ClientConfigBuilder;
use pravega_client::client_factory::ClientFactory;
use pravega_client_shared::ScopedSegment;
use std::io::Write;

#[tokio::main]
async fn main() {
    let config = ClientConfigBuilder::default()
     .controller_uri("localhost:9090")
     .build()
     .expect("creating config");
    
    let client_factory = ClientFactory::new(config);
    
    let segment = ScopedSegment::from("myscope/mystream/0");
    
    // write
    let mut byte_writer = client_factory.create_byte_writer(segment);
    byte_writer.seek_to_tail().await;
    
    let payload = "hello world".to_string().into_bytes();
    
    byte_writer.write(&payload).await.expect("write");
    byte_writer.flush().await.expect("flush");

    byte_writer.truncate_data_before(4).await.expect("truncate segment");

    byte_writer.seal().await.expect("seal segment");

    // read
    let segment = ScopedSegment::from("myscope/mystream/0");
    let mut byte_reader = client_factory.create_byte_reader(segment).await;

    let offset = byte_reader.current_head().await.expect("get current head offset");
    let mut buf: Vec<u8> = vec![0; 4];
    let size = byte_reader.read(&mut buf).await.expect("read from byte stream");
}
```

For more details please refer [API docs](../doc/pravega_client/byte).
