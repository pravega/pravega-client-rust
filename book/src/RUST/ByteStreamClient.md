# Byte Stream Client

Unlike EventStreamWriter, ByteStreamWriter writes raw bytes into Pravega Stream
without adding any headers or encoding. This means that the data stored in the Stream is a continuous
stream of bytes. Because of that, data written by ByteStreamWriter can only be read by
ByteStreamReader. 

The Byte Stream Client is useful in cases where a raw stream of bytes is desirable. For example in video streaming.

For more details please refer [API docs](./doc/pravega_client_rust/byte_stream/index.html).
