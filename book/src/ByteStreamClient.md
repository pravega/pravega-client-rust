# Byte Stream Client

Unlike EventStreamWriter, ByteStreamWriter writes bytes into Pravega Stream
without adding any headers, which means that the data stored in the Stream is a continuous
array of bytes. Because of that, data written by ByteStreamWriter can only be read by
ByteStreamReader. 

This API is useful in the context of video streaming where data are a continuous flow of bytes.

For more details please refer [API docs](./doc/pravega_client_rust/byte_stream/index.html).