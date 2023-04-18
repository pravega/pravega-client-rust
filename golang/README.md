# Pravega Golang client.

This project provides a way to interact with [Pravega](http://pravega.io) using Golang client.

Pravega is an open source distributed storage service implementing Streams. It offers Stream as the main primitive for
the foundation of reliable storage systems: a high-performance, durable, elastic, and unlimited append-only byte stream
with strict ordering and consistency.

## Build and Run examples
```
# build dynamic library & generate header
cd golang
cargo build --release

# test go code
go build examples/main.go
./main
```

## How to use pravega go client

1. build rust dynamic library
cd golang
cargo build --release
cd .. # cd to rust project root directory
mv ./target/release/libpravega_client_c.so /usr/lib

2. import to your project with `go get`
```
go get github.com/pravega/pravega-client-rust/golang@latest
``` 
or you can replace the dependence with local version
```
go mod edit -replace github.com/pravega/pravega-client-rust/golang=/root/go/src/your_golang_code_path
```

3. Write your application with the reference to example code or bench_test code. As for how to set the clientConfig and streamConfig, you can read rust document as reference [ClientConfig](https://docs.rs/pravega-client-config/latest/pravega_client_config/struct.ClientConfig.html),[StreamConfig](https://github.com/pravega/pravega-client-rust/blob/master/shared/src/lib.rs#L508-L514)
4. Lets Write a sample application by using the go client step by step. Add import in your go application like this 
```gotemplate
import (
client "github.com/pravega/pravega-client-rust/golang/pkg"
)
```
5. Create ClientConfig 
```gotemplate
config := client.NewClientConfig()
config.ControllerUri = "127.0.0.1:9090"
```
6. Create new Stream Manager
```gotemplate
manager, _ := client.NewStreamManager(config)
defer manager.Close()
```
7. Create Scope and stream 
```gotemplate
scope := "scope"
stream := "stream"
res, _ := manager.CreateScope(scope)

streamConfig := client.NewStreamConfiguration(scope, stream)
streamConfig.Scale.MinSegments = 3
res, _ = manager.CreateStream(streamConfig)
```
8. Create Writer and write some events
```gotemplate
writer, _ := manager.CreateWriter(scope, stream)
defer writer.Close()
writer.WriteEvent([]byte("hello"))
_ = writer.Flush()
```
9. Create ReaderGroup and reader and read the written events
```gotemplate
rg, _ := manager.CreateReaderGroup(rgName, scope, stream, false)
defer rg.Close()

reader, _ := rg.CreateReader("reader1")
defer reader.Close()
slice, _ := reader.GetSegmentSlice()
defer slice.Close()
event, _ := slice.Next()
```
Close resources and handle errors appropriately in your application to ensure proper cleanup and error handling.

For more detailed usage and configuration options, refer to the Rust documentation for ClientConfig and StreamConfig.
