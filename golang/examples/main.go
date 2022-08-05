package main

import (
	"fmt"
	"time"

	client "github.com/pravega/pravega-client-rust/golang/pkg"
)

func main() {
	scope := "foo"
	stream := "bar"

	config := client.NewClientConfig()
	config.ControllerUri = "127.0.0.1:9090"

	manager, err := client.NewStreamManager(config)
	if err != nil {
		println("fail to create stream manager:", err.Error())
		return
	}
	defer manager.Close()

	res, err := manager.CreateScope(scope)
	if err != nil {
		println("fail to create scope:", err.Error())
		return
	}
	println("create scope:", res)
	streamConfig := client.NewStreamConfiguration(scope, stream)
	streamConfig.Scale.MinSegments = 3
	res, err = manager.CreateStream(streamConfig)
	if err != nil {
		println("fail to create stream:", err.Error())
		return
	}
	println("create stream:", res)

	writer, err := manager.CreateWriter(scope, stream)
	if err != nil {
		println("fail to create writer:", err.Error())
		return
	}
	defer writer.Close()
	println("stream writer created")

	writer.WriteEvent([]byte("hello"))

	err = writer.Flush()
	if err != nil {
		println("fail to flush:", err.Error())
	}
	println("event wrote and flushed")

	unixMilli := time.Now().UnixMilli()
	rgName := fmt.Sprintf("rg%d", unixMilli)
	rg, err := manager.CreateReaderGroup(rgName, scope, stream, false)
	if err != nil {
		println("failed to create reader group:", err.Error())
		return
	}
	defer rg.Close()
	println("reader group created")

	reader, err := rg.CreateReader("reader1")
	if err != nil {
		println("failed to create reader:", err.Error())
		return
	}
	defer reader.Close()
	println("reader created")

	slice, err := reader.GetSegmentSlice()
	if err != nil {
		println("failed to get segment slice:", err.Error())
		return
	}
	defer slice.Close()
	event, err := slice.Next()
	if err != nil {
		println("failed to read event:", err.Error())
		return
	}
	if event != nil {
		println("read event:", string(event))
	}
}
