package main

import (
	stream_manager "pravega-client/pkg"
)

func main() {
	manager, err := stream_manager.NewStreamManager("127.0.0.1:9090")
	if err != nil {
		println("fail to create stream manager:", err.Error())
		return
	}
	defer manager.Close()

	res, err := manager.CreateScope("foo")
	if err != nil {
		println("fail to create scope foo:", err.Error())
		return
	}
	println("create scope foo:", res)

	res, err = manager.CreateStream("foo", "bar", 1)
	if err != nil {
		println("fail to create stream bar:", err.Error())
		return
	}
	println("create stream bar:", res)

	writer, err := manager.CreateWriter("foo", "bar", 10)
	if err != nil {
		println("fail to create stream bar:", err.Error())
		return
	}
	defer writer.Close()
	println("stream writer created")
	
	err = writer.WriteEvent("hello", "world")
	if err != nil {
		println("fail to write event:", err.Error())
		return
	}
	println("event wrote")

	err = writer.Flush()
	if err != nil {
		println("fail to flush:", err.Error())
	}
}
