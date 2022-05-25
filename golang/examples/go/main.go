package main

import (
	stream_manager "pravega-client/pkg"
)

func main() {
	manager, err := stream_manager.NewStreamManager("")
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
	println("create stream bar:", )
}
