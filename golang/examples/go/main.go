package main

import (
	stream_manager "pravega-client/pkg/stream_manager"
)

func main() {
	manager := stream_manager.NewStreamManager("127.0.0.1:9090")
	defer manager.Close()

	println("create scope foo:", manager.CreateScope("foo"))
	println("create stream bar:", manager.CreateStream("foo", "bar", 1))
}
