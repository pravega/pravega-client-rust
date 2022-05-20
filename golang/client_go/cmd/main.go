package main

import (
	streammanager "dellemc/pravega/goclient/stream-manager"
)

func main() {
	
	m := streammanager.NewStreamManager("127.0.0.1:9090")
	defer m.Close()
	
	println(m.CreateScope("dell2"))
}
