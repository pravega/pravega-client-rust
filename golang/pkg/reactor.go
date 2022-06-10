package pkg

/*
#include <stdlib.h>

#include "common.h"
*/
import "C"

import (
	"fmt"
	"sync"
)

// We declare the channel at this level, chan.go needs to see this too!
type Bridge struct {
	chanId int32
	objPtr uintptr
}
var bridgeChannel chan Bridge

var channelMap sync.Map

// This is an exported function. It takes a *C.Char, converts it to a Go string and sends it to the channel.
//export publishString
func publishBridge(chanId int32, objPtr uintptr) {
	var bridge Bridge = Bridge {
		chanId: chanId,
		objPtr: objPtr,
	}
	bridgeChannel <- bridge
}

func ReactorRun() {
	bridgeChannel = make(chan Bridge)

	go func() {
		for {
			select {
			case bridge := <-bridgeChannel:
				channelInterface := channelMap.LoadAndDelete(bridge.chanId)
				channel := channelInterface.(chan uintptr)
				channel <- bridge.objPtr
			}
		}
	}()
}
