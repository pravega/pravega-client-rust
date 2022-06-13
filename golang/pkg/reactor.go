package pkg

import "C"

import (
	"fmt"
	"sync"
	"unsafe"
	"sync/atomic"
)

var channelMap sync.Map

var chanIdCounter int64 = 0
func CreateChannel() (int64, chan unsafe.Pointer) {
	channel := make(chan unsafe.Pointer)

	// register the id and channel
	chanId := atomic.AddInt64(&chanIdCounter, 1)
	channelMap.Store(chanId, channel)
	return chanId, channel
}

type Bridge struct {
	ChanId   int64
	ObjPtr   unsafe.Pointer
}

// We declare the channel at this level, as the exported method `publishBridge` needs to notify this channel 
var bridgeChannel chan Bridge

//export publishBridge
func publishBridge(chanId int64, objPtr uintptr, errorMsg *C.char) {
	var bridge Bridge = Bridge {
		ChanId: chanId,
		ObjPtr: unsafe.Pointer(objPtr),
	}
	bridgeChannel <- bridge
}

func RunReactor() {
	bridgeChannel = make(chan Bridge)

	go func() {
		for {
			select {
			case bridge := <- bridgeChannel:
				{
					value, loaded := channelMap.LoadAndDelete(bridge.ChanId)
					if !loaded {
						fmt.Printf("unexpect channelId: %d", bridge.ChanId)
						break
					}
					channel := (value).(chan unsafe.Pointer)
					channel <- bridge.ObjPtr
				}

			}
		}
	}()
}
