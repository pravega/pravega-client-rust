package pkg

import "C"

import (
	"fmt"
	"sync"
	"unsafe"
)

var channelMap sync.Map

var chanIdCounter int32 = 0
var chanIdCounterMutex sync.Mutex

func CreateChannel() (int32, chan unsafe.Pointer) {
	channel := make(chan unsafe.Pointer)

	// register the id and channel
	chanIdCounterMutex.Lock()
	defer chanIdCounterMutex.Unlock()
	chanIdCounter += 1
	channelMap.Store(chanIdCounter, channel)
	return chanIdCounter, channel
}

type Bridge struct {
	ChanId   int32
	ObjPtr   unsafe.Pointer
}

// We declare the channel at this level, as the exported method `publishBridge` needs to notify this channel 
var bridgeChannel chan Bridge

//export publishBridge
func publishBridge(chanId int32, objPtr uintptr, errorMsg *C.char) {
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
