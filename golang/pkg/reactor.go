package pkg

import "C"

import (
	"fmt"
	"sync"
	"unsafe"
)

// We declare the channel at this level, chan.go needs to see this too!
type Bridge struct {
	ChanId   int32
	ObjPtr   unsafe.Pointer
	ErrorMsg string
}

var bridgeChannel chan Bridge

var channelMap sync.Map

//export publishBridge
func publishBridge(chanId int32, objPtr uintptr, errorMsg *C.char) {
	var bridge Bridge = Bridge{
		ChanId:   chanId,
		ObjPtr:   unsafe.Pointer(objPtr),
		ErrorMsg: C.GoString(errorMsg),
	}
	bridgeChannel <- bridge
}

func ReactorRun() {
	bridgeChannel = make(chan Bridge)

	go func() {
		for {
			select {
			case bridge := <-bridgeChannel:
				{
					itf, loaded := channelMap.LoadAndDelete(bridge.ChanId)
					if !loaded {
						fmt.Printf("unexpect channelId: %d", bridge.ChanId)
						continue
					}
					channel := (itf).(chan Bridge)
					channel <- bridge
				}

			}
		}
	}()
}
