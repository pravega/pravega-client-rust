package pkg

import "C"

import (
	"log"
	"sync"
	"sync/atomic"
	"unsafe"
)

var operationMap sync.Map

var operationIdCounter int64 = 0

// Register current request, will get response when rust side call ackOperationDone later.
func registerOperation() (int64, chan unsafe.Pointer) {
	channel := make(chan unsafe.Pointer)

	// register the id and channel
	// TODO: handle id overflow
	id := atomic.AddInt64(&operationIdCounter, 1)
	operationMap.Store(id, channel)
	return id, channel
}

type Operation struct {
	Id     int64
	ObjPtr unsafe.Pointer
}

// We declare the channel at this level, as the exported method `ackOperationDone` needs to notify this channel
var operationDoneAckChannel chan Operation

//export ackOperationDone
func ackOperationDone(id int64, objPtr uintptr) {
	var op Operation = Operation{
		Id:     id,
		ObjPtr: unsafe.Pointer(objPtr),
	}
	operationDoneAckChannel <- op
}

var runReactorOnce sync.Once

// Receive the response from rust side and send it to corresponding channel to wake the related goroutine
func runReactor() {
	runReactorOnce.Do(func() {
		operationDoneAckChannel = make(chan Operation, 50)

		go func() {
			for {
				select {
				case op := <-operationDoneAckChannel:
					{
						if op.Id == -1 {
							log.Printf("Reactor received the stop signal, will exit.")
							break
						}
						value, loaded := operationMap.LoadAndDelete(op.Id)
						if !loaded {
							log.Printf("WARNING: Reactor received a unexpected operationId, will exit.")
							break
						}
						channel := (value).(chan unsafe.Pointer)
						channel <- op.ObjPtr
					}
				}
			}
		}()
	})
}

func stopReactor() {
	ackOperationDone(-1, 0)
}