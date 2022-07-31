package pkg

import "C"

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

var operationMap sync.Map

var operationIdCounter int64 = 0

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

// We declare the channel at this level, as the exported method `publishBridge` needs to notify this channel
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

func runReactor() {
	runReactorOnce.Do(func() {
		operationDoneAckChannel = make(chan Operation, 50)

		go func() {
			for {
				select {
				case op := <-operationDoneAckChannel:
					{
						value, loaded := operationMap.LoadAndDelete(op.Id)
						if !loaded {
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
