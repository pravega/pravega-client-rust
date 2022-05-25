package stream_manager

/*
#cgo LDFLAGS: -L../../../target/debug/ -lpravega_client_c -Wl,-rpath,../target/debug/

#include "../pravega_client.h"
*/
import "C"

type StreamManager struct {
	Manager *C.StreamManager
}

func NewStreamManager(uri string) *StreamManager {
	uriCString := C.CString(uri)
	manager := C.stream_manager_new(uriCString)
	return &StreamManager{
		Manager: manager,
	}
}

func (manager *StreamManager) CreateScope(scope string) bool {
	scopeCString := C.CString(scope)
	result := C.stream_manager_create_scope(manager.Manager, scopeCString)

	return bool(result)
}

func (manager *StreamManager) CreateStream(scope string, stream string, num int32) bool {
	scopeCString := C.CString(scope)
	streamCString := C.CString(stream)
	numInt32 := C.int32_t(num)
	result := C.stream_manager_create_stream(manager.Manager, scopeCString, streamCString, numInt32)

	return bool(result)
}

func (manager *StreamManager) Close() {
	C.stream_manager_destroy(manager.Manager)
}
