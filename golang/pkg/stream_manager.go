package pkg

/*
#cgo LDFLAGS: -L../../target/debug/ -lpravega_client_c -Wl,-rpath,../target/debug/

#include "pravega_client.h"
*/
import "C"

type StreamManager struct {
	Manager *C.StreamManager
}

func NewStreamManager(uri string) (*StreamManager, error) {
	buf := C.Buffer{}
	uriCString := C.CString(uri)
	manager, err := C.stream_manager_new(uriCString, &buf)
	if err != nil {
		return nil, errorWithMessage(err, buf)
	}
	return &StreamManager{
		Manager: manager,
	}, err
}

func (manager *StreamManager) CreateScope(scope string) (bool, error) {
	buf := C.Buffer{}
	scopeCString := C.CString(scope)
	result, err := C.stream_manager_create_scope(manager.Manager, scopeCString, &buf)
	if err != nil {
		return false, errorWithMessage(err, buf)
	}
	return bool(result), nil
}

func (manager *StreamManager) CreateStream(scope string, stream string, num int32) (bool, error) {
	buf := C.Buffer{}
	scopeCString := C.CString(scope)
	streamCString := C.CString(stream)
	numInt32 := C.int32_t(num)
	result, err := C.stream_manager_create_stream(manager.Manager, scopeCString, streamCString, numInt32, &buf)
	if err != nil {
		return false, errorWithMessage(err, buf)
	}
	return bool(result), nil
}

func (manager *StreamManager) Close() {
	C.stream_manager_destroy(manager.Manager)
}
