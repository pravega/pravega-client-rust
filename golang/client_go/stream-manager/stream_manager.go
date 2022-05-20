package streammanager
/*
#cgo LDFLAGS: -L/root/rust/pravega-client-rust/target/debug/ -lpravega_client_c

#include "../pravega_client.h"
*/
import "C"

type StreamManager struct {
	Manager *C.StreamManager
}

func NewStreamManager(uri string) *StreamManager{
	str := C.CString(uri)
	m := C.stream_manager_new(str)
	return &StreamManager{
		Manager:m,
	}

}

func (manager *StreamManager )CreateScope(str string)bool{
	c_str := C.CString(str)
	result := C.stream_manager_create_scope(manager.Manager, c_str)
	
	return bool(result)
}

func (m *StreamManager )Close(){
	C.stream_manager_destroy(m.Manager)
}