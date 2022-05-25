package pkg

/*
#cgo LDFLAGS: -L../../target/debug/ -lpravega_client_c -Wl,-rpath,../target/debug/

#include "pravega_client.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

type u8_ptr = *C.uint8_t
type usize = C.uintptr_t
type cint = C.int

func receiveSlice(b C.Buffer) []byte {
	if emptyBuf(b) {
		return nil
	}
	res := C.GoBytes(unsafe.Pointer(b.ptr), cint(b.len))
	C.free_buffer(b)
	return res
}

func emptyBuf(b C.Buffer) bool {
	return b.ptr == u8_ptr(nil) || b.len == usize(0) || b.cap == usize(0)
}

func errorWithMessage(err error, b C.Buffer) error {
	msg := receiveSlice(b)
	if msg == nil {
		return err
	}
	return fmt.Errorf("%s", string(msg))
}
