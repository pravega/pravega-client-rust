package pkg

/*
#cgo LDFLAGS: -L../../target/debug/ -lpravega_client_c -Wl,-rpath,../target/debug/,-rpath,../../target/debug/

#include "pravega_client.h"
*/
import "C"
import (
	"fmt"
	"reflect"
	"unsafe"
)

type (
	cu8_ptr = *C.uint8_t
	usize   = C.uintptr_t
	cusize  = C.size_t
	cint    = C.int
	ci32    = C.int32_t
)

func receiveSlice(b C.Buffer) []byte {
	if emptyBuf(b) {
		return nil
	}
	res := C.GoBytes(unsafe.Pointer(b.ptr), cint(b.len))
	C.free_buffer(b)
	return res
}

func emptyBuf(b C.Buffer) bool {
	return b.ptr == cu8_ptr(nil) || b.len == usize(0) || b.cap == usize(0)
}

func errorWithMessage(err error, b C.Buffer) error {
	msg := receiveSlice(b)
	if msg == nil {
		return err
	}
	return fmt.Errorf("%s", string(msg))
}

func freeCString(str *C.char) {
	C.free(unsafe.Pointer(str))
}

// https://chai2010.cn/advanced-go-programming-book/ch2-cgo/ch2-07-memory.html
// makeView creates a view into the given byte slice what allows Rust code to read it.
// The byte slice is managed by Go and will be garbage collected. Use runtime.KeepAlive
// to ensure the byte slice lives long enough.
func makeView(s string) C.ByteSliceView {
	p := (*reflect.StringHeader)(unsafe.Pointer(&s))

	return C.ByteSliceView{
		ptr: cu8_ptr(unsafe.Pointer(p.Data)),
		len: cusize(len(s)),
	}
}
