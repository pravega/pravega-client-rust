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

type (
	cu8_ptr  = *C.uint8_t
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

// https://github.com/CosmWasm/wasmvm
// makeView creates a view into the given byte slice what allows Rust code to read it.
// The byte slice is managed by Go and will be garbage collected. Use runtime.KeepAlive
// to ensure the byte slice lives long enough.
func makeView(s []byte) C.ByteSliceView {
	if s == nil {
		return C.ByteSliceView{is_nil: true, ptr: cu8_ptr(nil), len: cusize(0)}
	}

	// In Go, accessing the 0-th element of an empty array triggers a panic. That is why in the case
	// of an empty `[]byte` we can't get the internal heap pointer to the underlying array as we do
	// below with `&data[0]`. https://play.golang.org/p/xvDY3g9OqUk
	if len(s) == 0 {
		return C.ByteSliceView{is_nil: false, ptr: cu8_ptr(nil), len: cusize(0)}
	}

	return C.ByteSliceView{
		is_nil: false,
		ptr:    cu8_ptr(unsafe.Pointer(&s[0])),
		len:    cusize(len(s)),
	}
}
