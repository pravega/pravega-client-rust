package pkg

/*
#cgo LDFLAGS: -L${SRCDIR}/../../target/release/ -lpravega_client_c -Wl,-rpath,${SRCDIR}/../../target/release/
#include "pravega_client.h"
*/
import "C"
import (
	"fmt"
	"reflect"
	"unsafe"
)

type (
	cu8ptr = *C.uint8_t
	usize  = C.uintptr_t
	cusize = C.size_t
	cint   = C.int
	ci32   = C.int32_t
	cu32   = C.uint32_t
	ci64   = C.int64_t
	cu64   = C.uint64_t
	cbool  = C.bool
)

func copyAndDestroyBuffer(b C.Buffer) []byte {
	if emptyBuffer(b) {
		return nil
	}
	res := C.GoBytes(unsafe.Pointer(b.ptr), cint(b.len))
	C.free_buffer(b)
	return res
}

func emptyBuffer(b C.Buffer) bool {
	return b.ptr == cu8ptr(nil) || b.len == usize(0) || b.cap == usize(0)
}

func errorWithMessage(err error, b C.Buffer) error {
	msg := copyAndDestroyBuffer(b)
	if msg == nil {
		return err
	}
	return fmt.Errorf("%s", string(msg))
}

func freeCString(str *C.char) {
	C.free(unsafe.Pointer(str))
}

// https://chai2010.cn/advanced-go-programming-book/ch2-cgo/ch2-07-memory.html
// https://github.com/CosmWasm/wasmvm/blob/main/api/memory.go
// makeView creates a view into the given byte slice which allows Rust code to read it.
// The byte slice is managed by Go and will be garbage collected. Use runtime.KeepAlive
// to ensure the byte slice lives long enough.
func makeViewFromString(s string) C.Buffer {
	p := (*reflect.StringHeader)(unsafe.Pointer(&s))

	return C.Buffer{
		ptr: cu8ptr(unsafe.Pointer(p.Data)),
		len: cusize(p.Len),
		cap: cusize(p.Len),
	}
}

func makeViewFromSlice(s []byte) (C.Buffer, int) {
	p := (*reflect.SliceHeader)(unsafe.Pointer(&s))

	return C.Buffer{
		ptr: cu8ptr(unsafe.Pointer(p.Data)),
		len: cusize(p.Len),
		cap: cusize(p.Cap),
	}, p.Len
}
