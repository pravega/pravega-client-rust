package pkg

// #include "pravega_client.h"
import "C"
import (
	"runtime"
)

type StreamWriter struct {
	Writer *C.StreamWriter
}

func (writer *StreamWriter) Close() {
	C.stream_writer_destroy(writer.Writer)
}

func (writer *StreamWriter) WriteEventByRoutingKey(routingKey string, event []byte) error {
	buf := C.Buffer{}
	defer runtime.KeepAlive(event)
	defer runtime.KeepAlive(routingKey)
	e := makeViewFromSlice(event)
	r := makeViewFromString(routingKey)
	_, err := C.stream_writer_write_event(writer.Writer, e, r, &buf)
	if err != nil {
		return errorWithMessage(err, buf)
	}
	return nil
}

func (writer *StreamWriter) WriteEvent(event []byte) error {
	return writer.WriteEventByRoutingKey("", event)
}

func (writer *StreamWriter) Flush() error {
	buf := C.Buffer{}
	_, err := C.stream_writer_flush(writer.Writer, &buf)
	if err != nil {
		return errorWithMessage(err, buf)
	}
	return nil
}
