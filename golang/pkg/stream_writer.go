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
	msg := C.Buffer{}
	
	e := makeViewFromSlice(event)
	defer runtime.KeepAlive(event)
	r := makeViewFromString(routingKey)
	defer runtime.KeepAlive(routingKey)
	
	_, err := C.stream_writer_write_event(writer.Writer, e, r, &msg)
	if err != nil {
		return errorWithMessage(err, msg)
	}
	return nil
}

func (writer *StreamWriter) WriteEvent(event []byte) error {
	return writer.WriteEventByRoutingKey("", event)
}

func (writer *StreamWriter) Flush() error {
	msg := C.Buffer{}
	_, err := C.stream_writer_flush(writer.Writer, &msg)
	if err != nil {
		return errorWithMessage(err, msg)
	}
	return nil
}
