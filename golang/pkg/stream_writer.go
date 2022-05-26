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

func (writer *StreamWriter) WriteEvent(event string, routingKey string) error {
	buf := C.Buffer{}
	eventBytes := []byte(event)
	routingKeyBytes := []byte(routingKey)

	e := makeView(eventBytes)
	defer runtime.KeepAlive(eventBytes)
	r := makeView(routingKeyBytes)
	defer runtime.KeepAlive(routingKeyBytes)
	_, err := C.stream_writer_write_event(writer.Writer, e, r, &buf)
	if err != nil {
		return errorWithMessage(err, buf)
	}
	return nil
}

func (writer *StreamWriter) Flush() error {
	buf := C.Buffer{}
	_, err := C.stream_writer_flush(writer.Writer, &buf)
	if err != nil {
		return errorWithMessage(err, buf)
	}
	return nil
}
