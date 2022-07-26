package pkg

// #include "pravega_client.h"
import "C"
import (
	"runtime"

	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
)

type StreamWriter struct {
	Writer *C.StreamWriter
	Sem    *semaphore.Weighted
	Ctx    context.Context
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

	id, channel := registerOperation()
	cId := ci64(id)
	writer.Sem.Acquire(writer.Ctx, 1)
	_, err := C.stream_writer_write_event(writer.Writer, e, r, cId, &msg)
	if err != nil {
		return errorWithMessage(err, msg)
	}
	// TODO: may add timeout here
	<-channel
	writer.Sem.Release(1)

	return nil
}

func (writer *StreamWriter) WriteEvent(event []byte) error {
	return writer.WriteEventByRoutingKey("", event)
}

func (writer *StreamWriter) Flush() error {
	msg := C.Buffer{}

	id, channel := registerOperation()
	cId := ci64(id)

	_, err := C.stream_writer_flush(writer.Writer, cId, &msg)
	if err != nil {
		return errorWithMessage(err, msg)
	}
	// TODO: may add timeout here
	<-channel

	return nil
}
