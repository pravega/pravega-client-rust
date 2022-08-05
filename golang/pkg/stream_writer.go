package pkg

// #include "pravega_client.h"
import "C"
import (
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
	"runtime"
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

	e, length := makeViewFromSlice(event)
	defer runtime.KeepAlive(event)
	r := makeViewFromString(routingKey)
	defer runtime.KeepAlive(routingKey)

	writer.Sem.Acquire(writer.Ctx, int64(length))
	id, channel := registerOperation()
	cId := ci64(id)

	_, err := C.stream_writer_write_event(writer.Writer, e, r, cId, &msg)
	if err != nil {
		writer.Sem.Release(int64(length))
		return errorWithMessage(err, msg)
	}
	go func() {
		<-channel
		writer.Sem.Release(int64(length))
	}()

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

	<-channel
	return nil
}
