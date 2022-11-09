package pkg

// #include "pravega_client.h"
import "C"
import (
	"runtime"

	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
)

// StreamWriter is responsible for writing data to given stream.
type StreamWriter struct {
	// Rust binding reference
	Writer *C.StreamWriter
	// 15MB writer buffer. If there is no more buffer for new events, user goroutine will wait until there are enough buffer for writing.
	Sem *semaphore.Weighted
	Ctx context.Context
}

// Close the StreamWriter. Codes in Rust side will release related resources.
func (writer *StreamWriter) Close() {
	C.stream_writer_destroy(writer.Writer)
}

// Write event with given routing key. It's an async operation, if no enough buffer for new coming events, user golang will block on Sem.Acquire()
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

// write event with empty routingKey
// TODO: write event with random routingKey
func (writer *StreamWriter) WriteEvent(event []byte) error {
	return writer.WriteEventByRoutingKey("", event)
}

// Wait the all events been writen to pravega
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
