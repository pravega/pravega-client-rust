package pkg

// #include "pravega_client.h"
import "C"

import (
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
)

const CHANNEL_CAPACITY int64 = 16 * 1024 * 1024
const MAX_INT int = int(^uint(0) >> 1)

type Event struct {
	RoutingKey  	string
	Payload 		[]byte
}

type StreamWriter struct {
	Writer 		  	*C.StreamWriter
	EventChannel  	chan Event
	Ctx				context.Context
	Sem				*semaphore.Weighted
}

func (writer *StreamWriter) Close() {
	C.stream_writer_destroy(writer.Writer)
}

func (writer *StreamWriter) WriteEventByRoutingKey(routingKey string, payload []byte) error {
	var event Event = Event {
		RoutingKey: routingKey,
		Payload: 	payload,
	}

	size := len(payload)
	println("send", size)
	if err := writer.Sem.Acquire(writer.Ctx, int64(size)); err != nil {
		return err
	}
	writer.EventChannel <- event

	return nil
}

func (writer *StreamWriter) WriteEvent(event []byte) {
	writer.WriteEventByRoutingKey("", event)
}

func (writer *StreamWriter) Flush() error {
	msg := C.Buffer{}
	_, err := C.stream_writer_flush(writer.Writer, &msg)
	if err != nil {
		return errorWithMessage(err, msg)
	}
	return nil
}
