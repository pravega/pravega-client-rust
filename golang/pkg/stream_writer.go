package pkg

// #include "pravega_client.h"
import "C"

type StreamWriter struct {
	Writer *C.StreamWriter
}

func (writer *StreamWriter) Close() {
	C.stream_writer_destroy(writer.Writer)
}

func (writer *StreamWriter) Flush() error {
	buf := C.Buffer{}
	_, err := C.stream_writer_flush(writer.Writer, &buf)
	if err != nil {
		return errorWithMessage(err, buf)
	}
	return nil
}
