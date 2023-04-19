package pkg

/*
#include "pravega_client.h"
*/
import "C"

type StreamReaderGroup struct {
	ReaderGroup *C.StreamReaderGroup
}

func (readerGroup *StreamReaderGroup) Close() {
	C.stream_reader_group_destroy(readerGroup.ReaderGroup)
}

// Create StreamReader with a given name.
func (readerGroup *StreamReaderGroup) CreateReader(reader string) (*StreamReader, error) {
	msg := C.Buffer{}
	cReader := C.CString(reader)
	r, err := C.stream_reader_group_create_reader(readerGroup.ReaderGroup, cReader, &msg)
	freeCString(cReader)
	if err != nil {
		return nil, errorWithMessage(err, msg)
	}
	return &StreamReader{
		Reader: r,
	}, nil
}
