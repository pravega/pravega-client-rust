package pkg

/*
#include "pravega_client.h"
*/
import "C"
import (
	"errors"
	"time"
)

// StreamReader is responsible for reading data from given stream.
type StreamReader struct {
	Reader *C.StreamReader
}

// Close the StreamReader. Codes in Rust side will release related resources.
func (reader *StreamReader) Close() {
	C.stream_reader_destroy(reader.Reader)
}

// getSegmentSlice reads from a channel and returns a SegmentSlice or an error.
// It waits for an object from the channel and processes it accordingly.
// If the object contains a valid slice, it returns a SegmentSlice with the corresponding data.
// If the object contains an error, it returns an error with the error message extracted from the object.
func (reader *StreamReader) GetSegmentSlice(timeout time.Duration) (*SegmentSlice, error) {
	id, channel := registerOperation()

	cId := ci64(id)
	C.stream_reader_get_segment_slice(reader.Reader, cId)

	select {
	case op := <-channel:
		if op.ObjPtr != nil {
			slice := (*C.Slice)(op.ObjPtr)

			return &SegmentSlice{
				Slice: slice,
			}, nil
		} else if op.ErrPtr != nil {
			errLen := C.int(op.ErrLen)
			errPtr := (*C.char)(op.ErrPtr)
			errMsg := C.GoStringN(errPtr, errLen)
			return nil, errors.New(errMsg)
		} else {
			return nil, errors.New("Unknown errors")
		}
	case <-time.After(timeout):
		return nil, errors.New("Timout occurred while getting segment slice")
	}
}

// SegmentSlice represents a slice of a segment that can be read by the user.
// To get a SegmentSlice, call reader.GetSegmentSlice()
// To read events from a SegmentSlice, call slice.Next()
type SegmentSlice struct {
	Slice *C.Slice
}

// Close the SegmentSlice. Codes in Rust side will release some resources.
func (slice *SegmentSlice) Close() {
	C.segment_slice_destroy(slice.Slice)
}

// Get the next event. If no more event, will return nil.
func (slice *SegmentSlice) Next() ([]byte, error) {
	msg := C.Buffer{}
	event := C.Buffer{}
	_, err := C.segment_slice_next(slice.Slice, &event, &msg)
	if err != nil {
		return nil, errorWithMessage(err, msg)
	}
	data := copyAndDestroyBuffer(event)
	return data, nil
}
