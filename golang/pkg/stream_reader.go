package pkg

/*
#include "pravega_client.h"
*/
import "C"

type StreamReader struct {
	Reader *C.StreamReader
}

func (reader *StreamReader) Close() {
	C.stream_reader_destroy(reader.Reader)
}

func (reader *StreamReader) GetSegmentSlice() (*SegmentSlice, error) {
	id, channel := CreateChannel()

	msg := C.Buffer{}
	cId := ci64(id)

	// in the rust side, it will prepare the segment slice
	// and send through the channel once it is ready
	_, err := C.stream_reader_get_segment_slice(reader.Reader, cId, &msg)
	if err != nil {
		return nil, errorWithMessage(err, msg)
	}

	// wait for the ack
	// TODO: may add timeout here
	ptr := <-channel
	slice := (*C.Slice)(ptr)

	return &SegmentSlice{
		Slice: slice,
	}, nil
}

type SegmentSlice struct {
	Slice *C.Slice
}

func (slice *SegmentSlice) Close() {
	C.segment_slice_destroy(slice.Slice)
}

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
