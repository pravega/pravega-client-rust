package pkg

/*
#include "pravega_client.h"
*/
import "C"
import "unsafe"

type StreamReader struct {
	Reader *C.StreamReader
}

func (reader *StreamReader) Close() {
	C.stream_reader_destroy(reader.Reader)
}

func (reader *StreamReader) GetSegmentSlice() (*SegmentSlice, error) {
	channel := make(chan uintptr)

	// invoke rust code to get segment slice
	// register the id and channel
	id := 1
	channelMap.Store(id, channel)

	buf := C.Buffer{}
	err := C.stream_reader_get_segment_slice(reader.Reader, &buf)
	if err != nil {
		return nil, errorWithMessage(err, buf)
	}

	// wait to the ack
	slicePtr := <-channel
	slice := (*C.Slice)(unsafe.Pointer(slicePtr))

	return &SegmentSlice{
		Slice: slice,
	}, nil
}

func (reader *StreamReader) Read() {
	C.stream_reader_test()
}

type SegmentSlice struct {
	Slice *C.Slice
}

func (slice *SegmentSlice) Close() {
	C.segment_slice_destroy(slice.Slice)
}

func (slice *SegmentSlice) Next() ([]byte, error) {
	buf := C.Buffer{}
	event := C.Buffer{}
	_, err := C.segment_slice_next(slice.Slice, &event, &buf)
	if err != nil {
		return nil, errorWithMessage(err, buf)
	}
	data := receiveSlice(event)
	return data, nil
}
