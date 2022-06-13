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
	channel := make(chan Bridge)

	// register the id and channel
	var id int32 = 12345
	channelMap.Store(id, channel)

	buf := C.Buffer{}
	cId := ci32(id)

	// in the rust side, it will prepare the segment slice 
	// and send through the channel once it is ready
	_, err := C.stream_reader_get_segment_slice(reader.Reader, cId, &buf)
	if err != nil {
		return nil, errorWithMessage(err, buf)
	}

	// wait for the ack
	result := <-channel
	slice := (*C.Slice)(result.ObjPtr)

	return &SegmentSlice{
		Slice: slice,
	}, nil
}

func (reader *StreamReader) Read() {
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
