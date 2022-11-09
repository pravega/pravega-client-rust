package pkg

/*
#include "pravega_client.h"
*/
import "C"

// StreamReader is responsible for reading data from given stream.
type StreamReader struct {
	Reader *C.StreamReader
}

// Close the StreamReader. Codes in Rust side will release related resources.
func (reader *StreamReader) Close() {
	C.stream_reader_destroy(reader.Reader)
}

// Get SegmentSlice successfully representing there are some data avaiable for reading.
func (reader *StreamReader) GetSegmentSlice() (*SegmentSlice, error) {
	id, channel := registerOperation()

	cId := ci64(id)
	C.stream_reader_get_segment_slice(reader.Reader, cId)

	// TODO: may add timeout here
	ptr := <-channel
	slice := (*C.Slice)(ptr)

	return &SegmentSlice{
		Slice: slice,
	}, nil
}

// SegmentSlice represent the piece of a segment which contains multiple events and user can read it.
/*
	for {
		slice, err := reader.GetSegmentSlice()
		if err != nil {
			logger.Errorf("failed to get segment slice:", err.Error())
			return
		}

		for {
			event, err := slice.Next()
			if err != nil {
				logger.Errorf("failed to read event:", err.Error())
				return
			}
			if event != nil {
				fmt.Println(string(event))
			} else {
				break
			}
		}
		slice.Close()
	}
*/
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
