package pkg

// #include "pravega_client.h"
import "C"

type StreamManager struct {
	Manager *C.StreamManager
}

func NewStreamManager(uri string) (*StreamManager, error) {
	buf := C.Buffer{}
	cUri := C.CString(uri)
	manager, err := C.stream_manager_new(cUri, &buf)
	freeCString(cUri)
	if err != nil {
		return nil, errorWithMessage(err, buf)
	}
	return &StreamManager{
		Manager: manager,
	}, nil
}

func (manager *StreamManager) Close() {
	C.stream_manager_destroy(manager.Manager)
}

func (manager *StreamManager) CreateScope(scope string) (bool, error) {
	buf := C.Buffer{}
	cScope := C.CString(scope)
	result, err := C.stream_manager_create_scope(manager.Manager, cScope, &buf)
	freeCString(cScope)
	if err != nil {
		return false, errorWithMessage(err, buf)
	}
	return bool(result), nil
}

func (manager *StreamManager) CreateStream(scope string, stream string, num int32) (bool, error) {
	buf := C.Buffer{}
	cScope := C.CString(scope)
	cStream := C.CString(stream)
	cNum := ci32(num)
	result, err := C.stream_manager_create_stream(manager.Manager, cScope, cStream, cNum, &buf)
	freeCString(cScope)
	freeCString(cStream)
	if err != nil {
		return false, errorWithMessage(err, buf)
	}
	return bool(result), nil
}

func (manager *StreamManager) CreateWriter(scope string, stream string, maxInflightCount uint) (*StreamWriter, error) {
	buf := C.Buffer{}
	cScope := C.CString(scope)
	cStream := C.CString(stream)
	count := cusize(maxInflightCount)
	writer, err := C.stream_writer_new(manager.Manager, cScope, cStream, count, &buf)
	freeCString(cScope)
	freeCString(cStream)
	if err != nil {
		return nil, errorWithMessage(err, buf)
	}
	return &StreamWriter{
		Writer: writer,
	}, err
}
