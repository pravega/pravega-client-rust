package pkg

// #include "pravega_client.h"
import "C"

type StreamManager struct {
	Manager *C.StreamManager
}

func NewStreamManager(uri string) (*StreamManager, error) {
	reactor := NewReactor("some reactor")
	reactor.Run()

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
	}, nil
}

func (manager *StreamManager) CreateReaderGroup(readerGroup string, scope string, stream string, readFromTail bool) (*StreamReaderGroup, error) {
	buf := C.Buffer{}
	cReaderGroup := C.CString(readerGroup)
	cScope := C.CString(scope)
	cStream := C.CString(stream)
	cReadFromTail := cbool(readFromTail)
	rg, err := C.stream_reader_group_new(manager.Manager, cReaderGroup, cScope, cStream, cReadFromTail, &buf)
	freeCString(cReaderGroup)
	freeCString(cScope)
	freeCString(cStream)
	if err != nil {
		return nil, errorWithMessage(err, buf)
	}
	return &StreamReaderGroup{
		ReaderGroup: rg,
	}, nil
}
