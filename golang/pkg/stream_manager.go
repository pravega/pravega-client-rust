package pkg

// #include "pravega_client.h"
import "C"

type StreamManager struct {
	Manager *C.StreamManager
}

func NewStreamManager(config *ClientConfig) (*StreamManager, error) {
	runReactor()

	msg := C.Buffer{}
	cConfig := config.toCtype()
	manager, err := C.stream_manager_new(cConfig, &msg)
	freeClientConfig(&cConfig)
	if err != nil {
		return nil, errorWithMessage(err, msg)
	}
	return &StreamManager{
		Manager: manager,
	}, nil
}

func (manager *StreamManager) Close() {
	stopReactor()
	C.stream_manager_destroy(manager.Manager)
}

func (manager *StreamManager) CreateScope(scope string) (bool, error) {
	msg := C.Buffer{}
	cScope := C.CString(scope)
	result, err := C.stream_manager_create_scope(manager.Manager, cScope, &msg)
	freeCString(cScope)
	if err != nil {
		return false, errorWithMessage(err, msg)
	}
	return bool(result), nil
}

func (manager *StreamManager) CreateStream(scope string, stream string, num int32) (bool, error) {
	msg := C.Buffer{}
	scf := streamConfig.toCtype()
	result, err := C.stream_manager_create_stream(manager.Manager, *scf, &msg)
	freeStreamConfiguration(scf)
	if err != nil {
		return false, errorWithMessage(err, msg)
	}
	return bool(result), nil
}

func (manager *StreamManager) CreateWriter(scope string, stream string, maxInflightCount uint) (*StreamWriter, error) {
	msg := C.Buffer{}
	cScope := C.CString(scope)
	cStream := C.CString(stream)
	count := cusize(maxInflightCount)
	writer, err := C.stream_writer_new(manager.Manager, cScope, cStream, count, &msg)
	freeCString(cScope)
	freeCString(cStream)
	if err != nil {
		return nil, errorWithMessage(err, msg)
	}

	return &StreamWriter{
		Writer: writer,
	}, nil
}

func (manager *StreamManager) CreateReaderGroup(readerGroup string, scope string, stream string, readFromTail bool) (*StreamReaderGroup, error) {
	msg := C.Buffer{}
	cReaderGroup := C.CString(readerGroup)
	cScope := C.CString(scope)
	cStream := C.CString(stream)
	cReadFromTail := cbool(readFromTail)
	rg, err := C.stream_reader_group_new(manager.Manager, cReaderGroup, cScope, cStream, cReadFromTail, &msg)
	freeCString(cReaderGroup)
	freeCString(cScope)
	freeCString(cStream)
	if err != nil {
		return nil, errorWithMessage(err, msg)
	}
	return &StreamReaderGroup{
		ReaderGroup: rg,
	}, nil
}
