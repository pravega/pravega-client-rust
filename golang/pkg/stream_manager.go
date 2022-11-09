package pkg

// #include "pravega_client.h"
import "C"

import (
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
)

// StreamManager is responsible for creating scope, stream, writers and readerGroup.
type StreamManager struct {
	Manager *C.StreamManager
}

// Create a StreamManager with specific clientConfig. It will start a reactor goroutine and waiting for the callback response from rust side.
// E.g.
//
//	config := client.NewClientConfig()
//	manager, err := client.NewStreamManager(config)
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

// Close the StreamManager. Stop the reactor goroutine and release the resources
func (manager *StreamManager) Close() {
	stopReactor()
	C.stream_manager_destroy(manager.Manager)
}

// Create scope with a given name. If scope has already exists, return false, else return true. If doesn't success, return error.
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

// Create stream with a given name.
// If stream has already exists, return false, else return true.
// If doesn't success, return error.
func (manager *StreamManager) CreateStream(streamConfig *StreamConfiguration) (bool, error) {
	msg := C.Buffer{}
	scf := streamConfig.toCtype()
	result, err := C.stream_manager_create_stream(manager.Manager, *scf, &msg)
	freeStreamConfiguration(scf)
	if err != nil {
		return false, errorWithMessage(err, msg)
	}
	return bool(result), nil
}

// Create EventStreamWriter with 15MB buffer.
// TODO: Make buffer size configurable.
func (manager *StreamManager) CreateWriter(scope string, stream string) (*StreamWriter, error) {
	msg := C.Buffer{}
	cScope := C.CString(scope)
	cStream := C.CString(stream)
	writer, err := C.stream_writer_new(manager.Manager, cScope, cStream, &msg)
	freeCString(cScope)
	freeCString(cStream)
	if err != nil {
		return nil, errorWithMessage(err, msg)
	}
	sem := semaphore.NewWeighted(15728640) // 15MB
	ctx := context.TODO()

	return &StreamWriter{
		Writer: writer,
		Sem:    sem,
		Ctx:    ctx,
	}, nil
}

// Create ReaderGroup for given scope/stream.
// If you want read latest data in the stream, set readFromTail=true, or read data from the very beginning.
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
