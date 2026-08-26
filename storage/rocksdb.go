package storage

/*
#cgo LDFLAGS: -lrocksdb
#include <stdlib.h>
#include <rocksdb/c.h>
*/
import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"unsafe"
)

type Mutation struct {
	Key   []byte
	Value []byte
}

type Entry struct {
	Key   []byte
	Value []byte
}

type KV interface {
	Get([]byte) ([]byte, bool, error)
	Put([]byte, []byte) error
	Write([]Mutation) error
	ScanPrefix([]byte) ([]Entry, error)
	Close()
}

// RocksDB is a deliberately small wrapper over RocksDB's stable C API. Keeping
// the wrapper narrow makes the engine's state operations explicit: synchronous
// writes persist each aggregate batch before Kafka offsets can be committed.
type RocksDB struct {
	db      *C.rocksdb_t
	read    *C.rocksdb_readoptions_t
	write   *C.rocksdb_writeoptions_t
	options *C.rocksdb_options_t
}

func Open(path string) (*RocksDB, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("create rocksdb directory: %w", err)
	}
	options := C.rocksdb_options_create()
	C.rocksdb_options_set_create_if_missing(options, 1)
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var cErr *C.char
	db := C.rocksdb_open(options, cPath, &cErr)
	if err := nativeError(cErr); err != nil {
		C.rocksdb_options_destroy(options)
		return nil, fmt.Errorf("open rocksdb: %w", err)
	}
	if db == nil {
		C.rocksdb_options_destroy(options)
		return nil, errors.New("open rocksdb: native library returned no database handle")
	}
	write := C.rocksdb_writeoptions_create()
	C.rocksdb_writeoptions_set_sync(write, 1)
	return &RocksDB{db: db, read: C.rocksdb_readoptions_create(), write: write, options: options}, nil
}

func (r *RocksDB) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, errors.New("rocksdb key must not be empty")
	}
	var cErr *C.char
	var size C.size_t
	value := C.rocksdb_get(r.db, r.read, asCBytes(key), C.size_t(len(key)), &size, &cErr)
	if err := nativeError(cErr); err != nil {
		return nil, false, err
	}
	if value == nil {
		return nil, false, nil
	}
	defer C.rocksdb_free(unsafe.Pointer(value))
	return C.GoBytes(unsafe.Pointer(value), C.int(size)), true, nil
}

func (r *RocksDB) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("rocksdb key must not be empty")
	}
	var cErr *C.char
	C.rocksdb_put(r.db, r.write, asCBytes(key), C.size_t(len(key)), asCBytes(value), C.size_t(len(value)), &cErr)
	return nativeError(cErr)
}

func (r *RocksDB) Write(mutations []Mutation) error {
	batch := C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(batch)
	for _, mutation := range mutations {
		if len(mutation.Key) == 0 {
			return errors.New("rocksdb key must not be empty")
		}
		C.rocksdb_writebatch_put(batch, asCBytes(mutation.Key), C.size_t(len(mutation.Key)), asCBytes(mutation.Value), C.size_t(len(mutation.Value)))
	}
	var cErr *C.char
	C.rocksdb_write(r.db, r.write, batch, &cErr)
	return nativeError(cErr)
}

func (r *RocksDB) ScanPrefix(prefix []byte) ([]Entry, error) {
	if len(prefix) == 0 {
		return nil, errors.New("rocksdb scan prefix must not be empty")
	}
	iterator := C.rocksdb_create_iterator(r.db, r.read)
	defer C.rocksdb_iter_destroy(iterator)
	C.rocksdb_iter_seek(iterator, asCBytes(prefix), C.size_t(len(prefix)))
	entries := make([]Entry, 0)
	for C.rocksdb_iter_valid(iterator) != 0 {
		var keySize, valueSize C.size_t
		key := C.rocksdb_iter_key(iterator, &keySize)
		if !bytes.HasPrefix(C.GoBytes(unsafe.Pointer(key), C.int(keySize)), prefix) {
			break
		}
		value := C.rocksdb_iter_value(iterator, &valueSize)
		entries = append(entries, Entry{Key: C.GoBytes(unsafe.Pointer(key), C.int(keySize)), Value: C.GoBytes(unsafe.Pointer(value), C.int(valueSize))})
		C.rocksdb_iter_next(iterator)
	}
	var cErr *C.char
	C.rocksdb_iter_get_error(iterator, &cErr)
	if err := nativeError(cErr); err != nil {
		return nil, err
	}
	return entries, nil
}

func (r *RocksDB) Close() {
	if r.db != nil {
		C.rocksdb_close(r.db)
		r.db = nil
	}
	if r.read != nil {
		C.rocksdb_readoptions_destroy(r.read)
		r.read = nil
	}
	if r.write != nil {
		C.rocksdb_writeoptions_destroy(r.write)
		r.write = nil
	}
	if r.options != nil {
		C.rocksdb_options_destroy(r.options)
		r.options = nil
	}
}

func asCBytes(value []byte) *C.char {
	if len(value) == 0 {
		return nil
	}
	return (*C.char)(unsafe.Pointer(&value[0]))
}

func nativeError(value *C.char) error {
	if value == nil {
		return nil
	}
	defer C.rocksdb_free(unsafe.Pointer(value))
	return errors.New(C.GoString(value))
}
