package log

import (
	"bytes"
	"encoding/gob"
	"io"
)

// WriteAheadLog keeps track of Key-Value pairs in a persistent manner.
// This means that if the program crashes, the Key-Value pairs will still be available on disk,
// and will be read back into memory when the program (using the WriteAheadLog) is restarted.
type WriteAheadLog struct {
	// The FileLog that the WriteAheadLog will write to.
	log *FileLog
	// The Key-Value data that the WriteAheadLog will write to the FileLog.
	// Note: string is used as the Key type because byte slices cannot be used as keys.
	//       So, the byte slice Key is converted to a string Key.
	data map[string][]byte
}

// WriteOperation is a single write operation that is performed to modify the WriteAheadLog.
type WriteOperation struct {
	WriteOperationType WriteOperationType
	Key                []byte
	Value              []byte
}

// WriteOperationType is the type of write operation that is being performed.
type WriteOperationType int

const (
	PUT    = 0
	DELETE = 1
)

func init() {
	gob.Register(WriteOperation{})
}

func NewWriteAheadLog(path string) (*WriteAheadLog, error) {
	log, err := NewFileLog(path)
	if err != nil {
		return nil, err
	}

	return NewWriteAheadLogWithFileLog(log)
}

func NewWriteAheadLogWithFileLog(log *FileLog) (*WriteAheadLog, error) {
	data, err := readAllLogEntries(log)
	if err != nil {
		return nil, err
	}

	return &WriteAheadLog{
		log:  log,
		data: data,
	}, nil
}

func (wal *WriteAheadLog) Get(key []byte) (value []byte, err error) {
	return wal.data[string(key)], nil
}

func (wal *WriteAheadLog) Put(key, value []byte) error {
	// Create a new WriteOperation object.
	op := WriteOperation{
		WriteOperationType: PUT,
		Key:                key,
		Value:              value,
	}

	// Create a new buffer to encode the WriteOperation object.
	buf := new(bytes.Buffer)

	// Encode the WriteOperation object.
	err := gob.NewEncoder(buf).Encode(op)
	if err != nil {
		return err
	}

	// Write the encoded WriteOperation object to the FileLog.
	_, err = wal.log.Append(buf.Bytes())
	if err != nil {
		return err
	}

	// Add the Key-Value pair to the map.
	wal.data[string(key)] = value
	return nil
}

func (wal *WriteAheadLog) Delete(key []byte) error {
	// Create a new WriteOperation object.
	op := WriteOperation{
		WriteOperationType: DELETE,
		Key:                key,
	}

	// Create a new buffer to encode the WriteOperation object.
	buf := new(bytes.Buffer)

	// Encode the WriteOperation object.
	err := gob.NewEncoder(buf).Encode(op)
	if err != nil {
		return err
	}

	// Write the encoded WriteOperation object to the FileLog.
	_, err = wal.log.Append(buf.Bytes())
	if err != nil {
		return err
	}

	// Remove the Key from the map.
	delete(wal.data, string(key))
	return nil
}

func readAllLogEntries(log *FileLog) (data map[string][]byte, err error) {
	// Initialize an empty map to store the Key-Value pairs.
	data = make(map[string][]byte)

	// Start reading from the beginning of the FileLog (offset 0).
	var offset uint64 = 0

	for {
		// Read the next record from the FileLog.
		record, nextOffset, err := log.Read(offset)
		if err != nil {
			if err == io.EOF {
				// If we've reached the end of the FileLog, break the loop.
				break
			} else {
				// If there was an error reading from the FileLog, return the error.
				return nil, err
			}
		}

		// Create a new Gob decoder.
		decoder := gob.NewDecoder(bytes.NewBuffer(record))

		// Decode the record into an WriteOperation object.
		var op WriteOperation
		if err := decoder.Decode(&op); err != nil {
			return nil, err
		}

		// Depending on the WriteOperationType of the WriteOperation, perform the corresponding operation on the map.
		switch op.WriteOperationType {
		case PUT:
			// If WriteOperationType is PUT, add the Key-Value pair to the map.
			data[string(op.Key)] = op.Value
		case DELETE:
			// If WriteOperationType is DELETE, remove the Key from the map.
			delete(data, string(op.Key))
		}

		// Move to the next record.
		offset = nextOffset
	}

	// Return the map.
	return data, nil
}
