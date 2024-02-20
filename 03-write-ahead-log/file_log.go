package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// FileLog is a Log that is stored in an os.File.
type FileLog struct {
	file   *os.File
	buffer []byte
}

func NewFileLog(path string) (*FileLog, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return &FileLog{file: file}, nil
}

func (fl *FileLog) Append(record []byte) (offset uint64, err error) {
	// Seek to the end of the file to find the next offset to write to.
	signedOffset, err := fl.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Convert the signed offset to an unsigned offset.
	offset = uint64(signedOffset)

	// Calculate the length of the record.
	lenRecord := uint64(len(record))

	// Create a buffer to hold the length of the record, the record itself, and the checksum.
	buf := new(bytes.Buffer)

	// Write the length of the record to the buffer.
	err = binary.Write(buf, binary.BigEndian, lenRecord)
	if err != nil {
		return 0, err
	}

	// Write the record to the buffer.
	_, err = buf.Write(record)
	if err != nil {
		return 0, err
	}

	// Calculate the checksum.
	checksum := crc32.ChecksumIEEE(record)

	// Write the checksum to the buffer.
	err = binary.Write(buf, binary.BigEndian, checksum)
	if err != nil {
		return 0, err
	}

	// Write the buffer to the file at the found offset.
	_, err = fl.file.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}

	// Return the offset where the new record was written.
	return offset, nil
}

func (fl *FileLog) Read(offset uint64) (record []byte, nextOffset uint64, err error) {
	// Seek to the offset.
	_, err = fl.file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, 0, err
	}

	// Read the length of the record.
	var lenRecord uint64
	err = binary.Read(fl.file, binary.BigEndian, &lenRecord)
	if err != nil {
		return nil, 0, err
	}

	// Read the record.
	record = make([]byte, lenRecord)
	_, err = io.ReadFull(fl.file, record)
	if err != nil {
		return nil, 0, err
	}

	// Read the checksum.
	var checksum uint32
	err = binary.Read(fl.file, binary.BigEndian, &checksum)
	if err != nil {
		return nil, 0, err
	}

	// Verify the checksum.
	if crc32.ChecksumIEEE(record) != checksum {
		return nil, 0, errors.New("data corruption detected")
	}

	// Return the record and the next offset.
	nextOffset = offset + 8 + lenRecord + 4
	return record, nextOffset, nil
}

func (fl *FileLog) Close() error {
	if fl.file != nil {
		err := fl.file.Close()
		if err != nil {
			return err
		}
		fl.file = nil
	}
	return nil
}
