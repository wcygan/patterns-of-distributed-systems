package log

import (
	"bytes"
	"os"
	"testing"
)

func CreateFileLog(t *testing.T) (*FileLog, func()) {
	f, err := os.CreateTemp("", "log")
	if err != nil {
		t.Fatalf("cannot create temporary file: %v", err)
	}
	log := &FileLog{file: f}

	// Return a cleanup function that closes the file.
	cleanup := func() {
		if err := log.Close(); err != nil {
			t.Errorf("cannot close log: %v", err)
		}
	}

	return log, cleanup
}

func TestHappyPath(t *testing.T) {
	t.Parallel()
	log, cleanup := CreateFileLog(t)
	defer cleanup()

	// Append a record to the log.
	record := []byte("hello, world")
	offset, err := log.Append(record)
	if err != nil {
		t.Fatalf("cannot append record: %v", err)
	}

	// Read the record back from the log.
	got, _, err := log.Read(offset)
	if err != nil {
		t.Fatalf("cannot read record: %v", err)
	}
	if string(got) != string(record) {
		t.Errorf("got %q, want %q", got, record)
	}

	// Close the log.
	if err := log.Close(); err != nil {
		t.Errorf("cannot close log: %v", err)
	}
}

func TestReadReturnsCorrectData(t *testing.T) {
	t.Parallel()
	log, cleanup := CreateFileLog(t)
	defer cleanup()

	record := []byte("hello, world")
	offset, err := log.Append(record)
	if err != nil {
		t.Fatalf("cannot append record: %v", err)
	}

	readRecord, _, err := log.Read(offset)
	if err != nil {
		t.Fatalf("cannot read record: %v", err)
	}

	if !bytes.Equal(readRecord, record) {
		t.Errorf("got %q, want %q", readRecord, record)
	}
}

func TestCloseClosesFile(t *testing.T) {
	t.Parallel()
	log, cleanup := CreateFileLog(t)
	defer cleanup()

	if err := log.Close(); err != nil {
		t.Errorf("cannot close log: %v", err)
	}

	if log.file != nil {
		t.Errorf("file field is not nil after Close")
	}
}

func TestAppendAfterCloseReturnsError(t *testing.T) {
	t.Parallel()
	log, cleanup := CreateFileLog(t)
	defer cleanup()

	if err := log.Close(); err != nil {
		t.Errorf("cannot close log: %v", err)
	}

	_, err := log.Append([]byte("hello, world"))
	if err == nil {
		t.Errorf("Append after Close did not return an error")
	}
}

func TestReadAfterCloseReturnsError(t *testing.T) {
	t.Parallel()
	log, cleanup := CreateFileLog(t)
	defer cleanup()

	if err := log.Close(); err != nil {
		t.Errorf("cannot close log: %v", err)
	}

	_, _, err := log.Read(0)
	if err == nil {
		t.Errorf("Read after Close did not return an error")
	}
}

func TestReadWithInvalidOffsetReturnsError(t *testing.T) {
	t.Parallel()
	log, cleanup := CreateFileLog(t)
	defer cleanup()

	_, _, err := log.Read(12345)
	if err == nil {
		t.Errorf("Read with invalid offset did not return an error")
	}
}

func TestReadAtInvalidOffsets(t *testing.T) {
	t.Parallel()
	log, cleanup := CreateFileLog(t)
	defer cleanup()

	offset, err := log.Append([]byte("hello, world"))
	if err != nil {
		t.Fatalf("cannot append record: %v", err)
	}

	_, err = log.Append([]byte("goodbye, world"))
	if err != nil {
		t.Fatalf("cannot append record: %v", err)
	}

	// Try to read at an offset one behind a proper offset.
	_, _, err = log.Read(offset - 1)
	if err == nil {
		t.Errorf("Read at offset one behind did not return an error")
	}

	// Try to read at an offset one ahead a proper offset.
	_, _, err = log.Read(offset + 1)
	if err == nil {
		t.Errorf("Read at offset one ahead did not return an error")
	}
}

func TestMultipleEntries(t *testing.T) {
	t.Parallel()
	log, cleanup := CreateFileLog(t)
	defer cleanup()

	// Append multiple records to the log.
	records := [][]byte{
		[]byte("hello, world"),
		[]byte("goodbye, world"),
		[]byte("another record"),
	}

	offsets := make([]uint64, len(records))
	for i, record := range records {
		var err error
		offsets[i], err = log.Append(record)
		if err != nil {
			t.Fatalf("cannot append record: %v", err)
		}
	}

	// Read the records back from the log.
	for i, offset := range offsets {
		got, nextOffset, err := log.Read(offset)
		if err != nil {
			t.Fatalf("cannot read record: %v", err)
		}
		if string(got) != string(records[i]) {
			t.Errorf("got %q, want %q", got, records[i])
		}
		// If this is not the last record, check that the next offset matches the offset of the next record.
		if i < len(records)-1 && nextOffset != offsets[i+1] {
			t.Errorf("next offset got %v, want %v", nextOffset, offsets[i+1])
		}
	}
}
