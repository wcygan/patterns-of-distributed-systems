package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPutAndGet(t *testing.T) {
	t.Parallel()
	dir, _ := os.MkdirTemp("", "wal")
	defer os.RemoveAll(dir)

	wal, _ := NewWriteAheadLog(dir + "/log")
	defer wal.log.Close()

	key := []byte("Key")
	value := []byte("Value")

	// Put a Key-Value pair into the WriteAheadLog.
	err := wal.Put(key, value)
	assert.NoError(t, err)

	// Get the Value of the Key from the WriteAheadLog.
	gotValue, err := wal.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, gotValue)
}

func TestDelete(t *testing.T) {
	t.Parallel()
	dir, _ := os.MkdirTemp("", "wal")
	defer os.RemoveAll(dir)

	wal, _ := NewWriteAheadLog(dir + "/log")
	defer wal.log.Close()

	key := []byte("Key")
	value := []byte("Value")

	// Put a Key-Value pair into the WriteAheadLog.
	err := wal.Put(key, value)
	assert.NoError(t, err)

	// Delete the Key from the WriteAheadLog.
	err = wal.Delete(key)
	assert.NoError(t, err)

	// Get the Value of the Key from the WriteAheadLog.
	// Since the Key was deleted, the Value should be nil.
	gotValue, err := wal.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, gotValue)
}

func TestGetNonExistentKey(t *testing.T) {
	t.Parallel()
	dir, _ := os.MkdirTemp("", "wal")
	defer os.RemoveAll(dir)

	wal, _ := NewWriteAheadLog(dir + "/log")
	defer wal.log.Close()

	key := []byte("nonexistent")

	// Get the Value of a nonexistent Key from the WriteAheadLog.
	// The Value should be nil.
	gotValue, err := wal.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, gotValue)
}

func TestPutAndGetAfterReopen(t *testing.T) {
	t.Parallel()
	dir, _ := os.MkdirTemp("", "wal")
	defer os.RemoveAll(dir)

	wal, _ := NewWriteAheadLog(dir + "/log")

	key := []byte("Key")
	value := []byte("Value")

	// Put a Key-Value pair into the WriteAheadLog.
	err := wal.Put(key, value)
	assert.NoError(t, err)

	// Get the file info before closing the WAL.
	assert.NoError(t, err)

	err = wal.log.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Reopen the WriteAheadLog.
	wal, _ = NewWriteAheadLog(dir + "/log")

	// Get the file info after reopening the WAL.
	assert.NoError(t, err)

	// Get the Value of the Key from the WriteAheadLog.
	// The Value should be the same as before, even after reopening the WriteAheadLog.
	gotValue, err := wal.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, gotValue)
}

func TestWriteMultipleRecordsAndReopen(t *testing.T) {
	t.Parallel()
	dir, _ := os.MkdirTemp("", "wal")
	defer os.RemoveAll(dir)

	wal, _ := NewWriteAheadLog(dir + "/log")

	// Create 10 key-value pairs.
	keys := make([][]byte, 10)
	values := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = []byte("Key" + string(rune(i)))
		values[i] = []byte("Value" + string(rune(i)))

		// Put each key-value pair into the WriteAheadLog.
		err := wal.Put(keys[i], values[i])
		assert.NoError(t, err)
	}

	err := wal.log.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Reopen the WriteAheadLog.
	wal, _ = NewWriteAheadLog(dir + "/log")

	// Get the value of each key from the WriteAheadLog and assert it's the same as before.
	for i := 0; i < 10; i++ {
		gotValue, err := wal.Get(keys[i])
		assert.NoError(t, err)
		assert.Equal(t, values[i], gotValue)
	}
}
