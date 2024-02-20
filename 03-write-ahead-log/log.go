package log

type Log interface {
	// Append a record to the log.
	Append(record []byte) (offset uint64, error error)
	// Read a record from the log.
	Read(offset uint64) (record []byte, error error)
	// Close the log.
	Close() error
}
