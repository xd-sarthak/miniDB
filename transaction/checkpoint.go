package transaction

import (
	"github.com/xd-sarthak/miniDB/file"
	"github.com/xd-sarthak/miniDB/log"
)

type CheckpointRecord struct {
	LogRecord
}
// NewCheckpointRecord creates a new checkpoint log record.
func NewCheckpointRecord() (*CheckpointRecord, error) {
	return &CheckpointRecord{}, nil
}

// OP returns the type of the log record, which is Checkpoint.
func (cr *CheckpointRecord) OP() LogRecordType {
	return Checkpoint
}

// TxnNum returns -1 for checkpoint records, as they do not have an associated transaction number.
func (cr *CheckpointRecord) TxnNum() int {
	return -1 // Checkpoint records do not have a transaction number
}

func (cr *CheckpointRecord) Undo(_ *Transaction) error {
	// Checkpoint records do not require undo operations, so this method is a no-op.
	return nil
}

func (cr *CheckpointRecord) String() string {
	return "<CHECKPOINT>"
}

func WriteCheckpointToLog(logManager *log.Manager) (int, error) {
	// Create a checkpoint record
	record := make([]byte, 4)

	page := file.NewPageFromBytes(record)
	page.SetInt(0, int(Checkpoint))

	return logManager.Append(record)
}