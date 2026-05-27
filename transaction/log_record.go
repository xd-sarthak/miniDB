package transaction

import (
	"errors"
	"github.com/xd-sarthak/miniDB/file"
)

type LogRecordType int

const (
	Checkpoint LogRecordType = iota
	Start
	Commit
	Rollback
	SetInt
	SetString
	SetBool
	SetDate
	SetLong
	SetShort
)

func (lrt LogRecordType) String() string {
	switch lrt {
	case Checkpoint:
		return "CHECKPOINT"
	case Start:
		return "START"
	case Commit:
		return "COMMIT"
	case Rollback:
		return "ROLLBACK"
	case SetInt:
		return "SETINT"
	case SetString:
		return "SETSTRING"
	default:
		return "UNKNOWN"
	}
}

func FromCode(code int) (LogRecordType, error) {
	switch code {
	case 0:
		return Checkpoint, nil
	case 1:
		return Start, nil
	case 2:
		return Commit, nil
	case 3:
		return Rollback, nil
	case 4:
		return SetInt, nil
	case 5:
		return SetString, nil
	default:
		return -1, errors.New("invalid log record type code")
	}
}

type LogRecord interface {
	// OP returns the type of the log record.
	OP() LogRecordType

	// TxNum returns the transaction number associated with the log record, if applicable.
	TxNum() int

	// Undo undoes the operation encoded by the log record, if applicable.
	Undo(tx *Transaction) error
}

// CreateLogRecord interprets the bytes to create the appropriate LogRecord based on the log record type code.
func CreateLogRecord(data []byte) (LogRecord, error){
	p := file.NewPageFromBytes(data)
	opCode := p.GetInt(0)
	logRecordType, err := FromCode(int(opCode))
	if err != nil {
		return nil, err
	}

	switch logRecordType {
	case Checkpoint:
		return NewCheckpointRecord()
	case Start:
		return NewStartRecord(p)
	case Commit:
		return NewCommitRecord(p)
	case Rollback:
		return NewRollbackRecord(p)
	case SetInt:
		return NewSetIntRecord(p)
	case SetString:
		return NewSetStringRecord(p)
	default:
		return nil, errors.New("unknown log record type")
	}
}