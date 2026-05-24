package transaction

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/log"
	"github.com/xd-sarthak/miniDB/file"
	"github.com/xd-sarthak/miniDB/utils"
)

type StartRecord struct {
	LogRecord
	txnNum int
}

// NewStartRecord creates a new StartRecord for the given transaction number.
func NewStartRecord(page *file.Page) (*StartRecord, error) {
	operationPos := 0
	txnNumPos := operationPos + utils.IntSize
	txNum := page.GetInt(txnNumPos)

	return &StartRecord{
		txnNum: txNum,
	}, nil
}

// Op returns the type of the log record, which is Start.
func (sr *StartRecord) Op() LogRecordType {
	return Start
}

// TxnNumber returns the transaction number associated with this StartRecord.
func (sr *StartRecord) TxnNumber() int {
	return sr.txnNum
}

// Undo does nothing for StartRecords, as they do not require undo operations.
func (sr *StartRecord) Undo(_ *Transaction) error {
	// Start records do not require undo operations, so this method is a no-op.
	return nil
}

func (sr *StartRecord) String() string {
	return fmt.Sprintf("<START %d>", sr.txnNum)
}

func WriteStartToLog(logManager *log.Manager, txnNum int) (int, error){
	record := make([]byte, 2*utils.IntSize)
	page := file.NewPageFromBytes(record)
	page.SetInt(0, int(Start))
	page.SetInt(utils.IntSize, int(txnNum))

	return logManager.Append(record)
}