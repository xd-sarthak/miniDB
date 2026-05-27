package transaction

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/file"
	"github.com/xd-sarthak/miniDB/log"
	"github.com/xd-sarthak/miniDB/utils"
)

// log record for setting a boolean value in the database
// Transaction X changed a boolean value at block B, offset O, previous value V

// SetBoolRecord is the in-memory representation of a log record
// represents that transcation txnNum changed a boool
// at block + offset and the old value was value
type SetBoolRecord struct {
	LogRecord
	txNum     int
	offset    int
	value    bool
	block   *file.BlockID
}

// NewSetBoolRecord creates a new SetBoolRecord from the given page.
func NewSetBoolRecord(page *file.Page) (*SetBoolRecord, error) {
	operationPos := 0 // bytes 0-3 for operation code
	txnNumPos := operationPos + utils.IntSize // bytes 4-7 for transaction number
	txNum := page.GetInt(txnNumPos) // read transaction number from page

	fileNamePos := txnNumPos + utils.IntSize // bytes 8-... for file name
	fileName,err := page.GetString(fileNamePos) // read file name from page
	if err != nil {
		return nil, err
	}

	blockNumPos := fileNamePos + file.MaxLength(len(fileName)) // bytes ... for block number
	blockNum := page.GetInt(blockNumPos)
	block := &file.BlockID{File: fileName, BlockNum: int(blockNum)}

	offsetPos := blockNumPos + utils.IntSize // bytes ... for offset
	offset := page.GetInt(offsetPos)

	valuePos := offsetPos + utils.IntSize // bytes ... for value
	value := page.GetBool(valuePos)

	return &SetBoolRecord{
		txNum:   txNum,
		offset:  offset,
		value:   value,
		block:   block,
	}, nil
}

// OP returns the type of the log record, which is SetBool.
func (r *SetBoolRecord) OP() LogRecordType {
	return SetBool
}

// TxNumber returns the transaction number associated with this log record.
func (r *SetBoolRecord) TxNumber() int {
	return r.txNum
}

// String returns a string representation of the log record for debugging purposes.
func (r *SetBoolRecord) String() string {
	return fmt.Sprintf("<SETBOOL %d %s %d %t>", r.txNum, r.block, r.offset, r.value)
}

// Undo undoes the operation encoded by the log record, which is to set the boolean value back to its old value.
func (r *SetBoolRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(r.block); err != nil {
		return err
	}
	defer tx.Unpin(r.block)
	return tx.SetBool(r.block, r.offset, r.value, false)
}

// WriteSetBoolToLog writes a SetBool record to the log and returns its lsn.
func WriteSetBoolToLog(logManager *log.Manager, txNum int, block *file.BlockID, offset int, val bool) (int, error) {
	operationPos := 0
	txNumPos := operationPos + utils.IntSize
	fileNamePos := txNumPos + utils.IntSize
	fileName := block.Filename()

	blockNumPos := fileNamePos + file.MaxLength(len(fileName))
	blockNum := block.Number()

	offsetPos := blockNumPos + utils.IntSize
	valuePos := offsetPos + utils.IntSize

	// 1 byte for bool
	recordLen := valuePos + 1

	recordBytes := make([]byte, recordLen)
	page := file.NewPageFromBytes(recordBytes)

	page.SetInt(operationPos, int(SetBool))
	page.SetInt(txNumPos, txNum)
	if err := page.SetString(fileNamePos, fileName); err != nil {
		return -1, err
	}
	page.SetInt(blockNumPos, blockNum)
	page.SetInt(offsetPos, offset)
	page.SetBool(valuePos, val)

	return logManager.Append(recordBytes)
}
