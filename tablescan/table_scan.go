package tablescan

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/query"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/file"
	"github.com/xd-sarthak/miniDB/transaction"
	"time"
)

/*

cursor that walks through records stored across disk pages
abstraction


let table be like:

users.tbl

Block 0:
    Slot 0 -> Alice
    Slot 1 -> Bob

Block 1:
    Slot 0 -> Charlie
    Slot 1 -> David



TableScan abstracts that to

scan.Next()
scan.GetInt("id")
scan.Insert()
scan.Delete()

File:  [ Block 0 ] [ Block 1 ] [ Block 2 ] ...
                       ↑
              recordPage (pinned)
                   [ slot 0 | slot 1 | slot 2 | ... ]
                                  ↑
                             currentSlot

*/

// storage convention
// USER table -> users.tbl
const fileExtension = ".tbl"

var _ query.UpdateScan = (*TableScan)(nil) // compile-time assertion that TableScan implements UpdateScan

type TableScan struct {
	query.UpdateScan  // embedding the UpdateScan interface to implement it
	tx               *transaction.Transaction
	layout           *records.Layout
	recordPage       *records.Page
	fileName         string
	currentSlot      int
}

// NewTableScan initializes a new TableScan for the given table name and layout
func NewTableScan(tx *transaction.Transaction, tableName string, layout *records.Layout) (*TableScan, error) {

	if layout.SlotSize() > tx.BlockSize(){
		return nil, fmt.Errorf("record size %d exceeds block size %d", layout.SlotSize(), tx.BlockSize())
	}

	ts := &TableScan{
		tx:       tx,
		layout:   layout,
		fileName: tableName + fileExtension,
		currentSlot: -1, // start before the first record
	}

	size, err := tx.Size(ts.fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to get file size: %v", err)
	}

	if size == 0 {
		// If the file is empty, we need to create the first block
		err = ts.moveToNewBlock()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize table file: %v", err)
		}
	} else {
		if err := ts.moveToBlock(0); err != nil {
			return nil, fmt.Errorf("move to block 0: %w",err)
		}
	}

	return ts, nil
}

// moveToBlock moves the scan to the specified block number and initializes the record page
func (ts *TableScan) BeforeFirst() error {
	return ts.moveToBlock(0)
}

// Next advances the scan to the next record. 
// It returns false if there are no more records, and true otherwise.
/*


Try NextAfter(currentSlot) on current page
    ├── found → update currentSlot, return true
    └── not found (page exhausted)
            ├── at last block → return false (EOF)
            └── not last block → moveToBlock(n+1), try NextAfter(-1)


*/
func (ts *TableScan) Next() (bool, error) {
	slot, err := ts.recordPage.NextAfter(ts.currentSlot)

	if err != nil {
		atLastBlock, err := ts.atLastBlock()
		if err != nil {
			return false, err
		}
		if atLastBlock {
			return false, nil
		}
		if err := ts.moveToBlock(ts.recordPage.Block().Number() + 1); err != nil {
			return false, err
		}
		slot, err = ts.recordPage.NextAfter(-1)
		if err != nil {
			return false, nil
		}
	}

	ts.currentSlot = slot
	return true, nil
}


func (ts *TableScan) GetInt(fieldName string) (int, error) {
	return ts.recordPage.GetInt(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetLong(fieldName string) (int64, error) {
	return ts.recordPage.GetLong(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetShort(fieldName string) (int16, error) {
	return ts.recordPage.GetShort(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetString(fieldName string) (string, error) {
	return ts.recordPage.GetString(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetBool(fieldName string) (bool, error) {
	return ts.recordPage.GetBool(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetDate(fieldName string) (time.Time, error) {
	return ts.recordPage.GetDate(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetVal(fieldName string) (query.Constant, error) {
	fieldType := ts.layout.Schema().Type(fieldName)

	switch fieldType {
	case records.Integer:
		val, err := ts.GetInt(fieldName)
		return query.NewConstant(val), err
	case records.Long:
		val, err := ts.GetLong(fieldName)
		return query.NewConstant(val), err
	case records.Short:
		val, err := ts.GetShort(fieldName)
		return query.NewConstant(val), err
	case records.Varchar:
		val, err := ts.GetString(fieldName)
		return query.NewConstant(val), err
	case records.Boolean:
		val, err := ts.GetBool(fieldName)
		return query.NewConstant(val), err
	case records.Date:
		val, err := ts.GetDate(fieldName)
		return query.NewConstant(val), err
	default:
		return query.Constant{}, fmt.Errorf("unsupported field type: %v", fieldType)
	}
}

func (ts *TableScan) SetInt(fieldName string, val int) error {
	return ts.recordPage.SetInt(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetLong(fieldName string, val int64) error {
	return ts.recordPage.SetLong(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetShort(fieldName string, val int16) error {
	return ts.recordPage.SetShort(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetString(fieldName string, val string) error {
	return ts.recordPage.SetString(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetBool(fieldName string, val bool) error {
	return ts.recordPage.SetBool(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetDate(fieldName string, val time.Time) error {
	return ts.recordPage.SetDate(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetVal(fieldName string, val query.Constant) error {
	switch ts.layout.Schema().Type(fieldName) {
	case records.Integer:
		if v, ok := val.AsInt(); ok {
			return ts.SetInt(fieldName, v)
		}
	case records.Long:
		if v, ok := val.AsLong(); ok {
			return ts.SetLong(fieldName, v)
		}
	case records.Short:
		if v, ok := val.AsShort(); ok {
			return ts.SetShort(fieldName, v)
		}
	case records.Varchar:
		if v, ok := val.AsString(); ok {
			return ts.SetString(fieldName, v)
		}
	case records.Boolean:
		if v, ok := val.AsBool(); ok {
			return ts.SetBool(fieldName, v)
		}
	case records.Date:
		if v, ok := val.AsDate(); ok {
			return ts.SetDate(fieldName, v)
		}
	}
	return fmt.Errorf("type mismatch for field %s", fieldName)
}

func (ts *TableScan) HasField(fieldName string) bool {
	return ts.layout.Schema().HasField(fieldName)
}

func (ts *TableScan) Close() error {
	if ts.recordPage != nil {
		ts.tx.Unpin(ts.recordPage.Block())
	}
	return nil
}

// Insert adds a new record to the table. It tries to insert after the current slot, and if that fails (e.g., page is full), it checks if it's at the last block. If it is, it moves to a new block; otherwise, it moves to the next block and tries again.
/*
Try InsertAfter(currentSlot) on current page
    ├── found a free slot → done
    └── no free slot
            ├── at last block → moveToNewBlock() (grow the file)
            └── not last block → moveToBlock(n+1)
                then InsertAfter(-1) on that block
*/
func (ts *TableScan) Insert() error {

	if ts.layout.SlotSize() > ts.tx.BlockSize() {
		return fmt.Errorf("record size %d exceeds block size %d", ts.layout.SlotSize(), ts.tx.BlockSize())
	}
	
	slot, err := ts.recordPage.InsertAfter(ts.currentSlot)

	// Key change: match Java's behavior for handling InsertAfter
	if err != nil {
		atLastBlock, err := ts.atLastBlock()
		if err != nil {
			return fmt.Errorf("checking last block: %w", err)
		}

		if atLastBlock {
			if err := ts.moveToNewBlock(); err != nil {
				return fmt.Errorf("move to new block: %w", err)
			}
		} else {
			if err := ts.moveToBlock(ts.recordPage.Block().Number() + 1); err != nil {
				return fmt.Errorf("move to next block: %w", err)
			}
		}

		slot, err = ts.recordPage.InsertAfter(ts.currentSlot) // Start from beginning of new block
		if err != nil {
			return fmt.Errorf("insert in new block: %w", err)
		}
	}

	ts.currentSlot = slot
	return nil
}


func (ts *TableScan) Delete() error {
	return ts.recordPage.Delete(ts.currentSlot)
}

func (ts *TableScan) GetRecordID() *records.ID {
	return records.NewID(ts.recordPage.Block().Number(), ts.currentSlot)
}

func (ts *TableScan) MoveToRecordID(rid *records.ID) error {
	if err := ts.Close(); err != nil {
		return fmt.Errorf("close current page: %w", err)
	}

	blk := &file.BlockID{
		File:        ts.fileName,
		BlockNum: rid.BlockNumber(),
	}

	page, err := records.NewPage(ts.tx, blk, ts.layout)
	if err != nil {
		return fmt.Errorf("create new page: %w", err)
	}

	ts.recordPage = page
	ts.currentSlot = rid.Slot()
	return nil
}



// moveToBlock moves the scan to the specified block number.
func (ts *TableScan) moveToBlock(blockNum int) error {
	if err := ts.Close(); err != nil {
		return fmt.Errorf("close current page: %w", err)
	}

	blk := &file.BlockID{
		File:        ts.fileName,
		BlockNum: blockNum,
	}

	page, err := records.NewPage(ts.tx, blk, ts.layout)
	if err != nil {
		return fmt.Errorf("create new page: %w", err)
	}

	ts.recordPage = page
	ts.currentSlot = -1
	return nil
}



// moveToNewBlock moves the scan to a new block.
func (ts *TableScan) moveToNewBlock() error {
	if err := ts.Close(); err != nil {
		return fmt.Errorf("close current page: %w", err)
	}

	blk, err := ts.tx.Append(ts.fileName)
	if err != nil {
		return fmt.Errorf("append block: %w", err)
	}

	page, err := records.NewPage(ts.tx, blk, ts.layout)
	if err != nil {
		return fmt.Errorf("create new page: %w", err)
	}

	if err := page.Format(); err != nil {
		return fmt.Errorf("format page: %w", err)
	}

	ts.recordPage = page
	ts.currentSlot = -1
	return nil
}

// atLastBlock returns true if the scan is at the last block.
func (ts *TableScan) atLastBlock() (bool, error) {
	fileSize, err := ts.tx.Size(ts.fileName)
	if err != nil {
		return false, fmt.Errorf("get file size: %w", err)
	}
	return ts.recordPage.Block().Number() == fileSize-1, nil
}



