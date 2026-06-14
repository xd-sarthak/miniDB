package btree

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/file"
	"github.com/xd-sarthak/miniDB/index"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/transaction"
	"github.com/xd-sarthak/miniDB/utils"
	"time"
)

type Page struct {
	tx         *transaction.Transaction
	currentBlk *file.BlockID
	layout     *records.Layout
}

func NewPage(tx *transaction.Transaction, currentBlk *file.BlockID, layout *records.Layout) (*Page, error) {
	if err := tx.Pin(currentBlk); err != nil {
		return nil, err
	}
	return &Page{
		tx:         tx,
		currentBlk: currentBlk,
		layout:     layout,
	}, nil
}

// FindSlotBefore calculates the position where the first record having
// the specified search key should be, then returns the position
// just before it.
func (p *Page) FindSlotBefore(searchKey any) (int, error) {
	numberOfRecords, err := p.GetNumberOfRecords()
	if err != nil {
		return -1, err
	}

	for slot := 0; slot < numberOfRecords; slot++ {
		dataVal, err := p.GetDataVal(slot)
		if err != nil {
			return -1, err
		}
		if utils.CompareSupportedTypes(dataVal, searchKey, utils.GE) {
			return slot - 1, nil
		}
	}

	// If no match is found, return the last slot
	return numberOfRecords - 1, nil
}

// Close closes the page by unpinning its buffer.
func (p *Page) Close() {
	if p.currentBlk != nil {
		p.tx.Unpin(p.currentBlk)
		p.currentBlk = nil
	}
}

// IsFull returns true if the block is full.
func (p *Page) IsFull() (bool, error) {
	numberOfRecords, err := p.GetNumberOfRecords()
	if err != nil {
		return false, err
	}
	return p.slotPosition(numberOfRecords+1) >= p.tx.BlockSize(), nil
}

// Split splits the page at the specified position.
// A new page is created, and the records of the page
// starting at the split position are transferred to the new page.
func (p *Page) Split(splitPos, flag int) (*file.BlockID, error) {
	newBlk, err := p.AppendNew(flag)
	if err != nil {
		return nil, err
	}
	newPage, err := NewPage(p.tx, newBlk, p.layout)
	if err != nil {
		return nil, err
	}
	if err := p.transferRecords(splitPos, newPage); err != nil {
		return nil, err
	}
	if err := newPage.SetFlag(flag); err != nil {
		return nil, err
	}
	newPage.Close()
	return newBlk, nil
}

// GetDataVal returns the data value of the record at the specified slot.
func (p *Page) GetDataVal(slot int) (any, error) {
	return p.getVal(slot, index.DataValueField)
}

// GetFlag returns the page's flag field.
func (p *Page) GetFlag() (int, error) {
	flag, err := p.tx.GetInt(p.currentBlk, 0)
	if err != nil {
		return -1, err
	}
	return flag, nil
}

// SetFlag sets the page's flag field to the specified value.
func (p *Page) SetFlag(val int) error {
	return p.tx.SetInt(p.currentBlk, 0, val, true)
}

// AppendNew appends a new block to the end of the specified b-tree file,
// having the specified flag value.
func (p *Page) AppendNew(flag int) (*file.BlockID, error) {
	blk, err := p.tx.Append(p.currentBlk.Filename())
	if err != nil {
		return nil, err
	}
	if err := p.tx.Pin(blk); err != nil {
		return nil, err
	}
	if err := p.format(blk, flag); err != nil {
		return nil, err
	}
	return blk, nil
}

func (p *Page) format(blk *file.BlockID, flag int) error {
	if err := p.tx.SetInt(blk, 0, flag, false); err != nil {
		return err
	}
	if err := p.tx.SetInt(blk, utils.IntSize, 0, false); err != nil {
		return err
	}
	recSize := p.layout.SlotSize()
	for pos := 2 * utils.IntSize; pos+recSize <= p.tx.BlockSize(); pos += recSize {
		if err := p.makeDefaultRecord(blk, pos); err != nil {
			return err
		}
	}
	return nil
}

func (p *Page) makeDefaultRecord(blk *file.BlockID, pos int) error {
	schema := p.layout.Schema()
	for _, field := range schema.Fields() {
		offset := p.layout.Offset(field)
		switch schema.Type(field) {
		case records.Integer:
			if err := p.tx.SetInt(blk, pos+offset, 0, false); err != nil {
				return err
			}
		case records.Varchar:
			if err := p.tx.SetString(blk, pos+offset, "", false); err != nil {
				return err
			}
		case records.Boolean:
			if err := p.tx.SetBool(blk, pos+offset, false, false); err != nil {
				return err
			}
		case records.Date:
			if err := p.tx.SetDate(blk, pos+offset, time.Time{}, false); err != nil {
				return err
			}
		case records.Long:
			if err := p.tx.SetLong(blk, pos+offset, 0, false); err != nil {
				return err
			}
		case records.Short:
			if err := p.tx.SetShort(blk, pos+offset, 0, false); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported type: %T", schema.Type(field))
		}
	}
	return nil
}

// GetChildNumber returns the block number stored in the index record at the specified slot.
func (p *Page) GetChildNumber(slot int) (int, error) {
	return p.getInt(slot, index.Blockfield)
}

func (p *Page) getInt(slot int, fieldName string) (int, error) {
	position := p.fieldPosition(slot, fieldName)
	return p.tx.GetInt(p.currentBlk, position)
}

func (p *Page) setInt(slot int, fieldName string, value int) error {
	position := p.fieldPosition(slot, fieldName)
	return p.tx.SetInt(p.currentBlk, position, value, true)
}

// InsertDirectory inserts a directory entry at the specified slot.
func (p *Page) InsertDirectory(slot int, value any, blockNumber int) error {
	if err := p.insert(slot); err != nil {
		return err
	}
	if err := p.setVal(slot, index.DataValueField, value); err != nil {
		return err
	}
	return p.setInt(slot, index.Blockfield, blockNumber)
}

// GetDataRecordID returns the record ID stored in the specified leaf index records.
func (p *Page) getDataRID(slot int) (*records.ID, error) {
	var err error
	var blockNumber, id int
	if blockNumber, err = p.getInt(slot, index.Blockfield); err != nil {
		return nil, err
	}
	if id, err = p.getInt(slot, index.IDField); err != nil {
		return nil, err
	}
	return records.NewID(blockNumber, id), nil
}

// InsertLeaf inserts a leaf entry at the specified slot.
func (p *Page) InsertLeaf(slot int, value any, rid *records.ID) error {
	if err := p.insert(slot); err != nil {
		return err
	}
	if err := p.setVal(slot, index.DataValueField, value); err != nil {
		return err
	}
	if err := p.setInt(slot, index.Blockfield, rid.BlockNumber()); err != nil {
		return err
	}
	return p.setInt(slot, index.IDField, rid.Slot())
}

// GetNumberOfRecords returns the number of index records in this page.
func (p *Page) GetNumberOfRecords() (int, error) {
	numRecs, err := p.tx.GetInt(p.currentBlk, utils.IntSize)
	if err != nil {
		return -1, err
	}
	return numRecs, nil
}

func (p *Page) transferRecords(slot int, destination *Page) error {
	destSlot := 0
	numberOfRecords, err := p.GetNumberOfRecords()
	if err != nil {
		return err
	}

	for slot < numberOfRecords {
		if err := destination.insert(destSlot); err != nil {
			return err
		}
		schema := p.layout.Schema()
		for _, field := range schema.Fields() {
			val, err := p.getVal(slot, field)
			if err != nil {
				return err
			}
			if err := destination.setVal(destSlot, field, val); err != nil {
				return err
			}
		}
		if err := p.delete(slot); err != nil {
			return err
		}
		destSlot++

		// Update number of records after deletion
		numberOfRecords, err = p.GetNumberOfRecords()
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Page) fieldPosition(slot int, fieldName string) int {
	return p.slotPosition(slot) + p.layout.Offset(fieldName)
}

// Helper methods for slot calculations
func (p *Page) slotPosition(slot int) int {
	slotSize := p.layout.SlotSize()
	return utils.IntSize*2 + slot*slotSize
}

func (p *Page) getVal(slot int, fieldName string) (any, error) {
	pos := p.fieldPosition(slot, fieldName)
	switch p.layout.Schema().Type(fieldName) {
	case records.Integer:
		return p.tx.GetInt(p.currentBlk, pos)
	case records.Varchar:
		return p.tx.GetString(p.currentBlk, pos)
	case records.Boolean:
		return p.tx.GetBool(p.currentBlk, pos)
	case records.Date:
		return p.tx.GetDate(p.currentBlk, pos)
	case records.Long:
		return p.tx.GetLong(p.currentBlk, pos)
	case records.Short:
		return p.tx.GetShort(p.currentBlk, pos)
	default:
		return nil, fmt.Errorf("unsupported type: %T", p.layout.Schema().Type(fieldName))
	}
}

func (p *Page) setVal(slot int, fieldName string, val any) error {
	pos := p.fieldPosition(slot, fieldName)
	switch p.layout.Schema().Type(fieldName) {
	case records.Integer:
		return p.tx.SetInt(p.currentBlk, pos, val.(int), true)
	case records.Varchar:
		return p.tx.SetString(p.currentBlk, pos, val.(string), true)
	case records.Boolean:
		return p.tx.SetBool(p.currentBlk, pos, val.(bool), true)
	case records.Date:
		return p.tx.SetDate(p.currentBlk, pos, val.(time.Time), true)
	case records.Long:
		return p.tx.SetLong(p.currentBlk, pos, val.(int64), true)
	case records.Short:
		return p.tx.SetShort(p.currentBlk, pos, val.(int16), true)
	default:
		return fmt.Errorf("unsupported type: %T", p.layout.Schema().Type(fieldName))
	}
}

func (p *Page) insert(slot int) error {
	numRecs, err := p.GetNumberOfRecords()
	if err != nil {
		return err
	}
	for i := numRecs; i > slot; i-- {
		if err := p.copyRecord(i-1, i); err != nil {
			return err
		}
	}
	if err := p.setNumberOfRecords(numRecs + 1); err != nil {
		return err
	}
	return nil
}

// Delete deletes the index record at the specified slot.
func (p *Page) delete(slot int) error {
	numRecs, err := p.GetNumberOfRecords()
	if err != nil {
		return err
	}
	for i := slot + 1; i < numRecs; i++ {
		if err := p.copyRecord(i, i-1); err != nil {
			return err
		}
	}
	if err := p.setNumberOfRecords(numRecs - 1); err != nil {
		return err
	}
	return nil
}

func (p *Page) setNumberOfRecords(n int) error {
	return p.tx.SetInt(p.currentBlk, utils.IntSize, n, true)
}

func (p *Page) copyRecord(from, to int) error {
	schema := p.layout.Schema()
	for _, field := range schema.Fields() {
		val, err := p.getVal(from, field)
		if err != nil {
			return err
		}
		if err := p.setVal(to, field, val); err != nil {
			return err
		}
	}
	return nil
}
