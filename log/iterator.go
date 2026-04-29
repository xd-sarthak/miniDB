package log

import (
	"errors"
	"fmt"
	"github.com/xd-sarthak/miniDB/file"
)

// reverse iterator to read log files newest to oldest
type Iterator struct {
	fileManager *file.Manager
	block       *file.BlockID
	page        *file.Page
	currentPos  int
	boundary    int
}

// creates iterator for the records in the log file positioned after the last log
func NewIterator(fileManager *file.Manager, block *file.BlockID) (*Iterator, error) {
	page := file.NewPage(fileManager.BlockSize())
	iterator := &Iterator{
		fileManager: fileManager,
		block: block,
		page: page,
	}

	if err := iterator.moveToBlock(block); err != nil {
		return nil,fmt.Errorf("failed to move to block: %v", err)
	}
	return iterator,nil
}

// moveToBlock moves to the specified log block and positions it at the first record in that block (i.e., the most recent one).
func (it *Iterator) moveToBlock(block *file.BlockID) error {
	if err := it.fileManager.Read(block, it.page); err != nil {
		return fmt.Errorf("failed to read block: %v", err)
	}

	it.boundary = int(it.page.GetInt(0))
	it.currentPos = it.boundary
	return nil
}

// HasNext determines if the current log record is the earliest record in the log file. Returns true if there is an earlier record.
func (it *Iterator) HasNext() bool {
	return it.currentPos < it.fileManager.BlockSize() || it.block.Number() > 0
}

// Next moves to the next log record in the block.
// If there are no more log records in the block, then move to the previous block and return the log record from there.
// Returns the next earliest log record.
func (it *Iterator) Next() ([]byte, error) {
	// Check if there are no more records left in the current block.
	if it.currentPos == it.fileManager.BlockSize() {
		// Check if this is the first block.
		if it.block.Number() == 0 {
			return nil, errors.New("no more log records")
		}

		// Move to the previous block in the log file.
		it.block = &file.BlockID{File: it.block.Filename(), BlockNum: it.block.Number() - 1}
		if err := it.moveToBlock(it.block); err != nil {
			return nil, fmt.Errorf("failed to move to block: %v", err)
		}
	}

	record := it.page.GetBytes(it.currentPos)
	it.currentPos += 4 + len(record) // (size of record) + (length of record)
	return record, nil
}


