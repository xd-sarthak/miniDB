package index

import (
	"github.com/xd-sarthak/miniDB/query"
	"github.com/xd-sarthak/miniDB/records"
)

// generic index interface
type Index interface {
	// BeforeFirst positions the index before the first record with the given search key
	BeforeFirst(searchkey query.Constant) error
	//Next moves the index to the next record and returns true if there is a next record, false otherwise
	Next() (bool, error)
	// GetDataRecordID returns the data record ID of the current index record
	GetDataRecordID() (*records.ID, error)
	// Insert inserts a new index record with the given data value and data record ID
	Insert(datavalue query.Constant, dataRecordID *records.ID) error
	// Delete deletes the index record with the given data value and data record ID
	Delete(datavalue query.Constant, dataRecordID *records.ID) error
	// Close closes the index and releases any resources held by it
	Close()
}

/*

index search happens in two steps:

1. The search key is used to find the block number and record ID of the data record in the index file.
2. The block number and record ID are then used to retrieve the actual data record from the data file.

*/