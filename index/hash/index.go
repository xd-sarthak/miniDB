package hash

import (
	"fmt"

	"github.com/xd-sarthak/miniDB/index"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/tablescan"
	"github.com/xd-sarthak/miniDB/transaction"
	"github.com/xd-sarthak/miniDB/utils"
)

const numBuckets = 100

type Index struct {
	index.Index
	transaction *transaction.Transaction
	indexName   string
	layout      *records.Layout
	searchKey   any
	tableScan   *tablescan.TableScan
}

// NewIndex opens a hash index for the specified index.
func NewIndex(transaction *transaction.Transaction, indexName string, layout *records.Layout) *Index {
	return &Index{
		transaction: transaction,
		indexName:   indexName,
		layout:      layout,
		searchKey:   nil,
		tableScan:   nil,
	}
}
// BeforeFirst positions the index before the first record with the given search key
// basically prepare the index for searching a specific key
/*

BeforeFirst(101)

    ↓
Close old bucket

    ↓
Hash 101 --> 83421 % 100 = 21

    ↓
Find bucket 21

    ↓
Open bucket file student_idx-21

    ↓
Store searchKey=101

    ↓
Ready for Next()

*/
func (idx *Index) BeforeFirst(searchkey any) error {
	idx.Close() //close old buckets if any

	idx.searchKey = searchkey                    //remembber the key
	hashValue, err := utils.HashValue(searchkey) // hash the key
	if err != nil {
		return fmt.Errorf("failed to compute hash value: %w", err)
	}
	bucket := hashValue % numBuckets // determine the bucket number

	tableName := fmt.Sprintf("%s_%d", idx.indexName, bucket) // construct the table name for the bucket
	idx.tableScan, err = tablescan.NewTableScan(idx.transaction, tableName, idx.layout)
	if err != nil {
		return fmt.Errorf("failed to create table scan: %w", err)
	}
	return nil

}

// Next moves to the next index record having the search key.
// The method loops through the table scan for the bucket, looking for a matching record,
// and returns false if there are no more such records.
func (idx *Index) Next() (bool, error) {
	for {
		hasNext, err := idx.tableScan.Next()
		if err != nil || !hasNext {
			return false, err
		}

		currentValue, err := idx.tableScan.GetVal(index.DataValueField)
		if err != nil {
			return false, err
		}
		// GetVal returns the raw value; searchKey is the raw value stored by
		// BeforeFirst. Compare directly.
		if currentValue == idx.searchKey {
			return true, nil
		}
	}
}

// GetDataRecordID retrieves the data record ID from the current record in the table scan for the bucket.
func (idx *Index) GetDataRecordID() (*records.ID, error) {
	blockNumber, err := idx.tableScan.GetInt(index.Blockfield)
	if err != nil {
		return nil, err
	}
	id, err := idx.tableScan.GetInt(index.IDField)
	if err != nil {
		return nil, err
	}

	return records.NewID(blockNumber, id), nil
}

// Insert inserts a new record into the table scan for the bucket.
func (idx *Index) Insert(dataValue any, dataRecordID *records.ID) error {
	if err := idx.BeforeFirst(dataValue); err != nil {
		return err
	}

	if err := idx.tableScan.Insert(); err != nil {
		return err
	}
	if err := idx.tableScan.SetInt(index.Blockfield, dataRecordID.BlockNumber()); err != nil {
		return err
	}
	if err := idx.tableScan.SetInt(index.IDField, dataRecordID.Slot()); err != nil {
		return err
	}
	return idx.tableScan.SetVal(index.DataValueField, dataValue)
}

// Delete deletes the specified record from the table scan for the bucket.
// The method starts at the beginning of the scan, and loops through the
// records until the specified record is found. If the record is found, it is deleted.
// If the record is not found, the method does nothing and does not return an error.
func (idx *Index) Delete(dataValue any, dataRecordID *records.ID) error {
	if err := idx.BeforeFirst(dataValue); err != nil {
		return err
	}

	for {
		hasNext, err := idx.tableScan.Next()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		currentRecordID, err := idx.GetDataRecordID()
		if err != nil {
			return err
		}

		if currentRecordID.Equals(dataRecordID) {
			return idx.tableScan.Delete()
		}
	}

	return nil
}

// Close closes the index by closing the current table scan.
func (idx *Index) Close() {
	if idx.tableScan != nil {
		idx.tableScan.Close()
		idx.tableScan = nil
	}
}

// SearchCost returns the cost of searching an index file having
// the specified number of blocks.
// the method assumes that all buckets are about the same size,
// so the cost is simply the size of the bucket.
func SearchCost(numBlocks, recordsPerBucket int) int {
	return numBlocks / numBuckets
}

/*

let this be a table,

Student Table

RID      ID      Name
------------------------
(0,0)    101     Bob
(0,1)    102     Alice
(1,0)    103     John
(1,1)    101     Mike

1. step 1: NewIndex()

indexName = student_id_idx

searchKey = nil

tableScan = nil

2. step 2: Insert()

suppose we insert 101 -> RID(0,0)

3. step 3: BeforeFirst(101)

seachKey = 101
hashvalue 100 -> hash-> 843721
bucket = 843721 % 100 = 21
tablename = student_id_idx_21

open table scan on student_id_idx_21
now the index is looking at bucket 21

it inserts
block      = 0
id         = 0
data_value = 101


so similarly

student_idx-21

101 -> (0,0)
101 -> (1,1)

------------------

student_idx-55

102 -> (0,1)

------------------

student_idx-77

103 -> (1,0)


NOw if we search WHERE id = 101

we hash 101 -> 843721 % 100 = 21

and open student_idx-21

and we find two records with data_value = 101 hence the Next() will return true twice

and we get their RIDs (0,0) and (1,1)


INSERT

Record
  ↓
RID
  ↓
Hash(Key)
  ↓
Bucket
  ↓
Store (Key,RID)

--------------------------------

SEARCH

Key
 ↓
Hash(Key)
 ↓
Bucket
 ↓
Next()
 ↓
GetDataRecordID()
 ↓
RID
 ↓
Actual Record

--------------------------------

DELETE

Key + RID
 ↓
Hash(Key)
 ↓
Bucket
 ↓
Find matching entry
 ↓
Delete it



*/
