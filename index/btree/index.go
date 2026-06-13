package btree

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/file"
	"github.com/xd-sarthak/miniDB/index"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/transaction"
	"math"
	"time"
)

var _ index.Index = (*Index)(nil)

const (
	leafSuffix      = "_leaf"
	directorySuffix = "_directory"
)

type Index struct {
	transaction     *transaction.Transaction
	directoryLayout *records.Layout
	leafLayout      *records.Layout
	leafTable       string
	leaf            *Leaf
	rootBlock       *file.BlockID
}

// NewIndex opens a b-tree index for the specified index.
// The method determines the appropriate files for the leaf
// and directory records, creating them if they do not exist.
func NewIndex(transaction *transaction.Transaction, indexName string, leafLayout *records.Layout) (index.Index, error) {
	idx := &Index{
		transaction: transaction,
		leafTable:   indexName + leafSuffix,
		leafLayout:  leafLayout,
		leaf:        nil,
	}

	leafTableSize, err := idx.transaction.Size(idx.leafTable)
	if err != nil {
		return nil, err
	}

	// Deal with the leaves
	if leafTableSize == 0 {
		block, err := idx.transaction.Append(idx.leafTable)
		if err != nil {
			return nil, err
		}
		node, err := NewPage(idx.transaction, block, leafLayout)
		if err != nil {
			return nil, err
		}
		if err := node.format(block, -1); err != nil {
			return nil, err
		}
	}

	// Deal with the directory
	directorySchema := records.NewSchema()
	directorySchema.Add(index.Blockfield, leafLayout.Schema())
	directorySchema.Add(index.DataValueField, leafLayout.Schema())

	directoryTable := indexName + directorySuffix
	idx.directoryLayout = records.NewLayout(directorySchema)

	idx.rootBlock = file.NewBlockID(directoryTable, 0)

	directoryTableSize, err := idx.transaction.Size(directoryTable)
	if err != nil {
		return nil, err
	}
	if directoryTableSize == 0 {
		// Create a new root block
		_, err := idx.transaction.Append(directoryTable)
		if err != nil {
			return nil, err
		}
		node, err := NewPage(idx.transaction, idx.rootBlock, idx.directoryLayout)
		if err != nil {
			return nil, err
		}
		if err := node.format(idx.rootBlock, 0); err != nil {
			return nil, err
		}

		// insert initial directory entry
		fieldType := directorySchema.Type(index.DataValueField)
		switch fieldType {
		case records.Integer:
			if err = node.InsertDirectory(0, 0, 0); err != nil {
				return nil, err
			}
		case records.Varchar:
			if err = node.InsertDirectory(0, "", 0); err != nil {
				return nil, err
			}
		case records.Boolean:
			if err = node.InsertDirectory(0, false, 0); err != nil {
				return nil, err
			}
		case records.Long:
			if err = node.InsertDirectory(0, int64(0), 0); err != nil {
				return nil, err
			}
		case records.Short:
			if err = node.InsertDirectory(0, int16(0), 0); err != nil {
				return nil, err
			}
		case records.Date:
			if err = node.InsertDirectory(0, time.Time{}, 0); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported type: %T", fieldType)
		}
		node.Close()
	}
	return idx, nil
}

// BeforeFirst traverses the directory to find the leaf block
// corresponding to the specified search key.
// The method then opens a page for that leaf block, and
// positions the page before the first record (if any)
// having that search key.
// The leaf page is left open for use by the methods
// Next and GetDataRecordID.
func (idx *Index) BeforeFirst(searchKey interface{}) error {
	idx.Close()
	root, err := NewDirectory(idx.transaction, idx.rootBlock, idx.directoryLayout)
	if err != nil {
		return err
	}
	blockNumber, err := root.Search(searchKey)
	if err != nil {
		return err
	}
	root.Close()

	leafBlock := file.NewBlockID(idx.leafTable, blockNumber)
	idx.leaf, err = NewLeaf(idx.transaction, leafBlock, idx.leafLayout, searchKey)
	return err
}

// Next moves to the next record having the previously specified search key.
// Returns false if there are no more such records.
func (idx *Index) Next() (bool, error) {
	return idx.leaf.Next()
}

// GetDataRecordID returns the record ID of the current leaf records.
func (idx *Index) GetDataRecordID() (*records.ID, error) {
	return idx.leaf.GetDataRID()
}

// Insert inserts the specified record in the index.
// The method first traverses the directory to find the
// appropriate leaf page; then it inserts the record
// into the leaf.
// If the insertion causes the leaf to split, the method
// calls insert on the root, passing it the directory
// entry of the new leaf page.
// If the root node splits, then makeNewRoot is called.
func (idx *Index) Insert(dataVal any, dataRID *records.ID) error {
	if err := idx.BeforeFirst(dataVal); err != nil {
		return err
	}
	// Insert the record into the leaf
	directoryEntry, err := idx.leaf.Insert(dataRID)
	idx.leaf.Close()

	if err != nil {
		return err
	}
	// If the leaf did not split, we are done
	if directoryEntry == nil {
		return nil
	}

	// Leaf split, insert the new directory entry.
	root, err := NewDirectory(idx.transaction, idx.rootBlock, idx.directoryLayout)
	if err != nil {
		return err
	}

	newDirectoryEntry, err := root.Insert(directoryEntry)
	if err != nil {
		return err
	}

	// If the root did not split, we are done.
	// Else, create a new root.
	if newDirectoryEntry != nil {
		return root.MakeNewRoot(newDirectoryEntry)
	}
	root.Close()
	return nil
}

// Delete deletes the specified index records.
// The method first traverses the directory to find the
// leaf page containing the record, then it deletes the
// record from the page.
func (idx *Index) Delete(dataVal any, dataRID *records.ID) error {
	if err := idx.BeforeFirst(dataVal); err != nil {
		return err
	}

	if err := idx.leaf.Delete(dataRID); err != nil {
		return err
	}

	idx.leaf.Close()
	return nil
}

// Close closes the index by closing the current leaf page, if necessary.
func (idx *Index) Close() {
	if idx.leaf != nil {
		idx.leaf.Close()
	}
}

// SearchCost returns the estimated number of block accesses
// required to find all the index records having a particular
// search key.
func (idx *Index) SearchCost(numBlocks, recordsPerBlock int) int {
	return 1 + int(math.Log(float64(numBlocks))/math.Log(float64(recordsPerBlock)))
}
