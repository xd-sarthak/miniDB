package metadata

import (
	"github.com/xd-sarthak/miniDB/index"
	"github.com/xd-sarthak/miniDB/index/hash"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/transaction"
)

type IndexInfo struct {
	indexName string
	fieldName string
	transaction *transaction.Transaction
	tableSchema *records.Schema
	indexLayout *records.Layout
	statInfo    *StatInfo
}


// NewIndexInfo creates an IndexInfo object for the specified index.
func NewIndexInfo(indexName, fieldName string, tableSchema *records.Schema,
	transaction *transaction.Transaction, statInfo *StatInfo) *IndexInfo {
	ii := &IndexInfo{
		indexName:   indexName,
		fieldName:   fieldName,
		transaction: transaction,
		tableSchema: tableSchema,
		statInfo:    statInfo,
	}
	ii.indexLayout = ii.CreateIndexLayout()
	return ii
}


// hashIndexAdapter wraps hash.Index to properly implement index.Index interface
type hashIndexAdapter struct {
	*hash.Index
}

func (a *hashIndexAdapter) BeforeFirst(searchkey any) error {
	return a.Index.BeforeFirst(searchkey)
}

func (a *hashIndexAdapter) Delete(dataValue any, dataRecordID *records.ID) error {
	return a.Index.Delete(dataValue, dataRecordID)
}

// Open opens the index described by this object.
func (ii *IndexInfo) Open() index.Index {
	idx := hash.NewIndex(ii.transaction, ii.indexName, ii.indexLayout)
	return &hashIndexAdapter{idx}
}

// BlocksAccessed estimates the number of block accesses required to
// find all the index records having a particular search key.
// The method uses the table's metadata to estimate the size of the
// index file and the number of index records per block.
// It then passes this information to the traversalCost method of the
// appropriate index type, which then provides the estimate.
func (ii *IndexInfo) BlocksAccessed() int {
	recordsPerBlock := ii.transaction.BlockSize() / ii.indexLayout.SlotSize()
	numBlocks := ii.statInfo.RecordsOutput() / recordsPerBlock
	return hash.SearchCost(numBlocks, recordsPerBlock)
	//return BtreeIndex.SearchCost(numBlocks, recordsPerBlock)
}

// RecordsOutput returns the estimated number of records having a search key.
// This value is the same as doing a select query; that is, it is the number of records in the table
// divided by the number of distinct values of the indexed field.
func (ii *IndexInfo) RecordsOutput() int {
	return ii.statInfo.RecordsOutput() / ii.statInfo.DistinctValues(ii.fieldName)
}

// DistinctValues returns the number of distinct values for the indexed field
// in the underlying table, or 1 for the indexed field.
func (ii *IndexInfo) DistinctValues(fieldName string) int {
	if ii.fieldName == fieldName {
		return 1
	}
	return ii.statInfo.DistinctValues(fieldName)
}

// CreateIndexLayout returns the layout of the index records.
// The schema consists of the dataRecordID (which is represented as two integers,
// the block number and the record ID) and the dataValue (which is the indexed field).
// Schema information about the indexed field is obtained from the table's schema.
func (ii *IndexInfo) CreateIndexLayout() *records.Layout {
	schema := records.NewSchema()
	schema.AddIntField(index.Blockfield)
	schema.AddIntField(index.IDField)
	switch ii.tableSchema.Type(ii.fieldName) {
	case records.Integer:
		schema.AddIntField(index.DataValueField)
	case records.Varchar:
		schema.AddStringField(index.DataValueField, ii.tableSchema.Length(ii.fieldName))
	case records.Boolean:
		schema.AddBoolField(index.DataValueField)
	case records.Long:
		schema.AddLongField(index.DataValueField)
	case records.Short:
		schema.AddShortField(index.DataValueField)
	case records.Date:
		schema.AddDateField(index.DataValueField)
	}

	return records.NewLayout(schema)
}



/*

IndexManager
    ↓
knows WHICH indexes exist

IndexInfo
    ↓
knows EVERYTHING ABOUT ONE INDEX -> statistical info









*/