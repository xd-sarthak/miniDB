package plan_impl

import (
	"github.com/xd-sarthak/miniDB/metadata"
	"github.com/xd-sarthak/miniDB/plan"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/scan"
	"github.com/xd-sarthak/miniDB/tablescan"
	"github.com/xd-sarthak/miniDB/transaction"
)

var _ plan.Plan = &TablePlan{}

type TablePlan struct {
	tableName   string
	transaction *transaction.Transaction
	layout      *records.Layout
	statInfo    *metadata.StatInfo
}

// NewTablePlan creates a leaf node in the query tree
// corresponding to the specified table.
func NewTablePlan(transaction *transaction.Transaction, tableName string, metadataManager *metadata.Manager) (*TablePlan, error) {
	tp := &TablePlan{
		tableName:   tableName,
		transaction: transaction,
	}

	var err error
	if tp.layout, err = metadataManager.GetLayout(tableName, transaction); err != nil {
		return nil, err
	}
	if tp.statInfo, err = metadataManager.GetStatInfo(tableName, tp.layout, transaction); err != nil {
		return nil, err
	}
	return tp, nil
}

// Open creates a table scan for this query
func (tp *TablePlan) Open() (scan.Scan, error) {
	return tablescan.NewTableScan(tp.transaction, tp.tableName, tp.layout)
}

// BlocksAccessed estimates the number of block accesses for the table,
// which is obtainable from the statistics manager.
func (tp *TablePlan) BlocksAccessed() int {
	return tp.statInfo.BlocksAccessed()
}

// RecordsOutput estimates the number of records in the table,
// which is obtainable from the statistics manager.
func (tp *TablePlan) RecordsOutput() int {
	return tp.statInfo.RecordsOutput()
}

// DistinctValues estimates the number of distinct values for the specified field
// in the table, which is obtainable from the stats manager.
func (tp *TablePlan) DistinctValues(fieldName string) int {
	return tp.statInfo.DistinctValues(fieldName)
}

// Schema determines the schema of the table,
// which is obtainable from the catalog manager
func (tp *TablePlan) Schema() *records.Schema {
	return tp.layout.Schema()
}
