package materialize

import (
	"fmt"
	"sync"

	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/scan"
	"github.com/xd-sarthak/miniDB/tablescan"
	"github.com/xd-sarthak/miniDB/transaction"
)

const tempTablePrefix = "temp"

// TempTable represents a temporary table not registered in the catalog.
type TempTable struct {
	tx      *transaction.Transaction
	tblName string
	layout  *records.Layout
}

var (
	nextTableNum   = 0
	nextTableNumMu sync.Mutex
)

// NewTempTable creates a new temporary table with the specified schema and transaction.
func NewTempTable(tx *transaction.Transaction, schema *records.Schema) *TempTable {
	return &TempTable{
		tx:      tx,
		tblName: nextTableName(),
		layout:  records.NewLayout(schema),
	}
}

// Open opens a table scan for the temporary table.
func (tt *TempTable) Open() (scan.UpdateScan, error) {
	return tablescan.NewTableScan(tt.tx, tt.tblName, tt.layout)
}

// TableName returns the name of the temporary table.
func (tt *TempTable) TableName() string {
	return tt.tblName
}

// GetLayout returns the table's metadata (layout).
func (tt *TempTable) GetLayout() *records.Layout {
	return tt.layout
}

// nextTableName generates a unique name for the next temporary table.
func nextTableName() string {
	nextTableNumMu.Lock()
	defer nextTableNumMu.Unlock()
	nextTableNum++
	return fmt.Sprintf("%s%d", tempTablePrefix, nextTableNum)
}
