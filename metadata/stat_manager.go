package metadata

import (
	"sync"

	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/tablescan"
	"github.com/xd-sarthak/miniDB/transaction"
)

/*

we need metdata about the tables to estimate the cost of queries and to optimize them. -> TableManager

StatMgr
   |
   | asks
   v
TableManager
   |
   v
Table Layout

tableStats is a cache
without it:
	Every stats request
		↓
	Full table scan
		↓
	Very slow

numCalls counts how many times we've requested stats for a table. If it exceeds a certain threshold, we refresh the stats by performing a full scan of the table.

we refresh stats after a certain number of calls to ensure that our statistics remain accurate and up-to-date, especially if the underlying data has changed significantly.

*/

// StatManager is responsible for managing statistics about tables, such as the number of blocks, records, and distinct values for fields.
type StatManager struct {
	tableManager  *TableManager
	tableStats    map[string]*StatInfo
	numCalls      int
	mu            sync.Mutex // protection for concurrent access to tableStats and numCalls
	refreshInterval int
}

// NewStatMgr creates a new StatManager instance, initializing statistics by scanning the entire database.
func NewStatMgr(tableManager *TableManager, transaction *transaction.Transaction, refreshLimit int) (*StatManager, error) {
	statMgr := &StatManager{
		tableManager: tableManager,
		tableStats:   make(map[string]*StatInfo),
		refreshInterval: refreshLimit,
	}
	if err := statMgr.RefreshStatistics(transaction); err != nil {
		return nil, err
	}
	return statMgr, nil
}

// GetStatInfo retrieves the statistics for a given table. 
// If the statistics are not already cached, it calculates them by performing a full scan of the table.
func (s *StatManager) GetStatInfo(table string, layout *records.Layout, transaction *transaction.Transaction) (*StatInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.numCalls++
	if s.numCalls >= s.refreshInterval {
		if err := s.RefreshStatistics(transaction); err != nil {
			return nil, err
		}
		s.numCalls = 0
	}

	if statInfo, exists := s.tableStats[table]; exists {
		return statInfo, nil
	}

	//calculate stats if not already present
	statInfo, err := s.calculateStats(table, layout, transaction)
	if err != nil {
		return nil, err
	}
	s.tableStats[table] = statInfo
	return statInfo, nil
}

// RefreshStatistics publically forces a refresh of all table statistics
//  This method is useful when we know that the underlying data has changed significantly and we want to ensure that our statistics are up-to-date for accurate query optimization.
func (s *StatManager) RefreshStatistics(transaction *transaction.Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s._refreshStatistics(transaction)
}

// _refreshStatistics is the internal method that refreshes the statistics for all tables in the catalog.
func (s *StatManager) _refreshStatistics(transaction *transaction.Transaction) error {
	// assuming the caller has already acquired the lock

	s.tableStats = make(map[string]*StatInfo) // clear existing stats
	s.numCalls = 0 // reset call count

	tableCatalogLayout, err := s.tableManager.GetLayout(tableCatalogTableName,transaction)
	if err != nil {
		return err
	}

	tableCatalogTableScan, err := tablescan.NewTableScan(transaction,tableCatalogTableName,tableCatalogLayout)
	if err != nil {
		return err
	}
	defer tableCatalogTableScan.Close()

	for {
		hasNext, err := tableCatalogTableScan.Next()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		tblName, err := tableCatalogTableScan.GetString(tableNameField)
		if err != nil {
			return err
		}

		layout, err := s.tableManager.GetLayout(tblName, transaction)
		if err != nil {
			return err
		}

		statInfo, err := s.calculateStats(tblName, layout, transaction)
		if err != nil {
			return err
		}
		s.tableStats[tblName] = statInfo
	}

	return nil
}

/*

Scan the entire table once
    ↓
Count records
    ↓
Count blocks used
    ↓
Count unique values per field
    ↓
Create StatInfo


 */
// calculateTableStats scans the entire table once to collect statistics about the table's records and fields.
func (s *StatManager) calculateStats(tableName string, layout *records.Layout, transaction *transaction.Transaction) (*StatInfo, error) {
	numRecords := 0
	numblocks := 0
	distinctValues := make(map[string]map[any]interface{}) // fieldname -> distinct value counts
	/*
	{
    "id":   {},
    "name": {},
	 */

	for _, field := range layout.Schema().Fields() {
		distinctValues[field] = make(map[any]interface{})
	}

	ts, err := tablescan.NewTableScan(transaction, tableName, layout)
	if err != nil {
		return nil, err
	}
	defer ts.Close()

	for {
		hasNext, err := ts.Next()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}
		numRecords++
		rid := ts.GetRecordID()
		if rid.BlockNumber() >= numblocks {
			numblocks = rid.BlockNumber() + 1
		}
		for _, field := range layout.Schema().Fields() {
			value, err := ts.GetVal(field)
			if err != nil {
				return nil, err
			}
			distinctValues[field][value] = struct{}{}
		}
	}

	distinctCounts := make(map[string]int)
	for field, values := range distinctValues {
		distinctCounts[field] = len(values)
	}

	return &StatInfo{
		numBlocks:       numblocks,
		numRecords:      numRecords,
		distinctValues:  distinctCounts,
	}, nil
}