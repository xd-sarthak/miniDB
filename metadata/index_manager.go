package metadata

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/tablescan"
	"github.com/xd-sarthak/miniDB/transaction"
)

const (
	indexCatalogTable = "index_catalog"
	indexNameField    = "index_name"
)

// IndexManager is responsible for managing index metadata, including creating and retrieving index information.
type IndexManager struct {
	layout 		 *records.Layout
	tableManager *TableManager
	statManager  *StatManager
}

// NewIndexManager initializes the IndexManager, creating the index catalog table if it is a new database.
func NewIndexManager(isNew bool, tableManager *TableManager, statManager *StatManager, transaction *transaction.Transaction) (*IndexManager, error) {
	if isNew {
		schema := records.NewSchema()
		schema.AddStringField(indexNameField, maxNameLength)
		schema.AddStringField(tableNameField, maxNameLength)
		schema.AddStringField(fieldNameField, maxNameLength)
	

	if err := tableManager.CreateTable(indexCatalogTable, schema, transaction); err != nil {
		return nil, fmt.Errorf("failed to create index catalog table: %w", err)
	}
}

	layout, err := tableManager.GetLayout(indexCatalogTable, transaction)
	if err != nil {
		return nil, fmt.Errorf("failed to get layout for index catalog table: %w", err)
	}

	return &IndexManager{
		layout:       layout,
		tableManager:  tableManager,
		statManager:   statManager,
	}, nil
}

// CreateIndex creates a new index for the specified table and field, storing its metadata in the index catalog.
// A unique ID is assigned to this index, and its information is stored in the indexCatalogTable.
func (m *IndexManager) CreateIndex(indexName, tableName, fieldName string, transaction *transaction.Transaction) error {
	ts, err := tablescan.NewTableScan(transaction, indexCatalogTable, m.layout)
	if err != nil {
		return fmt.Errorf("failed to create table scan for index catalog: %w", err)
	}
	defer ts.Close()

	if err := ts.Insert(); err != nil {
		return fmt.Errorf("failed to insert new index record: %w", err)
	}

	if err := ts.SetString(indexNameField, indexName); err != nil {
		return fmt.Errorf("failed to set index name: %w", err)
	}
	if err := ts.SetString(tableNameField, tableName); err != nil {
		return fmt.Errorf("failed to set table name: %w", err)
	}
	if err := ts.SetString(fieldNameField, fieldName); err != nil {
		return fmt.Errorf("failed to set field name: %w", err)
	}
	
	return nil

}

func (m *IndexManager) GetIndexInfo(tableName string, transaction *transaction.Transaction) (map[string]*IndexInfo, error) {
	tableScan, err := tablescan.NewTableScan(transaction, indexCatalogTable, m.layout)
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan for index catalog: %w", err)
	}
	defer tableScan.Close()

	result := make(map[string]*IndexInfo)

	for {
		hasNext, err := tableScan.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to advance table scan: %w", err)
		}
		if !hasNext {
			break
		}

		currentTableName, err := tableScan.GetString(tableNameField)
		if err != nil {
			return nil, fmt.Errorf("failed to get table name from index catalog: %w", err)
		}
		if currentTableName != tableName {
			continue
		}

		var indexName, fieldName string

		indexName, err = tableScan.GetString(indexNameField)
		if err != nil {
			return nil, fmt.Errorf("failed to get index name from index catalog: %w", err)
		}
		fieldName, err = tableScan.GetString(fieldNameField)
		if err != nil {
			return nil, fmt.Errorf("failed to get field name from index catalog: %w", err)
		}

		tableLayout, err := m.tableManager.GetLayout(tableName, transaction)
		if err != nil {
			return nil, fmt.Errorf("failed to get table layout for index info: %w", err)
		}

		tableStatsInfo,err := m.statManager.GetStatInfo(tableName,tableLayout,transaction)
		if err != nil {
			return nil, fmt.Errorf("failed to get table stats info for index info: %w", err)
		}
		
		indexInfo := NewIndexInfo(indexName, fieldName, tableLayout.Schema(), transaction, tableStatsInfo)
		result[indexName] = indexInfo
	}

	return result, nil
}

/*
let
CREATE INDEX student_id_idx ON student(id);
CREATE INDEX student_name_idx ON student(name);

so we need a catalog table storing

index_name       table_name      field_name
------------------------------------------------
student_id_idx   student         id
student_name_idx student         name

*/