package metadata

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/tablescan"
	"github.com/xd-sarthak/miniDB/transaction"
)

/*

Table metadata manager

CREATE TABLE
      ↓
Store schema metadata
      ↓
table_catalog ->  stores table name and size
field_catalog -> stores field name and type and length and offset
   
catalog tables -> store info about other tables
*/

const (
	maxNameLength = 16 // max length allowed for names
	tableNameField = "table_name"
	slotSizeField  = "slot_size"
	fieldNameField = "field_name"
	typeField      = "type"
	lengthField    = "length"
	offsetField    = "offset"
    
	tableCatalogTableName = "table_catalog"
	fieldCatalogTableName = "field_catalog"
)

// TableManager manages the metadata for tables and their fields.
// It has methods to create a table,
// save the metadata to the catalog tables,
// and retrieve table and field information.
type TableManager struct {
	tableCatalogLayout *records.Layout
	fieldCatalogLayout *records.Layout
}

// NewTableManager creates a new TableManager instance.
// It initializes the table catalog and field catalog layouts,
// and creates the catalog tables if they are new.
func NewTableManager(isNew bool, tx *transaction.Transaction) (*TableManager, error) {
	tm := &TableManager{}

	tableCatalogSchema := records.NewSchema()
	tableCatalogSchema.AddStringField(tableNameField, maxNameLength)
	tableCatalogSchema.AddIntField(slotSizeField)

	// computes offsets and slot size for the table catalog layout
	tm.tableCatalogLayout = records.NewLayout(tableCatalogSchema)

	fieldCatalogSchema := records.NewSchema()
	fieldCatalogSchema.AddStringField(tableNameField, maxNameLength)
	fieldCatalogSchema.AddStringField(fieldNameField, maxNameLength)
	fieldCatalogSchema.AddIntField(typeField)
	fieldCatalogSchema.AddIntField(lengthField)
	fieldCatalogSchema.AddIntField(offsetField)
	tm.fieldCatalogLayout = records.NewLayout(fieldCatalogSchema)

	if isNew {
		if err := tm.CreateTable(tableCatalogTableName, tableCatalogSchema, tx); err != nil {
			return nil, fmt.Errorf("failed to create table catalog: %w", err)
		}
		if err := tm.CreateTable(fieldCatalogTableName, fieldCatalogSchema, tx); err != nil {
			return nil, fmt.Errorf("failed to create field catalog: %w", err)
		}
	}
	return tm, nil
}

// CreateTable creates a new table in the database.
// It first inserts the table's layout into the table catalog, 
// then inserts each field's metadata into the field catalog.
func (tm *TableManager) CreateTable(tableName string, schema *records.Schema, tx *transaction.Transaction) error {
	tableLayout := records.NewLayout(schema)

	if err := tm.insertIntoTableCatalog(tx, tableName, tableLayout); err != nil {
		return fmt.Errorf("failed to insert into table catalog: %w", err)
	}

	if err := tm.insertIntoFieldCatalog(tx, tableName, schema, tableLayout); err != nil {
		return fmt.Errorf("failed to insert into field catalog: %w", err)
	}

	return nil
}

// insertIntoTableCatalog inserts a new entry into the table catalog for the given table name and layout.
// It first creates a new table scan for the table catalog, then inserts a new record and sets the table name and slot size fields.
func (tm *TableManager) insertIntoTableCatalog(
	tx *transaction.Transaction,
	tableName string,
	tableLayout *records.Layout,
) error {

	tableCatalog, err := tablescan.NewTableScan(
		tx,
		tableCatalogTableName,
		tm.tableCatalogLayout,
	)
	if err != nil {
		return fmt.Errorf("failed to create table catalog scan: %w", err)
	}

	if err := tableCatalog.Insert(); err != nil {
		return fmt.Errorf("failed to insert into table catalog: %w", err)
	}

	if err := tableCatalog.SetString(tableNameField, tableName); err != nil {
		return fmt.Errorf("failed to set table name: %w", err)
	}

	if err := tableCatalog.SetInt(slotSizeField, tableLayout.SlotSize()); err != nil {
		return fmt.Errorf("failed to set slot size: %w", err)
	}

	return tableCatalog.Close()
}

// insertIntoFieldCatalog inserts the schema of a table into the field catalog.
// Each field in the schema is inserted as a separate record into the field catalog.
func (tm *TableManager) insertIntoFieldCatalog(
	tx *transaction.Transaction,
	tableName string,
	schema *records.Schema,
	tableLayout *records.Layout,
) error {

	fieldCatalog, err := tablescan.NewTableScan(
		tx,
		fieldCatalogTableName,
		tm.fieldCatalogLayout,
	)
	if err != nil {
		return fmt.Errorf("failed to create field catalog scan: %w", err)
	}

	for _, field := range schema.Fields() {

		if err := fieldCatalog.Insert(); err != nil {
			return fmt.Errorf("failed to insert into field catalog: %w", err)
		}

		if err := fieldCatalog.SetString(tableNameField, tableName); err != nil {
			return fmt.Errorf("failed to set table name: %w", err)
		}

		if err := fieldCatalog.SetString(fieldNameField, field); err != nil {
			return fmt.Errorf("failed to set field name: %w", err)
		}

		if err := fieldCatalog.SetInt(
			typeField,
			int(schema.Type(field)),
		); err != nil {
			return fmt.Errorf("failed to set field type: %w", err)
		}

		if err := fieldCatalog.SetInt(
			lengthField,
			schema.Length(field),
		); err != nil {
			return fmt.Errorf("failed to set field length: %w", err)
		}

		if err := fieldCatalog.SetInt(
			offsetField,
			tableLayout.Offset(field),
		); err != nil {
			return fmt.Errorf("failed to set field offset: %w", err)
		}
	}

	return fieldCatalog.Close()
}


func (tm *TableManager) GetLayout(tableName string, tx *transaction.Transaction) (*records.Layout, error) {
	size := -1

	// Read slot size from table_catalog
	tableCatalog, err := tablescan.NewTableScan(
		tx,
		tableCatalogTableName,
		tm.tableCatalogLayout,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create table catalog scan: %w", err)
	}

	for {
		hasNext, err := tableCatalog.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to scan table catalog: %w", err)
		}
		if !hasNext {
			break
		}

		currentTableName, err := tableCatalog.GetString(tableNameField)
		if err != nil {
			return nil, fmt.Errorf("failed to get table name: %w", err)
		}

		if currentTableName == tableName {
			size, err = tableCatalog.GetInt(slotSizeField)
			if err != nil {
				return nil, fmt.Errorf("failed to get slot size: %w", err)
			}
			break
		}
	}

	if err := tableCatalog.Close(); err != nil {
		return nil, fmt.Errorf("failed to close table catalog scan: %w", err)
	}

	if size == -1 {
		return nil, fmt.Errorf("table %q not found", tableName)
	}

	schema := records.NewSchema()
	offsets := make(map[string]int)

	// Read fields from field_catalog
	fieldCatalog, err := tablescan.NewTableScan(
		tx,
		fieldCatalogTableName,
		tm.fieldCatalogLayout,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create field catalog scan: %w", err)
	}

	for {
		hasNext, err := fieldCatalog.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to scan field catalog: %w", err)
		}
		if !hasNext {
			break
		}

		currentTableName, err := fieldCatalog.GetString(tableNameField)
		if err != nil {
			return nil, fmt.Errorf("failed to get table name: %w", err)
		}

		if currentTableName != tableName {
			continue
		}

		fieldName, err := fieldCatalog.GetString(fieldNameField)
		if err != nil {
			return nil, fmt.Errorf("failed to get field name: %w", err)
		}

		fieldType, err := fieldCatalog.GetInt(typeField)
		if err != nil {
			return nil, fmt.Errorf("failed to get field type: %w", err)
		}

		fieldLength, err := fieldCatalog.GetInt(lengthField)
		if err != nil {
			return nil, fmt.Errorf("failed to get field length: %w", err)
		}

		fieldOffset, err := fieldCatalog.GetInt(offsetField)
		if err != nil {
			return nil, fmt.Errorf("failed to get field offset: %w", err)
		}

		schema.AddField(
			fieldName,
			records.SchemaType(fieldType),
			fieldLength,
		)

		offsets[fieldName] = fieldOffset
	}

	if err := fieldCatalog.Close(); err != nil {
		return nil, fmt.Errorf("failed to close field catalog scan: %w", err)
	}

	return records.NewLayoutFromMetadata(schema, offsets, size), nil
}



func (tm *TableManager) TableCatalogLayout() *records.Layout {
	return tm.tableCatalogLayout
}

func (tm *TableManager) FieldCatalogLayout() *records.Layout {
	return tm.fieldCatalogLayout
}
