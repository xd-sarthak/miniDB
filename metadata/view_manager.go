package metadata

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/tablescan"
	"github.com/xd-sarthak/miniDB/transaction"

)

/*

VIEW -> basically like a query shortcut

CREATE VIEW cs_students AS
SELECT *
FROM student
WHERE majorid = 10; means that we can now do SELECT * FROM cs_students; 
and it will run the query above for us. 
It's just a shortcut to make our lives easier.

we store viewname and viewdefinition in the view manager.

*/

const (
	maxViewDefinitionLength = 100
	viewNameField		  = "view_name"
	viewDefinitionField	  = "view_definition"
	viewCatalogTableName  = "view_catalog"
)

// ViewManager manages the creation and retrieval of views in the database.
type ViewManager struct {
	tableManager *TableManager
}

// NewViewManager initializes a new ViewManager. 
// If isNew is true, it creates the view catalog table in the database.
func NewViewManager(isNew bool,tableManager *TableManager, tx *transaction.Transaction) (*ViewManager, error) {
	vm := &ViewManager{tableManager: tableManager}

	if isNew {
		schema := records.NewSchema()
		schema.AddStringField(viewNameField, maxNameLength)
		schema.AddStringField(viewDefinitionField, maxViewDefinitionLength)

		if err := vm.tableManager.CreateTable(viewCatalogTableName, schema, tx); err != nil {
			return nil, fmt.Errorf("failed to create view catalog table: %v", err)
		}
	}
	return vm, nil
}

// CreateView creates a new view with the given name and definition, and stores it in the view catalog.
func (vm *ViewManager) CreateView(viewName, viewDefinition string, tx *transaction.Transaction) error {
	// get layout of catalog name
	layout, err := vm.tableManager.GetLayout(viewCatalogTableName,tx)
	if err != nil {
		return err
	}

	viewCatalogTableScan, err := tablescan.NewTableScan(tx,viewCatalogTableName,layout)
	if err != nil {
		return fmt.Errorf("failed to create table scan: %v", err)
	}

	if err := viewCatalogTableScan.Insert(); err != nil {
		return fmt.Errorf("failed to insert into view catalog: %v", err)
	}

	if err := viewCatalogTableScan.SetString(viewNameField, viewName); err != nil {
		return fmt.Errorf("failed to set view name: %v", err)
	}
	
	if err := viewCatalogTableScan.SetString(viewDefinitionField, viewDefinition); err != nil {
		return fmt.Errorf("failed to set view definition: %v", err)
	}

	return viewCatalogTableScan.Close()

}

// GetViewDefinition retrieves the view definition for a given view name from the view catalog.
func (vm *ViewManager) GetViewDefinition(viewName string, tx *transaction.Transaction) (string, error) {
	var result string

	layout, err := vm.tableManager.GetLayout(viewCatalogTableName,tx)
	if err != nil {
		return "", err
	}

	viewCatalogTableScan, err := tablescan.NewTableScan(tx,viewCatalogTableName,layout)
	if err != nil {
		return "", fmt.Errorf("failed to create table scan: %v", err)
	}

	for {
		hasNext, err := viewCatalogTableScan.Next()
		if err != nil {
			return "", fmt.Errorf("failed to iterate through view catalog: %v", err)
		}
		if !hasNext {
			break
		}

		name, err := viewCatalogTableScan.GetString(viewNameField)
		if err != nil {
			return "", fmt.Errorf("failed to get view name: %v", err)
		}

		if name == viewName {
			result, err = viewCatalogTableScan.GetString(viewDefinitionField)
			if err != nil {
				return "", fmt.Errorf("failed to get view definition: %v", err)
			}
			break
		}
	}
	return result, nil
}

/*

let 
CREATE VIEW cs_students AS
SELECT *
FROM student
WHERE majorid = 10;

NewViewManager creates the catalog table -> view_catalog with view_name and view_definition fields.

Then in CreateView():
1. We get the layout of the view_catalog table.
2. We create a new table scan on the view_catalog table.
3. We insert a new record into the view_catalog table.
4. We set the view_name and view_definition fields of the new record to the provided values.
5. We close the table scan.

| view_name   | view_definition                          |
| ----------- | ---------------------------------------- |
| cs_students | SELECT * FROM student WHERE majorid = 10 |

when a query runs like SELECT * FROM cs_students; we call GetViewDefinition("cs_students") to get the view definition, 
which is "SELECT * FROM student WHERE majorid = 10".
 We can then execute that query to get the results.

*/