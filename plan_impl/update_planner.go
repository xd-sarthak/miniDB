package plan_impl

import (
	"github.com/xd-sarthak/miniDB/parser"
	"github.com/xd-sarthak/miniDB/transaction"
)

type UpdatePlanner interface {
	// ExecuteInsert executes the specified insert statement, and
	// returns the numbeb of affected records.
	ExecuteInsert(data *parser.InsertData, transaction *transaction.Transaction) (int, error)

	// ExecuteDelete executes the specified delete statement, and
	// returns the number of affected records.
	ExecuteDelete(data *parser.DeleteData, transaction *transaction.Transaction) (int, error)

	// ExecuteModify executes the specified modify statement, and
	// returns the number of affected records.
	ExecuteModify(data *parser.ModifyData, transaction *transaction.Transaction) (int, error)

	// ExecuteCreateTable executes the specified create table statement, and
	// returns the number of affected records.
	ExecuteCreateTable(data *parser.CreateTableData, transaction *transaction.Transaction) (int, error)

	// ExecuteCreateView executes the specified create view statement, and
	// returns the number of affected records.
	ExecuteCreateView(data *parser.CreateViewData, transaction *transaction.Transaction) (int, error)

	// ExecuteCreateIndex executes the specified create index statement, and
	// returns the number of affected records.
	ExecuteCreateIndex(data *parser.CreateIndexData, transaction *transaction.Transaction) (int, error)
}
