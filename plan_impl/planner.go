package plan_impl

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/parser"
	"github.com/xd-sarthak/miniDB/plan"
	"github.com/xd-sarthak/miniDB/transaction"
)

type Planner struct {
	queryPlanner  QueryPlanner
	updatePlanner UpdatePlanner
}

func NewPlanner(queryPlanner QueryPlanner, updatePlanner UpdatePlanner) *Planner {
	return &Planner{
		queryPlanner:  queryPlanner,
		updatePlanner: updatePlanner,
	}
}

// CreateQueryPlan creates a plan for a SQL select statement, using the supplied planner.
func (planner *Planner) CreateQueryPlan(sql string, transaction *transaction.Transaction) (plan.Plan, error) {
	p := parser.NewParser(sql)
	data, err := p.Query()
	if err != nil {
		return nil, err
	}
	if err := verifyQuery(data); err != nil {
		return nil, err
	}
	return planner.queryPlanner.CreatePlan(data, transaction)
}

// ExecuteUpdate executes a SQL insert, delete, modify, or create statement.
// The method dispatches to the appropriate method of the supplied update planner,
// depending on what the parser returns.
func (planner *Planner) ExecuteUpdate(sql string, transaction *transaction.Transaction) (int, error) {
	p := parser.NewParser(sql)
	data, err := p.UpdateCmd()
	if err != nil {
		return 0, err
	}

	if err := verifyUpdate(data); err != nil {
		return 0, err
	}

	switch data.(type) {
	case *parser.InsertData:
		return planner.updatePlanner.ExecuteInsert(data.(*parser.InsertData), transaction)
	case *parser.DeleteData:
		return planner.updatePlanner.ExecuteDelete(data.(*parser.DeleteData), transaction)
	case *parser.ModifyData:
		return planner.updatePlanner.ExecuteModify(data.(*parser.ModifyData), transaction)
	case *parser.CreateTableData:
		return planner.updatePlanner.ExecuteCreateTable(data.(*parser.CreateTableData), transaction)
	case *parser.CreateViewData:
		return planner.updatePlanner.ExecuteCreateView(data.(*parser.CreateViewData), transaction)
	case *parser.CreateIndexData:
		return planner.updatePlanner.ExecuteCreateIndex(data.(*parser.CreateIndexData), transaction)
	default:
		return 0, fmt.Errorf("unexpected type %T", data)
	}
}

func verifyQuery(data *parser.QueryData) error {
	// TODO: Implement this
	return nil
}

func verifyUpdate(data any) error {
	// TODO: Implement this
	return nil
}
