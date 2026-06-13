package plan_impl

import (
	"github.com/xd-sarthak/miniDB/parser"
	"github.com/xd-sarthak/miniDB/plan"
	"github.com/xd-sarthak/miniDB/transaction"
)

// QueryPlanner is an interface implemented by planners for the SQL select statement.
type QueryPlanner interface {
	// CreatePlan creates a query plan for the specified query data.
	CreatePlan(queryData *parser.QueryData, transaction *transaction.Transaction) (plan.Plan, error)
}
