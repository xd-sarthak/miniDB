package parser

import (
	"github.com/xd-sarthak/miniDB/query"
	"github.com/xd-sarthak/miniDB/query/functions"
)

// OrderByItem represents a single field in an ORDER BY clause along with its
// sort direction.
type OrderByItem struct {
	field      string
	descending bool
}

func (obi *OrderByItem) Field() string {
	return obi.field
}

func (obi *OrderByItem) Descending() bool {
	return obi.descending
}

// QueryData is the parsed representation of a select statement.
// It stores the fields to select, the tables to query from, the predicate for
// filtering, and the optional group-by / having / order-by / aggregate clauses.
type QueryData struct {
	fields     []string
	tables     []string
	predicate  *query.Predicate
	groupBy    []string                        // Fields to group by
	having     *query.Predicate                // Having clause predicate
	orderBy    []OrderByItem                   // Order by clause items
	aggregates []functions.AggregationFunction // Aggregate functions in use
}

// NewQueryData creates a new QueryData instance with the given fields, tables, and predicate.
func NewQueryData(fields, tables []string, predicate *query.Predicate) *QueryData {
	return &QueryData{
		fields:    fields,
		tables:    tables,
		predicate: predicate,
	}
}

// Fields returns the fields to select in the query.
func (qd *QueryData) Fields() []string {
	return qd.fields
}

// Tables returns the tables to query from in the query.
func (qd *QueryData) Tables() []string {
	return qd.tables
}

// Pred returns the predicate for filtering results in the query.
func (qd *QueryData) Pred() *query.Predicate {
	return qd.predicate
}

// Predicate is an alias for Pred, kept for backwards compatibility.
func (qd *QueryData) Predicate() *query.Predicate {
	return qd.predicate
}

func (qd *QueryData) GroupBy() []string {
	return qd.groupBy
}

func (qd *QueryData) Having() *query.Predicate {
	return qd.having
}

func (qd *QueryData) OrderBy() []OrderByItem {
	return qd.orderBy
}

func (qd *QueryData) Aggregates() []functions.AggregationFunction {
	return qd.aggregates
}

func (qd *QueryData) String() string {
	if len(qd.fields) == 0 || len(qd.tables) == 0 {
		return ""
	}
	result := "select "
	for _, fieldName := range qd.fields {
		result += fieldName + ", "
	}
	// remove final comma/space
	if len(qd.fields) > 0 {
		result = result[:len(result)-2]
	}
	result += " from "
	for _, tableName := range qd.tables {
		result += tableName + ", "
	}
	if len(qd.tables) > 0 {
		result = result[:len(result)-2]
	}
	predicateString := qd.predicate.String()
	if predicateString != "" {
		result += " where " + predicateString
	}
	return result
}
