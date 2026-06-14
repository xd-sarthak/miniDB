package plan_impl

import (
	"github.com/xd-sarthak/miniDB/plan"
	"github.com/xd-sarthak/miniDB/query"
	"github.com/xd-sarthak/miniDB/query/functions"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/scan"
	"github.com/xd-sarthak/miniDB/transaction"
)

var _ plan.Plan = &GroupByPlan{}

type GroupByPlan struct {
	inputPlan            plan.Plan
	groupFields          []string
	aggregationFunctions []functions.AggregationFunction
	schema               *records.Schema
}

// NewGroupByPlan creates a grorupbyy plan for the underlying
// query. The grouping is determined by the specified collection
// of group fields, and the aggregation is computed by the specified
// aggregation functions.
func NewGroupByPlan(transaction *transaction.Transaction, inputPlan plan.Plan, groupFields []string, aggregationFunctions []functions.AggregationFunction) *GroupByPlan {
	gbp := &GroupByPlan{
		inputPlan:            NewSortPlan(transaction, inputPlan, groupFields),
		groupFields:          groupFields,
		aggregationFunctions: aggregationFunctions,
		schema:               records.NewSchema(),
	}

	for _, field := range groupFields {
		gbp.schema.Add(field, gbp.inputPlan.Schema())
	}

	for _, f := range aggregationFunctions {
		gbp.schema.AddIntField(f.FieldName())
	}

	return gbp
}

// Open opens a sort plan for the specified plan.
// The sort plan ensures that the underlying records
// will be appropriately grouped.
func (p *GroupByPlan) Open() (scan.Scan, error) {
	sortScan, err := p.inputPlan.Open()
	if err != nil {
		return nil, err
	}

	groupByScan, err := query.NewGroupByScan(sortScan, p.groupFields, p.aggregationFunctions)
	if err != nil {
		return nil, err
	}

	return groupByScan, nil
}

// BlocksAccessed returns the estimated number of block accesses
// required to compute the aggregation,
// which is one pass through the sorted table.
// It does not include the one-time cost of materializing and sorting the records.
func (p *GroupByPlan) BlocksAccessed() int {
	return p.inputPlan.BlocksAccessed()
}

// RecordsOutput returns the number of groups. Assuming equal distribution,
// this is the product of the distinct values of each grouping field.
func (p *GroupByPlan) RecordsOutput() int {
	numGroups := 1
	for _, field := range p.groupFields {
		numGroups *= p.inputPlan.DistinctValues(field)
	}
	return numGroups
}

// DistinctValues are the number of distinct values for the specified field.
// If the field is a grouping field, then the number of distinct values is the
// same as in the underlying query.
// If the field is an aggregation field, then we assume that all the values are distinct.
func (p *GroupByPlan) DistinctValues(fieldName string) int {
	if p.schema.HasField(fieldName) {
		return p.inputPlan.DistinctValues(fieldName)
	}
	return p.RecordsOutput()
}

// Schema returns the schema of the output table.
// The schema consists of the grouping fields and the aggregation fields.
func (p *GroupByPlan) Schema() *records.Schema {
	return p.schema
}
