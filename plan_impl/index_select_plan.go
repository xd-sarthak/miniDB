package plan_impl

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/metadata"
	"github.com/xd-sarthak/miniDB/plan"
	"github.com/xd-sarthak/miniDB/query"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/scan"
	"github.com/xd-sarthak/miniDB/tablescan"
)

var _ plan.Plan = &IndexSelectPlan{}

type IndexSelectPlan struct {
	inputPlan plan.Plan
	indexInfo *metadata.IndexInfo
	value     any
}

// NewIndexSelectPlan creates a new indexselect node in the query tree
// for the specified index and selection constant.
func NewIndexSelectPlan(inputPlan plan.Plan, indexInfo *metadata.IndexInfo, value any) *IndexSelectPlan {
	return &IndexSelectPlan{
		inputPlan: inputPlan,
		indexInfo: indexInfo,
		value:     value,
	}
}

// Open creates a new indexselect scan for this query.
func (isp *IndexSelectPlan) Open() (scan.Scan, error) {
	inputScan, err := isp.inputPlan.Open()
	if err != nil {
		return nil, err
	}
	tableScan, ok := inputScan.(*tablescan.TableScan)
	if !ok {
		return nil, fmt.Errorf("IndexSelectPlan requires a tablescan")
	}
	idx := isp.indexInfo.Open()
	return query.NewIndexSelectScan(tableScan, idx, isp.value)
}

// BlocksAccessed returns the estimated number of block accesses
// to compute the index selection, which is the same as the index
// traversal cost plus the number of matching data records.
func (isp *IndexSelectPlan) BlocksAccessed() int {
	return isp.indexInfo.BlocksAccessed() + isp.RecordsOutput()
}

// RecordsOutput returns the estimated number of records in the
// index selection, which is the same as the number of search
// key values for the index.
func (isp *IndexSelectPlan) RecordsOutput() int {
	return isp.indexInfo.RecordsOutput()
}

// DistinctValues returns the estimated number of distinct values
// as defined by the index.
func (isp *IndexSelectPlan) DistinctValues(fieldName string) int {
	return isp.indexInfo.DistinctValues(fieldName)
}

// Schema returns the schema of the data table.
func (isp *IndexSelectPlan) Schema() *records.Schema {
	return isp.inputPlan.Schema()
}
