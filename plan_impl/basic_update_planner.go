package plan_impl

import (
	"github.com/xd-sarthak/miniDB/metadata"
	"github.com/xd-sarthak/miniDB/parser"
	"github.com/xd-sarthak/miniDB/plan"
	"github.com/xd-sarthak/miniDB/scan"
	"github.com/xd-sarthak/miniDB/transaction"
)

var _ UpdatePlanner = &BasicUpdatePlanner{}

type BasicUpdatePlanner struct {
	metadataManager *metadata.Manager
}

// NewBasicUpdatePlanner creates a new BasicUpdatePlanner.
func NewBasicUpdatePlanner(metadataManager *metadata.Manager) UpdatePlanner {
	return &BasicUpdatePlanner{metadataManager: metadataManager}
}

func (up *BasicUpdatePlanner) ExecuteDelete(data *parser.DeleteData, transaction *transaction.Transaction) (int, error) {
	var p plan.Plan
	p, err := NewTablePlan(transaction, data.TableName(), up.metadataManager)
	if err != nil {
		return 0, err
	}

	p = NewSelectPlan(p, data.Predicate())
	s, err := p.Open()
	if err != nil {
		return 0, err
	}
	updateScan := s.(scan.UpdateScan)
	defer updateScan.Close()

	count := 0
	for {
		hasNext, err := updateScan.Next()
		if err != nil || !hasNext {
			return count, err
		}

		if err := updateScan.Delete(); err != nil {
			return count, err
		}
		count++
	}
}

func (up *BasicUpdatePlanner) ExecuteModify(data *parser.ModifyData, transaction *transaction.Transaction) (int, error) {
	var p plan.Plan
	p, err := NewTablePlan(transaction, data.TableName(), up.metadataManager)
	if err != nil {
		return 0, err
	}

	p = NewSelectPlan(p, data.Predicate())
	s, err := p.Open()
	if err != nil {
		return 0, err
	}
	updateScan := s.(scan.UpdateScan)
	defer updateScan.Close()

	count := 0
	for {
		hasNext, err := updateScan.Next()
		if err != nil || !hasNext {
			return count, err
		}

		val, err := data.NewValue().Evaluate(updateScan)
		if err != nil {
			return count, err
		}
		if err := updateScan.SetVal(data.TargetField(), val); err != nil {
			return count, err
		}
		count++
	}
}

func (up *BasicUpdatePlanner) ExecuteInsert(data *parser.InsertData, transaction *transaction.Transaction) (int, error) {
	p, err := NewTablePlan(transaction, data.TableName(), up.metadataManager)
	if err != nil {
		return 0, err
	}

	s, err := p.Open()
	if err != nil {
		return 0, err
	}
	updateScan := s.(scan.UpdateScan)
	defer updateScan.Close()

	if err := updateScan.Insert(); err != nil {
		return 0, err
	}

	vals := data.Values()
	for idx, field := range data.Fields() {
		val := vals[idx]
		if err := updateScan.SetVal(field, val); err != nil {
			return 0, err
		}
	}

	return 1, nil
}

func (up *BasicUpdatePlanner) ExecuteCreateTable(data *parser.CreateTableData, transaction *transaction.Transaction) (int, error) {
	err := up.metadataManager.CreateTable(data.TableName(), data.NewSchema(), transaction)
	return 0, err
}

func (up *BasicUpdatePlanner) ExecuteCreateView(data *parser.CreateViewData, transaction *transaction.Transaction) (int, error) {
	err := up.metadataManager.CreateView(data.ViewName(), data.ViewDefinition(), transaction)
	return 0, err
}

func (up *BasicUpdatePlanner) ExecuteCreateIndex(data *parser.CreateIndexData, transaction *transaction.Transaction) (int, error) {
	err := up.metadataManager.CreateIndex(data.IndexName(), data.TableName(), data.FieldName(), transaction)
	return 0, err
}
