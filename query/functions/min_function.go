package functions

import (
	"github.com/xd-sarthak/miniDB/query"
	"github.com/xd-sarthak/miniDB/scan"
)

var _ AggregationFunction = &MinFunction{}

const minFunctionPrefix = "minOf"

type MinFunction struct {
	fieldName string
	value     any
}

// NewMinFunction creates a new min aggregation function for the specified field.
func NewMinFunction(fieldName string) *MinFunction {
	return &MinFunction{
		fieldName: fieldName,
	}
}

// ProcessFirst starts a new minimum to be the field value in the current record.
func (f *MinFunction) ProcessFirst(s scan.Scan) error {
	val, err := s.GetVal(f.fieldName)
	if err != nil {
		return err
	}
	f.value = val
	return nil
}

// ProcessNext replaces the current minimum with the field value in the current
// record if it is smaller.
func (f *MinFunction) ProcessNext(s scan.Scan) error {
	newVal, err := s.GetVal(f.fieldName)
	if err != nil {
		return err
	}

	if query.CompareSupportedTypes(newVal, f.value, query.LT) {
		f.value = newVal
	}
	return nil
}

// FieldName returns the field's name, prepended by minFunctionPrefix.
func (f *MinFunction) FieldName() string {
	return minFunctionPrefix + f.fieldName
}

// Value returns the current minimum value.
func (f *MinFunction) Value() any {
	return f.value
}
