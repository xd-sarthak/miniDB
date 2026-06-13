package functions

import (
	"github.com/xd-sarthak/miniDB/query"
	"github.com/xd-sarthak/miniDB/scan"
)

var _ AggregationFunction = &MaxFunction{}

const maxFunctionPrefix = "maxOf"

type MaxFunction struct {
	fieldName string
	value     any
}

// NewMaxFunction creates a new max aggregation function for the specified field.
func NewMaxFunction(fieldName string) *MaxFunction {
	return &MaxFunction{
		fieldName: fieldName,
	}
}

// ProcessFirst starts a new maximum to be the field
// value in the current record.
func (f *MaxFunction) ProcessFirst(s scan.Scan) error {
	var err error
	f.value, err = s.GetVal(f.fieldName)
	return err
}

// ProcessNext replaces the current maximum with the field
// value in the current record if it is greater.
func (f *MaxFunction) ProcessNext(s scan.Scan) error {
	newValue, err := s.GetVal(f.fieldName)
	if err != nil {
		return err
	}

	if query.CompareSupportedTypes(newValue, f.value, query.GT) {
		f.value = newValue
	}

	return nil
}

// FieldName returns the field's name, prepended by maxFunctionPrefix.
func (f *MaxFunction) FieldName() string {
	return maxFunctionPrefix + f.fieldName
}

// Value returns the current maximum value.
func (f *MaxFunction) Value() any {
	return f.value
}
