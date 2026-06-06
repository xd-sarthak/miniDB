package query

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/scan"
)

//this file the layer that knows how to get a value from the current row
// and whether the value is a constant or a field name

/*

example majorId = 10
here majorId is the field name and 10 is the constant value

*/

type Expression struct {
	fieldName string
	value	  any
}

// NewFieldExpression creates a new expression for a field name
func NewFieldExpression(fieldName string) *Expression {
	return &Expression{fieldName: fieldName}
}

// NewConstantExpression creates a new expression for a constant value
func NewConstantExpression(value any) *Expression {
	return &Expression{value: value}
}

// Evaluate evaluates the expression and returns the value
// basically for current row what value does this expression represent
func (e *Expression) Evaluate(s scan.Scan) (any, error) {
	if e.value != nil {
		return e.value, nil
	}
	return s.GetVal(e.fieldName)
}

// IsFieldName returns true if the expression is a field reference.
func (e *Expression) IsFieldName() bool {
	return e.fieldName != ""
}

// IsConstant returns true if the expression is a constant expression,
// or nil if the expression does not denote a constant.
func (e *Expression) asConstant() any {
	return e.value
}

// IsFieldName returns the field name if the expression is a field reference,
// or an empty string if the expression does not denote a field.
func (e *Expression) asFieldName() string {
	return e.fieldName
}

// AppliesTo determines if all the fields mentioned in this expression are contained in the specified schema.
func (e *Expression) AppliesTo(schema *records.Schema) bool {
	return e.value != nil || schema.HasField(e.fieldName)
}

func (e *Expression) String() string {
	if e.value != nil {
		return fmt.Sprintf("%v", e.value)
	}
	return e.fieldName
}

