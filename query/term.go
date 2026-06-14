package query

// expression.go extracted the values 
// term.go will use the expression to evaluate the condition and return true or false

import (
	"github.com/xd-sarthak/miniDB/plan"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/scan"
	"github.com/xd-sarthak/miniDB/utils"
)

type Term struct {
	lhs *Expression
	rhs *Expression
	op   Operator
}

// NewTerm creates a new term with the specified left-hand side, operator, and right-hand side.
func NewTerm(lhs,rhs *Expression,op Operator) *Term {
	return &Term{lhs:lhs, rhs:rhs,op:op}
}

// 
func (t *Term) IsSatisfied(inputScan scan.Scan) bool {
	var lhsVal, rhsVal any
	var err error
	if lhsVal, err = t.lhs.Evaluate(inputScan); err != nil {
		return false
	}

	if rhsVal, err = t.rhs.Evaluate(inputScan); err != nil {
		return false
	}

	switch t.op {
	case EQ:
		return lhsVal == rhsVal
	case NE:
		return lhsVal != rhsVal
	case LT, LE, GT, GE:
		return compareSupportedTypes(lhsVal, rhsVal, t.op)
	default:
		return false
	}
}

// compareSupportedTypes handles comparison for supported types.
func compareSupportedTypes(lhs, rhs any, op Operator) bool {
	return CompareSupportedTypes(lhs, rhs, op)
}

// CompareSupportedTypes handles comparison for supported types for all
// operators (EQ/NE/LT/LE/GT/GE). It delegates to the shared implementation in
// the utils package so the logic lives in exactly one place and is available to
// other packages (btree, functions, etc.) without an import cycle.
func CompareSupportedTypes(lhs, rhs any, op Operator) bool {
	return utils.CompareSupportedTypes(lhs, rhs, utils.Operator(op))
}

// Query Optimizer
// If i apply this condition how much smaller will the result becomme
/*

eg if majorId = 10

if 1000 records
and 10 distinct major IDs
then 1000/10 = 100 records
 so reduction factor is 10

*/


func (t *Term) ReductionFactor(p plan.Plan) int {
	var lhsName, rhsName string

	// if both sides are columns
	// pessimistic approach
	if t.lhs.IsFieldName() && t.rhs.IsFieldName() {
		lhsName = t.lhs.asFieldName()
		rhsName = t.rhs.asFieldName()
		return max(p.DistinctValues(lhsName), p.DistinctValues(rhsName))
	}

	// if RHS is a field name, use its distinct values
	if t.rhs.IsFieldName(){
		rhsName = t.rhs.asFieldName()
		return reductionForConstantComparison(p.DistinctValues(rhsName),t.op)
	}

	// if LHS is a field name, use its distinct values
	if t.lhs.IsFieldName(){
		lhsName = t.lhs.asFieldName()
		return reductionForConstantComparison(p.DistinctValues(lhsName),t.op)
	}

	// handle constant vs constant case, which is either 1 (if they are equal) or infinity (if they are not)
	lhsConst := t.lhs.asConstant()
	rhsConst := t.rhs.asConstant()

	if lhsConst == rhsConst && t.op == EQ {
		return 1 // No reduction, as all records satisfy the condition
	}
	if lhsConst != rhsConst && t.op == NE {
		return 1 // No reduction, as all records satisfy the condition
	}

	// default case for unsupported operators or mismatched constants
	return int(^uint(0) >> 1) // Return a very large number to indicate no reduction
}

// Helper to calculate reduction factor for constant comparisons using distinct values.
func reductionForConstantComparison(distinctValues int, op Operator) int {
	switch op {
	case EQ:
		return max(1, distinctValues)
	case NE:
		// Assumes non-equality doesn't significantly reduce distinct values.
		if distinctValues <= 1 {
			return 1
		}
		// approximate: the portion we keep is (distinctValues-1)/distinctValues,
		// so the factor = distinctValues/(distinctValues-1)
		return distinctValues / (distinctValues - 1)
	case LT, LE, GT, GE:
		// Assume uniform distribution; range operators keep roughly half the rows.
		return 2
	default:
		return 1 // Default for unsupported operators, assume no reduction.
	}
}


// EquatesWithConstant determines if this term is of the form "F=c"
// where F is the specified field and c is some constant.
// If so, the method returns that constant.
// If not, the method returns nil.
func (t *Term) EquatesWithConstant(fieldName string) any {
	if t.op != EQ { // Explicit check for equality
		return nil
	}
	if t.lhs.IsFieldName() && t.lhs.asFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.rhs.asConstant()
	} else if t.rhs.IsFieldName() && t.rhs.asFieldName() == fieldName && !t.lhs.IsFieldName() {
		return t.lhs.asConstant()
	}
	return nil
}


// ComparesWithConstant determines if this term is of the form "F1 < 100"
// where F1 is the specified field and the other side is a constant.
// If so, it returns (operator, constant); otherwise (NONE, nil).
func (t *Term) ComparesWithConstant(fieldName string) (Operator, any) {
	// LHS is the field, RHS is a constant
	if t.lhs.IsFieldName() && t.lhs.asFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.op, t.rhs.asConstant()
	}
	// RHS is the field, LHS is a constant
	if t.rhs.IsFieldName() && t.rhs.asFieldName() == fieldName && !t.lhs.IsFieldName() {
		return t.op, t.lhs.asConstant()
	}
	return NONE, nil
}

// EquatesWithField determines if this term is of the form "F1=F2"
// where F1 is the specified field and F2 is another field.
// If so, the method returns the name of the other field.
// If not, the method returns an empty string.
func (t *Term) EquatesWithField(fieldName string) string {
	if t.op != EQ { // Explicit check for equality
		return ""
	}
	if t.lhs.IsFieldName() && t.lhs.asFieldName() == fieldName && t.rhs.IsFieldName() {
		return t.rhs.asFieldName()
	} else if t.rhs.IsFieldName() && t.rhs.asFieldName() == fieldName && t.lhs.IsFieldName() {
		return t.lhs.asFieldName()
	}
	return ""
}

// AppliesTo returns true if both of the term's expressions
// apply to the specified schema.
func (t *Term) AppliesTo(schema *records.Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

func (t *Term) String() string {
	return t.lhs.String() + " " + t.op.String() + " " + t.rhs.String()
}