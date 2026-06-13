package query

// expression.go extracted the values 
// term.go will use the expression to evaluate the condition and return true or false

import (
	"fmt"
	"time"
	"github.com/xd-sarthak/miniDB/records"
	"github.com/xd-sarthak/miniDB/scan"
	"github.com/xd-sarthak/miniDB/plan"
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

// CompareSupportedTypes handles comparison for supported types. It is the
// exported entry point used by other packages (btree, functions, etc.).
func CompareSupportedTypes(lhs, rhs any, op Operator) bool {
	// Handle nil values explicitly
	if lhs == nil || rhs == nil {
		return false // Null comparisons always return false in SQL semantics
	}

	// Type-specific comparisons
	switch lhs := lhs.(type) {
	case int:
		if rhs, ok := rhs.(int); ok {
			return compareInts(lhs, rhs, op)
		}
	case int64:
		if rhs, ok := rhs.(int64); ok {
			return compareInt64s(lhs, rhs, op)
		}
	case int16:
		if rhs, ok := rhs.(int16); ok {
			return compareInt16s(lhs, rhs, op)
		}
	case string:
		if rhs, ok := rhs.(string); ok {
			return compareStrings(lhs, rhs, op)
		}
	case bool:
		if rhs, ok := rhs.(bool); ok {
			return compareBools(lhs, rhs, op)
		}
	case time.Time:
		if rhs, ok := rhs.(time.Time); ok {
			return compareTimes(lhs, rhs, op)
		}
	default:
		// Log unsupported type for debugging
		fmt.Printf("Unsupported type for comparison: lhs=%T, rhs=%T\n", lhs, rhs)
	}

	// Return false for unsupported or mismatched types
	return false
}

// compareInts compares two integers.
func compareInts(lhs, rhs int, op Operator) bool {
	switch op {
	case LT:
		return lhs < rhs
	case LE:
		return lhs <= rhs
	case GT:
		return lhs > rhs
	case GE:
		return lhs >= rhs
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false
	}
}

// compareInt64s compares two int64 values.
func compareInt64s(lhs, rhs int64, op Operator) bool {
	switch op {
	case LT:
		return lhs < rhs
	case LE:
		return lhs <= rhs
	case GT:
		return lhs > rhs
	case GE:
		return lhs >= rhs
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false
	}
}

// compareInt16s compares two int16 values.
func compareInt16s(lhs, rhs int16, op Operator) bool {
	switch op {
	case LT:
		return lhs < rhs
	case LE:
		return lhs <= rhs
	case GT:
		return lhs > rhs
	case GE:
		return lhs >= rhs
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false
	}
}

// compareStrings compares two strings.
func compareStrings(lhs, rhs string, op Operator) bool {
	switch op {
	case LT:
		return lhs < rhs
	case LE:
		return lhs <= rhs
	case GT:
		return lhs > rhs
	case GE:
		return lhs >= rhs
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false
	}
}

// compareBools compares two booleans (only equality comparisons make sense).
func compareBools(lhs, rhs bool, op Operator) bool {
	switch op {
	case EQ:
		return lhs == rhs
	case NE:
		return lhs != rhs
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false // Invalid for comparison operators like <, >
	}
}

// compareTimes compares two time.Time values.
func compareTimes(lhs, rhs time.Time, op Operator) bool {
	switch op {
	case LT:
		return lhs.Before(rhs)
	case LE:
		return lhs.Before(rhs) || lhs.Equal(rhs)
	case GT:
		return lhs.After(rhs)
	case GE:
		return lhs.After(rhs) || lhs.Equal(rhs)
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false
	}
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
		return distinctValues
	case LT, LE, GT, GE:
		// Assume uniform distribution; halve the distinct values for range operators.
		return max(1, distinctValues/2)
	default:
		return distinctValues // Default for unsupported operators
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