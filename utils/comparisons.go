package utils

import (
	"fmt"
	"time"
)

// Operator is the comparison operator used by CompareSupportedTypes.
// It is defined here (a leaf package) so that both the query package and the
// aggregation functions package can share comparison logic without creating an
// import cycle.
type Operator int

const (
	NONE Operator = -1
	EQ   Operator = iota
	NE
	LT
	LE
	GT
	GE
)

// CompareSupportedTypes handles comparison for supported types.
func CompareSupportedTypes(lhs, rhs any, op Operator) bool {
	// Handle nil values explicitly
	if lhs == nil || rhs == nil {
		return false // Null comparisons always return false in SQL semantics
	}

	// First try to unify integer types.
	if lhsInt, lhsIsInt := toInt(lhs); lhsIsInt {
		if rhsInt, rhsIsInt := toInt(rhs); rhsIsInt {
			return compareInts(lhsInt, rhsInt, op)
		}
	}

	switch lhs := lhs.(type) {
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
		fmt.Printf("Unsupported or mismatched types for comparison: lhs=%T, rhs=%T\n", lhs, rhs)
	}

	return false
}

// toInt attempts to convert an interface to int.
func toInt(i any) (int, bool) {
	switch v := i.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case int16:
		return int(v), true
	default:
		return 0, false
	}
}

func compareInts(lhs, rhs int, op Operator) bool {
	switch op {
	case NE:
		return lhs != rhs
	case EQ:
		return lhs == rhs
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

func compareStrings(lhs, rhs string, op Operator) bool {
	switch op {
	case NE:
		return lhs != rhs
	case EQ:
		return lhs == rhs
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

func compareBools(lhs, rhs bool, op Operator) bool {
	switch op {
	case EQ:
		return lhs == rhs
	case NE:
		return lhs != rhs
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false
	}
}

func compareTimes(lhs, rhs time.Time, op Operator) bool {
	switch op {
	case NE:
		return !lhs.Equal(rhs)
	case EQ:
		return lhs.Equal(rhs)
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
