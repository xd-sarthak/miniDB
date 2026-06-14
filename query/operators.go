package query

import "fmt"

type Operator int

const (
	// NONE represents the absence of an operator.
	NONE Operator = -1
	// EQ represents the equality operator (==).
	EQ Operator = iota
	// NE represents the inequality operator (!=).
	NE
	// LT represents the less than operator (<).
	LT
	// LE represents the less than or equal to operator (<=).
	LE
	// GT represents the greater than operator (>).
	GT
	// GE represents the greater than or equal to operator (>=).
	GE
)

func (op Operator) String() string {
	switch op {
	case EQ:
		return "=="
	case NE:
		return "!="
	case LT:
		return "<"
	case LE:
		return "<="
	case GT:
		return ">"
	case GE:
		return ">="
	default:
		return "unknown operator"
	}
}

// OperatorFromString returns the Operator from the given string.
func OperatorFromString(op string) (Operator, error) {
	switch op {
	case "=", "==":
		return EQ, nil
	case "<>", "!=":
		return NE, nil
	case "<":
		return LT, nil
	case "<=":
		return LE, nil
	case ">":
		return GT, nil
	case ">=":
		return GE, nil
	default:
		return NONE, fmt.Errorf("invalid operator: %s", op)
	}
}