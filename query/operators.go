package query

type Operator int

const (
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