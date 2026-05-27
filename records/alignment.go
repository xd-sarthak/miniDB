package records

import "github.com/xd-sarthak/miniDB/utils"

/*

CPU cares about memory alignment
eg. GOOD ALIGNMENT
address 0
address 8
address 16
address 24

eg. BAD ALIGNMENT
address 3
address 11
address 19
address 27

BAD ALIGNMENT can cause performance issues because CPU may need to do multiple 
memory accesses to read/write data that is not properly aligned.

bool -> 1
short -> 2
int -> 4
long -> 8

so for long CPU prefer -> address%8 == 0
for int CPU prefer -> address%4 == 0
for short CPU prefer -> address%2 == 0
for bool CPU prefer -> address%1 == 0 (any address is fine)

if pos = 5
then for long -> 5%8 != 0 (bad alignment)
so we need to add padding bytes to make it aligned
pos = 5 + 3 (padding) = 8 (good alignment for long)

classic space vs cpu time tradeoff
*/

const (
	LongAlignment  = 8
	ShortAlignment = 2
	BoolAlignment  = 1
	DateAlignment  = 8
	VarcharAlignment = 1
)

// alignmentRequirement returns the alignment size for a given field type.
func alignmentRequirement(fieldType SchemaType) int {
	switch fieldType {
	case Integer:
		return utils.IntSize
	case Long:
		return LongAlignment
	case Short:
		return ShortAlignment
	case Boolean:
		return BoolAlignment
	case Date:
		return DateAlignment
	case Varchar:
		return VarcharAlignment
	default:
		return 1
	}
}

// Helper function to find the maximum alignment from the map
func maxAlignment(fieldAlignments map[string]int) int {
	maxAlign := 1
	for _, align := range fieldAlignments {
		if align > maxAlign {
			maxAlign = align
		}
	}
	return maxAlign
}