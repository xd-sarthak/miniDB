package records

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/utils"
	"github.com/xd-sarthak/miniDB/file"
	"sort"
)

/*

translation layer between schema and actual record layout on disk

the NewLayout is basically

1. Determine alignment requirements
2. Sort fields
3. Insert padding
4. Assign offsets
5. Compute final slot size
6. Align slot itself

*/

// Layout represents the layout of records on disk
type Layout struct {
    schema   *Schema // logical definition of the record structure
    offsets  map[string]int // physical byte locations
    slotSize int // total size of each record in bytes
}

func NewLayout(schema *Schema) *Layout {
	layout := &Layout{
		schema:  schema,
		offsets: make(map[string]int),
	}

	// step 1: Determine the alignment and sizes of fields
	fieldAlignments := make(map[string]int)
	for _, field := range schema.Fields() {
		fieldAlignments[field] = alignmentRequirement(schema.Type(field))
	}

	// step 2: Sort fields by alignment requirements (largest first)
	// This ensures that fields with larger alignment requirements are placed first, minimizing padding
	fields := schema.Fields()
	sort.Slice(fields, func(i, j int) bool {
		return fieldAlignments[fields[i]] > fieldAlignments[fields[j]]
	})

	pos := utils.IntSize // Reserve space of empty/in-use field

	for _, field := range fields {
		align := fieldAlignments[field]

		// step 3: Insert padding if needed to meet alignment requirements
		if pos%align != 0 {
			padding := align - (pos % align)
			pos += padding
		}

		// step 4: Assign offset for the field
		layout.offsets[field] = pos

		// move position by the size of the field
		pos += layout.lengthInBytes(field)

	}

	// step 5: Compute final slot size
	// align the total slot size to alignment requirements
	largestAlignment := maxAlignment(fieldAlignments)
	if pos%largestAlignment != 0 {
		padding := largestAlignment - (pos % largestAlignment)
		pos += padding
	}

	// step 6: Assign final slot size
	layout.slotSize = pos

	return layout

}

// NewLayoutFromMetadata creates a new layout from the specified metadata.
// This method is used when the metadata is retrieved from the catalog.
func NewLayoutFromMetadata(schema *Schema, offsets map[string]int, slotSize int) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

// Schema returns the schema of the table's records.
func (l *Layout) Schema() *Schema {
	return l.schema
}

func (l *Layout) Offset(fieldName string) int {
	return l.offsets[fieldName]
}
// SlotSize returns the size of a record slot in bytes.
func (l *Layout) SlotSize() int {
	return l.slotSize
}

// lengthInBytes returns the length of a field in bytes.
func (l *Layout) lengthInBytes(fieldName string) int {
	fieldType := l.schema.Type(fieldName)

	switch fieldType {
	case Integer:
		return utils.IntSize
	case Long:
		return 8 // 8 bytes for long
	case Short:
		return 2 // 2 bytes for short
	case Boolean:
		return 1 // 1 byte for boolean
	case Date:
		return 8 // 8 bytes for date (64 bit Unix timestamp)
	case Varchar:
		return file.MaxLength(l.schema.Length(fieldName))
	default:
		panic(fmt.Sprintf("Unknown field type: %d", fieldType))
	}
}

/*

eg let schema be
bool active
long id
short type

then sorted order is 
long
short
bool

intially pos = 4 (for empty/in-use field)
now for long we need to add 4 bytes of padding to align it to 8 bytes
so long starts at 8 and += 8 = 16 so pos = 16
now short we need to align by 2 and pos = 16%2 == 0 so no padding
pos = 16 + 2 = 18
now bool we need to align by 1 and pos = 18%1 == 0 so no padding
pos = 18 + 1 = 19

now we need to align the total slot size to largest alignment which is 8
so we need to add 5 bytes of padding to make it 24
so final slot size is 24 bytes

layout is
offset 0: empty/in-use field (4 bytes)
offset 4: padding (4 bytes)
offset 8: long id (8 bytes)
offset 16: short type (2 bytes)
offset 18: bool active (1 byte)
offset 19-23: padding (5 bytes)
*/


