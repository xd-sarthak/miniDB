package records

import "fmt"

// ID represents a unique identifier for a record, 
// composed of a block number and a slot number.
type ID struct {
	blockNumber int
	slot        int
}

// NewID creates a new ID instance with the given block number and slot.
func NewID(blockNumber, slot int) *ID {
	return &ID{
		blockNumber: blockNumber,
		slot:        slot,
	}
}

// BlockNumber returns the block number of this ID.
func (id *ID) BlockNumber() int {
	return id.blockNumber
}

// Slot returns the slot number of this ID.
func (id *ID) Slot() int {
	return id.slot
}

func (id *ID) Equals(other *ID) bool {
	return id.blockNumber == other.blockNumber && id.slot == other.slot
}

func (id *ID) String() string {
	return fmt.Sprintf("[%d, %d]", id.blockNumber, id.slot)
}
