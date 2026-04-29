package file

import "fmt"

// db stores data in blocks so this is the ID of a block
type BlockID struct {
	filename string // which file
	blockNum int // which block in the file
}

// construct a pointer to disk
func NewBlockID(filename string, blockNum int) *BlockID {
	return &BlockID{
		filename: filename,
		blockNum: blockNum,
	}
}

func (b *BlockID) Filename() string {
	return b.filename
}

func (b *BlockID) Number() int {
	return b.blockNum
}

func (b *BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]",b.filename,b.blockNum)
}
