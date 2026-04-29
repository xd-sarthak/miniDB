package file

import "fmt"

// db stores data in blocks so this is the ID of a block
type BlockID struct {
	File string // which file
	BlockNum int // which block in the file
}

// construct a pointer to disk
func NewBlockID(filename string, blockNum int) *BlockID {
	return &BlockID{
		File: filename,
		BlockNum: blockNum,
	}
}

func (b *BlockID) Filename() string {
	return b.File
}

func (b *BlockID) Number() int {
	return b.BlockNum
}

func (b *BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]",b.File,b.BlockNum)
}
