package buffer

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/file"
	"github.com/xd-sarthak/miniDB/log"
)

// Buffer -> individual buffer 
// data buffer wraps page and stores info about it
type Buffer struct {
	fileManager   *file.Manager
	logManager    *log.Manager
	contents      *file.Page
	block         *file.BlockID
	pins           int
	txnNum         int64 //transaction number
	lsn            int64
}

func NewBuffer(fileManager *file.Manager, logManager *log.Manager) *Buffer {
	return &Buffer{
		fileManager: fileManager,
		logManager: logManager,
		contents: file.NewPage(fileManager.BlockSize()),
		block: nil,
		pins: 0,
		txnNum: -1,
		lsn: -1,
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

// Block returns a reference to the disk block allocated to the buffer.
func (b *Buffer) Block() *file.BlockID {
	return b.block
}

func (b *Buffer) SetModified(txnNum, lsn int64) {
	b.txnNum = txnNum

	// If LSN is smaller than 0, it indicates that a log record was not generated for this update.
	if lsn >= 0 {
		b.lsn = lsn
	}
}

// isPinned returns true if the buffer is currently pinned (that is, if it has a nonzero pin count).
func (b *Buffer) isPinned() bool {
	return b.pins > 0
}

func (b *Buffer) modifyingTxn() int64 {
	return b.txnNum
}


// reads content of specified block into buffer content
// if buffer was dirty then flush previous content to disk before reading new block
func (b *Buffer) assignToBlock(block *file.BlockID) error {
	if err := b.flush(); err != nil {
		if b.block != nil {
			return fmt.Errorf("failed to flush buffer for block %s: %w", b.block.String(), err)
		}
		return fmt.Errorf("failed to flush buffer before assigning block %s: %w", block.String(), err)
	}
	b.block = block
	if err := b.fileManager.Read(block, b.contents); err != nil {
		return fmt.Errorf("failed to read block %s into buffer: %w", block.String(), err)
	}
	b.pins = 0
	return nil
}

// flush writes the buffer to its disk block if it is dirty (that is, if it has been modified since it was last read from disk).
func (b *Buffer) flush() error {
	if b.block != nil && b.txnNum >= 0 {
		if err := b.logManager.Flush(b.lsn); err != nil {
			return fmt.Errorf("failed to flush log for buffer with block %s: %w", b.block.String(), err)
		}
		if err := b.fileManager.Write(b.block, b.contents); err != nil {
			return fmt.Errorf("failed to write buffer contents to block %s: %w", b.block.String(), err)
		}
		b.txnNum = -1
	}
	return nil
}


// pin increases the buffer's pin count.
func (b *Buffer) pin() {
	b.pins++
}

// unpin decreases the buffer's pin count.
func (b *Buffer) unpin() {
	if b.pins == 0 {
		panic("buffer unpin called with pin count 0")
	}
	b.pins--
}
