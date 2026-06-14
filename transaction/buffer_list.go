package transaction

import (
	"github.com/xd-sarthak/miniDB/buffer"
	"github.com/xd-sarthak/miniDB/file"
)

// pinnedBuffer holds a buffer and its reference count.
type pinnedBuffer struct {
	buffer *buffer.Buffer
	refCount int
}

// BufferList manages a transaction's currently pinned buffers.
type BufferList struct {
	buffers       map[file.BlockID]*pinnedBuffer
	bufferManager *buffer.Manager
}

// NewBufferList creates a new BufferList.
func NewBufferList(bufferManager *buffer.Manager) *BufferList {
	return &BufferList{
		buffers:       make(map[file.BlockID]*pinnedBuffer),
		bufferManager: bufferManager,
	}
}

// GetBuffer returns the buffer pinned to the specified block.
// The method returns nil if the transaction has not pinned the block.
func (bl *BufferList) GetBuffer(block *file.BlockID) *buffer.Buffer {
	pinnedBuf, ok := bl.buffers[*block]
	if !ok {
		return nil
	}
	return pinnedBuf.buffer
}

// Pin pins the block and keeps track of the buffer internally.
func (bl *BufferList) Pin(block *file.BlockID) error {
	if pinnedBuf, ok := bl.buffers[*block]; ok {
		// Already pinned by this transaction; just increase refCount
		pinnedBuf.refCount++
		return nil
	}

	// Not pinned yet; ask bufferManager for a fresh pin
	buff, err := bl.bufferManager.Pin(block)
	if err != nil {
		return err
	}
	bl.buffers[*block] = &pinnedBuffer{
		buffer:   buff,
		refCount: 1,
	}
	return nil
}

// Unpin unpins the block and removes it from the internal list of pinned buffers.
func (bl *BufferList) Unpin(block *file.BlockID) {
	pinnedBuf, ok := bl.buffers[*block]
	if !ok {
		// This block isn't pinned or was already unpinned.
		// In production, you might log a warning or return silently.
		return
	}
	pinnedBuf.refCount--
	if pinnedBuf.refCount <= 0 {
		// Now fully unpin from buffer manager and remove from our map
		bl.bufferManager.Unpin(pinnedBuf.buffer)
		delete(bl.buffers, *block)
	}
}

// UnpinAll unpins all the blocks and clears the internal list of pinned buffers.
//
// Pin only forwards to bufferManager.Pin once per distinct block (subsequent
// pins of the same block by this transaction merely bump refCount), so the
// buffer manager holds exactly one pin per distinct block in the map.
// Therefore UnpinAll must call bufferManager.Unpin exactly once per distinct
// buffer — unpinning refCount times would over-unpin and trip the buffer
// manager's unpinned-buffer guard.
func (bl *BufferList) UnpinAll() {
	for _, pinnedBuf := range bl.buffers {
		bl.bufferManager.Unpin(pinnedBuf.buffer)
	}
	// Clear our map
	bl.buffers = make(map[file.BlockID]*pinnedBuffer)
}
