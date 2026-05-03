package buffer

import "sync"

type replacementStrategy interface {
	// initialise bufferpool
	initialize(buffers []*Buffer)
	// pinBuffer notifies the strategy that a buffer has been pinned
	pinBuffer(buffer *Buffer)
	// unpinBuffer notifies the strategy that a buffer has been unpinned
	unpinBuffer(buffer *Buffer)
	// chooseUnpinned picks an unpinned buffer
	chooseUnpinned() *Buffer
}

type NaiveStrategy struct {
	replacementStrategy
	buffers []*Buffer
	mu sync.Mutex
}

// intialize initializes the strategy with the given buffers
func NewNaiveStrategy() *NaiveStrategy {
	return &NaiveStrategy{}
}

// initialize the strategy with the buffer pool
func (ns *NaiveStrategy) initialize(buffers []*Buffer) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.buffers = buffers
}

// notifies the strategy that a buffer has been pinned
// no action is needed for the naive strategy
func (ns *NaiveStrategy) pinBuffer(buffer *Buffer) {
	// no action needed for naive strategy
}

// notifies the strategy that a buffer has been unpinned
// no action is needed for the naive strategy
func (ns *NaiveStrategy) unpinBuffer(buffer *Buffer) {
	// no action needed for naive strategy
}

// selects an unpinned buffer from the pool
func (ns *NaiveStrategy) chooseUnpinned() (*Buffer){
	ns.mu.Lock()
	defer ns.mu.Unlock()
	for _, buffer := range ns.buffers {
		if !buffer.isPinned() {
			return buffer
		}
	}
	return nil
}
