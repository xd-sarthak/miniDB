package buffer

import (
	"context"
	"errors"
	"fmt"
	"github.com/xd-sarthak/miniDB/file"
	"github.com/xd-sarthak/miniDB/log"
	"sync"
	"time"
)

// max time to wait for a buffer to become available
const maxWaitTime = 10 * time.Second

// Manager the pinning and unpinning of buffers to blocks
type Manager struct {
	bufferPool	[]*Buffer
	numAvailable int
	mu 		     sync.Mutex // thread safety
	cond		*sync.Cond // thread sleeps when no buffers are available
}

// creates a buffer manager having the specified number of buffer slots
func NewManager(fileManager *file.Manager, logManager *log.Manager, numBuffers int) *Manager {
	m := &Manager{
		bufferPool: make([]*Buffer,numBuffers),
		numAvailable: numBuffers,
	}
	m.cond = sync.NewCond(&m.mu)

	for i := 0; i < numBuffers; i++ {
		m.bufferPool[i] = NewBuffer(fileManager, logManager)
	}
	return m
}

// Available returns the number of available (i.e., unpinned) buffers.
func (m *Manager) Available() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.numAvailable
}


// flushes the dirty buffers modified by the specified transaction to disk
func (m *Manager) FlushAll(txnNum int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, buff := range m.bufferPool {
		if buff.modifyingTxn() == txnNum {
			if err := buff.flush(); err != nil {
				return fmt.Errorf("failed to flush buffer for block %s with txnNum %d: %w", buff.Block().String(), txnNum, err)
			}
	}
}
	return nil
}

// unpins the specfied buffer
// if picnt -> zero, it becomes available for other transactions to pin
func (m *Manager) Unpin(buff *Buffer){
	m.mu.Lock()
	defer m.mu.Unlock()

	buff.unpin()
	if !buff.isPinned() {
		m.numAvailable++
		m.cond.Broadcast() // signal waiting threads that a buffer has become available
	}
}

// pimns a buffer to speicfied block potentially waiting until a buffer becomes available
// if no buffer becomes available within maxWaitTime, then returns an error
// uses conditional with wait pattern
/*
mu.Lock()
for !condition {
    cond.Wait()
}
# do work (condition is now true)
mu.Unlock()
*/

/*

Lock →
    Try to pin →
        If success → return
        Else → wait (cond)
            Repeat until:
                success OR timeout
Unlock

*/

func (m *Manager) Pin(block *file.BlockID) (*Buffer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), maxWaitTime)
	defer cancel()

	var buff *Buffer
	var err error

	waitOnCond := func() error {
		// set up a goroutine to wait for the condition to be signaled or for the context to timeout
		done := make(chan struct{})
		defer close(done)

		go func() {
			// mimicking a 10s timeout for waiting for a buffer to become available
			select {
				case <-ctx.Done():
					m.mu.Lock()
					// wake up the waiting goroutine
					m.cond.Broadcast()
					m.mu.Unlock()
				case <-done:
					// the pinning operation completed successfully, so we can stop waiting

			}
		}()

		for {
			if buff, err = m.tryToPin(block); err != nil {
				return err
			}
			if buff != nil {
				// successfully pinned the buffer, so we can stop waiting
				break
			}
			m.cond.Wait()

			// check if the context has timed out
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}

		return nil
	}

	if err := waitOnCond(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("timed out waiting for buffer to become available: %w", err)
		}
		return nil, fmt.Errorf("error while waiting for buffer to become available: %v", err)
	}
	return buff, nil
}


// tryToPin tries to pin a buffer to the specified block.
// If there is already a buffer assigned to that block, it uses that buffer.
// Otherwise, it chooses an unpinned buffer from the pool.
// Returns nil if there are no available buffers.
// This method is not thread-safe.
func (m *Manager) tryToPin(block *file.BlockID) (*Buffer, error) {
	buffer := m.findExistingBuffer(block)
	if buffer == nil {
		buffer = m.chooseUnpinnedBuffer()
		if buffer == nil {
			return nil, nil
		}
		if err := buffer.assignToBlock(block); err != nil {
			return nil, err
		}
	}
	if !buffer.isPinned() {
		m.numAvailable--
	}
	buffer.pin()
	return buffer, nil
}


// findExistingBuffer searches for a buffer assigned to the specified block.
func (m *Manager) findExistingBuffer(block *file.BlockID) *Buffer {
	for _, buffer := range m.bufferPool {
		b := buffer.Block()
		if b != nil && b.Equals(block) {
			return buffer
		}
	}
	return nil
}

// chooseUnpinnedBuffer returns an unpinned buffer from the pool or nil if none are available.
func (m *Manager) chooseUnpinnedBuffer() *Buffer {
	for _, buffer := range m.bufferPool {
		if !buffer.isPinned() {
			return buffer
		}
	}
	return nil
}
