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
// It also handles flushing of dirty buffers
// It uses a replacement strategy to choose which unpinned buffer to use when pinning a new block
type Manager struct {
	bufferPool	[]*Buffer
	numAvailable int
	mu 		     sync.Mutex // thread safety
	cond		*sync.Cond // thread sleeps when no buffers are available
	strategy 	replacementStrategy
}

// NewManager creates a buffer manager having the specified number of buffer slots.
// It depends on a file.Manager and log.Manager instance. Uses the Naive replacement strategy by default.
func NewManager(fileManager *file.Manager, logManager *log.Manager, numBuffers int) *Manager {
	return NewManagerWithReplacementStrategy(fileManager, logManager, numBuffers, NewNaiveStrategy())
}

// NewManagerWithReplacementStrategy creates a buffer manager with a given replacement strategy having the specified number of buffer slots.
// It depends on a file.Manager and log.Manager instance.
func NewManagerWithReplacementStrategy(fileManager *file.Manager, logManager *log.Manager, numBuffers int, strategy replacementStrategy) *Manager {
	bm := &Manager{
		bufferPool:   make([]*Buffer, numBuffers),
		numAvailable: numBuffers,
		strategy:     strategy,
	}
	bm.cond = sync.NewCond(&bm.mu)
	for i := 0; i < numBuffers; i++ {
		bm.bufferPool[i] = NewBuffer(fileManager, logManager)
	}
	// initialize the strategy with the buffer pool
	strategy.initialize(bm.bufferPool)
	return bm
}

// Available returns the number of available (i.e., unpinned) buffers.
func (m *Manager) Available() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.numAvailable
}


// flushes the dirty buffers modified by the specified transaction to disk
func (m *Manager) FlushAll(txnNum int) error {
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

	if !buff.isPinned() {
		panic("buffer manager unpin called on an unpinned buffer")
	}
	buff.unpin()
	m.strategy.unpinBuffer(buff) // notify the strategy that a buffer has been unpinned
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

	// this runs after context expires
	stop := context.AfterFunc(ctx, func() {
		// we acquire cond.L to make sure broadcast doesnt happen before call to wait -> Deadlock

		// Scenario Without Locking in AfterFunc:
		//
		// 1. Goroutine A (Waiter) Starts:
		// - Acquires cond.L.Lock().
		// - Checks conditionMet(), which returns false.
		// - Enters the loop and is about to call cond.Wait().
		//
		// 2. Context Cancellation Occurs:
		// - The AfterFunc is triggered.
		// - Without locking cond.L, it calls cond.Broadcast() immediately.
		//
		// 3. Goroutine A Calls cond.Wait():
		// - cond.Wait() releases the lock (which it already holds), but since it was not held during Broadcast(), there's no synchronization.
		// - Goroutine A begins waiting.
		//
		// 4. Missed Signal:
		// - Since cond.Broadcast() was called before Goroutine A was actually waiting, Goroutine A misses the signal.
		// - No further broadcasts are scheduled.
		// - Goroutine A remains blocked indefinitely, leading to a deadlock.
		m.cond.L.Lock()
		m.cond.Broadcast()
		m.cond.L.Unlock()
	})

	// calling the returned stop function stops the association of ctx with func
	defer stop()

		for {
			if buff, err := m.tryToPin(block); err != nil {
				return nil,err
			} else if buff != nil {
				// successfully pinned the buffer, so we can stop waiting
				return buff, nil
			}
			m.cond.Wait()

			// check if the context has timed out
			if ctx.Err() != nil {
				// check if the wait timed out, if yes, retur a buffer abort exception
				// client should abort and retry the transaction
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return nil, fmt.Errorf("timed out waiting for buffer to become available: %w", ctx.Err())
				}
				// otherwise, return the error that occurred while waiting
				return nil, fmt.Errorf("error while waiting for buffer to become available: %v", ctx.Err())
			}
		}

	}



// tryToPin tries to pin a buffer to the specified block.
// If there is already a buffer assigned to that block, it uses that buffer.
// Otherwise, it chooses an unpinned buffer from the pool.
// Returns nil if there are no available buffers.
// This method is not thread-safe.
func (m *Manager) tryToPin(block *file.BlockID) (*Buffer, error) {
	buffer := m.findExistingBuffer(block)
	if buffer == nil {
		buffer = m.strategy.chooseUnpinned()
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
	m.strategy.pinBuffer(buffer) // notify the strategy that a buffer has been pinned
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
