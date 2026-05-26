package concurrency

import (
	"context"
	"errors"
	"github.com/xd-sarthak/miniDB/file"
	"sync"
	"fmt"
)

// LockTable provides methods to lock and unlock block
// If a transaction requests a lock that causes a conflict with an existing lock,
// then that transaction is placed on a wait list.
// There is only one wait list for all blocks.
// When the last lock on a block is unlocked,
// then all transactions are removed from the wait list and rescheduled.
// If one of those transactions discovers that the lock it is waiting for is still locked,
// it will place itself back on the wait list. 
const maxWaitTime = 10000 // in milliseconds

// locks -> 0 means no lock, 1 means one shared lock, n means n shared locks and -1 means exclusive locks
type LockTable struct {
	locks   map[file.BlockID]int
	mu      sync.Mutex
	cond    *sync.Cond // waiting room for a lock
}

// here mutex -> bathroom key and cond -> people waiting outside bathroom

// NewLockTabble creates a new LockTable
func NewLockTable() *LockTable {
	lt := &LockTable{
		locks : make(map[file.BlockID]int),
	}
	lt.cond = sync.NewCond(&lt.mu) // creating a condition vaiable attached to the mutex
	return lt
}

// Slock grants a shared loc on the specified block
// If an XLock exists when the method is called, then the calling thread is placed on the wait list until the lock is released.
// If the lock is not granted within a certain time limit, then the method throws a LockAbortException.
func (lt *LockTable) SLock (block *file.BlockID) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	// deadlock timeout 
	ctx,cancel := context.WithTimeout(context.Background(),maxWaitTime)
	defer cancel()

	// this function runs after context expires
	stop := context.AfterFunc(ctx, func(){
		lt.cond.L.Lock()
		lt.cond.Broadcast()
		lt.cond.L.Unlock()
	})

	defer stop()

	for {
		// If no exclusive lock go on
		if !lt.hasXLock(block) {
			// get number of shared locks
			val := lt.getLockVal(block)
			// grant the lock
			lt.locks[*block] = val + 1
			return nil
		}

		// wait untl notified or context is done
		lt.cond.Wait()

		if ctx.Err() != nil {
			if errors.Is(ctx.Err(),context.DeadlineExceeded){
				return fmt.Errorf("lock abort exception: could not acquire shared lock on block %v: %v", block, ctx.Err())
			}
			return ctx.Err()
		}
	}
}

// XLock grants an exclusive lock on the specified block.
// Assumes that the calling thread already has a shared lock on the block.
// If a lock of any type (by some other transaction) exists when the method is called,
// then the calling thread will be placed on a wait list until the locks are released.
// If the thread remains on the wait list for too long (10 seconds for now),
// then the method will return an error.
func (lt *LockTable) XLock(block *file.BlockID) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), maxWaitTime)
	defer cancel()

	stop := context.AfterFunc(ctx, func() {
		lt.cond.L.Lock()
		lt.cond.Broadcast()
		lt.cond.L.Unlock()
	})

	defer stop()

	for {
		// Assume that the calling thread already has a shared lock. If any other shared locks exist, we can't proceed.
		if !lt.hasOtherSLocks(block) {
			lt.locks[*block] = -1
			return nil
		}

		lt.cond.Wait()

		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("lock abort exception: could not acquire exclusive lock on block %v: %v", block, ctx.Err())
			}
			return ctx.Err()
		}
	}
}

// Unlock releases the lock on the specified block.
// If this lock is the last lock on that block,
// then the waiting transactions are notified.
func (lt *LockTable) Unlock(block *file.BlockID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	val := lt.getLockVal(block)
	if val > 1 {
		lt.locks[*block] = val - 1
	} else {
		delete(lt.locks, *block)
		lt.cond.Broadcast()
	}
}

// hasXLock returns true if there is an exclusive lock on the block.
func (lt *LockTable) hasXLock(block *file.BlockID) bool {
	return lt.getLockVal(block) < 0
}

// hasOtherSLocks returns true if there is more than one shared locks on the block.
func (lt *LockTable) hasOtherSLocks(block *file.BlockID) bool {
	return lt.getLockVal(block) > 1
}

func (lt *LockTable) getLockVal(block *file.BlockID) int {
	return lt.locks[*block]
}







