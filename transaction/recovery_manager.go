package transaction

import (
	"time"
	"github.com/xd-sarthak/miniDB/buffer"
	"github.com/xd-sarthak/miniDB/log"
)

// RecoveryManager is responsible for recovering transactions from the log.
// It provides methods for committing and rolling back transactions, as well as for recovering from crashes.
// commit writes a commit record to the log and flushes all buffers modified by the transaction to disk.
// rollback writes a rollback record to the log and flushes all buffers modified by the transaction to disk.
// recover reads the log and applies the necessary changes to recover from a crash.
type RecoveryManager struct {
	logManager *log.Manager
	bufferManager *buffer.Manager
	transaction   *Transaction
	txNum          int
}

// NewRecoveryManager creates a new RecoveryManager for the given transaction and transaction number.
func NewRecoveryManager(tx *Transaction, txNum int, logManager *log.Manager, bufferManager *buffer.Manager) *RecoveryManager {
	return &RecoveryManager{
		logManager:    logManager,
		bufferManager: bufferManager,
		transaction:   tx,
		txNum:           txNum,
	}
}

func (rm *RecoveryManager) Commit() error {
	// flushes all buffers modified by the transaction to disk and writes a commit record to the log
	if err := rm.bufferManager.FlushAll(rm.txNum); err != nil {
		return err
	}

	// write commit record to log
	lsn, err := WriteCommitToLog(rm.logManager,rm.txNum)
	if err != nil {
		return err
	}
	//flushes commit log record to the disk
	return rm.logManager.Flush(lsn)
}

// Rollback rolls back the txn, writes a rollback record to the log and flushes it to the disk
func (rm *RecoveryManager) Rollback() error {
	if err := rm.doRollback(); err != nil {
		return err
	}

	if err := rm.bufferManager.FlushAll(rm.txNum); err != nil {
		return err
	}
	lsn, err := WriteRollbackToLog(rm.logManager, rm.txNum)
	if err != nil {
		return err
	}
	return rm.logManager.Flush(lsn)
}

// Recover recovers uncompleted transactions from the log
// and then writes a quiescent checkpoint record to the log anf flushes it
func (rm *RecoveryManager) Recover() error {
	if err := rm.doRecover(); err != nil {
		return err
	}

	if err := rm.bufferManager.FlushAll(rm.txNum); err != nil {
		return err
	}
	lsn, err := WriteCheckpointToLog(rm.logManager)
	if err != nil {
		return err
	}
	return rm.logManager.Flush(lsn)
}

// SetInt writes a SetInt record to the log and returns its lsn.
func (rm *RecoveryManager) SetInt(buffer *buffer.Buffer, offset int, newVal int) (int, error) {
	oldVal := buffer.Contents().GetInt(offset)
	block := buffer.Block()
	return WriteSetIntToLog(rm.logManager, rm.txNum, block, offset, oldVal)
}

// SetString writes a SetString record to the log and returns its lsn.
func (rm *RecoveryManager) SetString(buffer *buffer.Buffer, offset int, newVal string) (int, error) {
	oldVal, err := buffer.Contents().GetString(offset)
	if err != nil {
		return -1, err
	}
	block := buffer.Block()
	return WriteSetStringToLog(rm.logManager, rm.txNum, block, offset, oldVal)
}

// SetBool writes a SetBool record to the log and returns its lsn.
func (rm *RecoveryManager) SetBool(buffer *buffer.Buffer, offset int, newVal bool) (int, error) {
	oldVal := buffer.Contents().GetBool(offset)
	block := buffer.Block()
	return WriteSetBoolToLog(rm.logManager, rm.txNum, block, offset, oldVal)
}

// SetShort writes a SetShort record to the log and returns its lsn.
func (rm *RecoveryManager) SetShort(buffer *buffer.Buffer, offset int, newVal int16) (int, error) {
	oldVal := buffer.Contents().GetShort(offset)
	block := buffer.Block()
	return WriteSetShortToLog(rm.logManager, rm.txNum, block, offset, oldVal)
}

// SetDate writes a SetDate record to the log and returns its lsn.
func (rm *RecoveryManager) SetDate(buffer *buffer.Buffer, offset int, newVal time.Time) (int, error) {
	oldVal := buffer.Contents().GetDate(offset)
	block := buffer.Block()
	return WriteSetDateToLog(rm.logManager, rm.txNum, block, offset, oldVal)
}


// SetLong writes a SetLong record to the log and returns its lsn.
func (rm *RecoveryManager) SetLong(buffer *buffer.Buffer, offset int, newVal int64) (int, error) {
	oldVal := buffer.Contents().GetLong(offset)
	block := buffer.Block()
	return WriteSetLongToLog(rm.logManager, rm.txNum, block, offset, oldVal)
}


// doRollback rolls back the transaction,
// by iterating through the log records until it finds the transaction's Start record,
// calling Undo() for each of the transaction's log records.
func (rm *RecoveryManager) doRollback() error {
	iter, err := rm.logManager.Iterator()
	if err != nil {
		return err
	}

	// iterate through the log records
	for iter.HasNext() {
		bytes, err := iter.Next()
		if err != nil {
			return err
		}

		// create a log record from the bytes
		logRecord, err := CreateLogRecord(bytes)
		if err != nil {
			return err
		}

		// if this log record is related to the transaction, undo it.
		if logRecord.TxNum() == rm.txNum {
			// if this is the start record, break the loop.
			// We have successfully undone all the changes related to this transaction.
			if logRecord.OP() == Start {
				break
			}
			if err := logRecord.Undo(rm.transaction); err != nil {
				return err
			}
		}
	}
	return nil
}


// doRecovers performs a complete database recovery.
// The method iterates through the log records.
// Whenever it finds a log record for an unfinished transaction,
// it calls Undo() on that record.
// The method stops when it encounters a Checkpoint record or the end of the log.
func (rm *RecoveryManager) doRecover() error {
	finishedTransactions := make([]int, 0, 10)
	iter, err := rm.logManager.Iterator()
	if err != nil {
		return err
	}

	for iter.HasNext() {
		bytes, err := iter.Next()
		if err != nil {
			return err
		}

		logRecord, err := CreateLogRecord(bytes)
		if err != nil {
			return err
		}

		if logRecord.OP() == Checkpoint {
			return nil
		}

		if logRecord.OP() == Commit || logRecord.OP() == Rollback {
			finishedTransactions = append(finishedTransactions, logRecord.TxNum())
		} else if !contains(finishedTransactions, logRecord.TxNum()) {
			if err := logRecord.Undo(rm.transaction); err != nil {
				return err
			}
		}
	}
	return nil
}

// Generic contains function for slices of any comparable type
func contains[T comparable](slice []T, element T) bool {
	for _, v := range slice {
		if v == element {
			return true
		}
	}
	return false
}
