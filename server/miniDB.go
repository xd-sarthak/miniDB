package server

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/buffer"
	"github.com/xd-sarthak/miniDB/file"
	"github.com/xd-sarthak/miniDB/log"
	"github.com/xd-sarthak/miniDB/transaction"
	"github.com/xd-sarthak/miniDB/metadata"
	"github.com/xd-sarthak/miniDB/transaction/concurrency"
)

const (
	blockSize = 400
	bufferPoolSize = 8
	logFileName = "minidb.log"
)

type MiniDB struct {
	fileManager *file.Manager
	bufferManager *buffer.Manager
	logManager *log.Manager
	mdManager *metadata.Manager
	lockTable *concurrency.LockTable
}

// NewMiniDBWithOptions creates a new instance of MiniDB with the specified options.
// used for debugging and testing purposes.
func NewMiniDBWithOptions(dirName string, blockSize, bufferSize int) (*MiniDB, error) {
	db := &MiniDB{}
	var err error

	if db.fileManager, err = file.NewManager(dirName, blockSize); err != nil {
		return nil, err
	}
	if db.logManager, err = log.NewManager(db.fileManager, logFileName); err != nil {
		return nil, err
	}
	db.bufferManager = buffer.NewManager(db.fileManager, db.logManager, bufferSize)
	db.lockTable = concurrency.NewLockTable()

	return db, nil
}

// NewMiniDB creates a new instance of MiniDB with default settings.
func NewMiniDB(dbDirectory string) (*MiniDB, error) {
	db,err := NewMiniDBWithOptions(dbDirectory, blockSize, bufferPoolSize)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize MiniDB: %v", err)
	}

	tx := db.NewTx()
	isNew := db.fileManager.IsNew()
	if isNew {
		fmt.Println("Initializing new database...")
	} else {
		fmt.Println("Existing database found. Performing recovery...")
		if err := tx.Recover(); err != nil {
			return nil, fmt.Errorf("recovery failed: %v", err)
		}
	}

	db.mdManager,err = metadata.NewManager(isNew, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metadata manager: %v", err)
	}

	// TODO: QueryPlanner, UpdatePlanner, etc.
	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %v", err)
	}

	return db, nil
}













func (db *MiniDB) NewTx() *transaction.Transaction {
	return transaction.NewTransaction(db.fileManager, db.logManager, db.bufferManager)
}

func (db *MiniDB) MetadataManager() *metadata.Manager {
	return db.mdManager
}

func (db *MiniDB) FileManager() *file.Manager {
	return db.fileManager
}

func (db *MiniDB) LogManager() *log.Manager {
	return db.logManager
}

func (db *MiniDB) BufferManager() *buffer.Manager {
	return db.bufferManager
}
