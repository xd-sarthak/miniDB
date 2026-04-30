package log

import (
	"fmt"
	"github.com/xd-sarthak/miniDB/file"
	"sync"
)

// append only WAL
// records grow left to right

/*

[0..3]         → boundary (int32) — offset where the last written record starts
[4..boundary)  → unused space
[boundary..end] → log records, newest first (right-to-left growth)

*/

type Manager struct {
	fileManager   *file.Manager
	logFile       string
	logPage       *file.Page
	currentBlock  *file.BlockID
	latestLSN     int
	lastSavedLSN  int
	mu            sync.Mutex
}

// creates the manager for specified log file
// if log file doesnt exist it is created with empty first block
func NewManager(fileManager *file.Manager, logFile string) (*Manager, error){
	// create a new empty page
	logPage := file.NewPage(fileManager.BlockSize())

	// get number of blocks in the log file
	logSize, err := fileManager.Length(logFile)
	if err != nil{
		return nil, fmt.Errorf("failed to get log file length: %v", err)
	}

	var currentBlock *file.BlockID
	if logSize == 0 {
		// if log is empty append a new block to it
		currentBlock,err = appendNewBlock(fileManager,logFile,logPage)
		if err != nil {
			return nil, fmt.Errorf("failed to append new block: %v", err)
		}
	} else {
		// if log file isnt empty read the last block into the page
		currentBlock = &file.BlockID{File: logFile, BlockNum: logSize-1}
		if err := fileManager.Read(currentBlock,logPage); err != nil {
			return nil,fmt.Errorf("failed to read log page: %v", err)
		}
	}

	return &Manager{
		fileManager: fileManager,
		logFile: logFile,
		logPage: logPage,
		currentBlock: currentBlock,
		latestLSN: 0,
	},nil
}


// initialises the byte buffer and appends it to the log file
func appendNewBlock(fileManager *file.Manager, logFile string, logPage *file.Page) (*file.BlockID, error) {
	// add an empty block to the log file
	block, err := fileManager.Append(logFile)
	if err != nil {
		return nil,fmt.Errorf("failed to append new block: %v", err)
	}

	// set the initial boundary for the page, flush the page we reset its contents
	// we reset the boundary 
	// intial value of boundary is the blockSize
	logPage.SetInt(0,int32(fileManager.BlockSize()))
	if err := fileManager.Write(&block,logPage); err != nil {
		return nil,fmt.Errorf("failed to write new block: %v", err)
	}
	return &block,nil
}

// UnsafeFlush writes the buffer to the log file. This method is not thread-safe.
func (m *Manager) UnsafeFlush() error {
	if err := m.fileManager.Write(m.currentBlock, m.logPage); err != nil {
		return fmt.Errorf("failed to write log page: %v", err)
	}
	m.lastSavedLSN = m.latestLSN
	return nil
}

func (m *Manager) Flush(lsn int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if lsn >= m.lastSavedLSN {
		return m.UnsafeFlush()
	}
	return nil
}

// returns as iterator for the log records in the log file
func (m *Manager) Iterator() (*Iterator, error){
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.UnsafeFlush(); err != nil {
		return nil, fmt.Errorf("failed to flush log page: %v", err)
	}
	return NewIterator(m.fileManager, m.currentBlock)
}

// Append appends a log record to the log buffer.
// The record consists of an arbitrary byte slice.
// Log records are written from right to left in the buffer.
// The size of the record is written before the bytes.
// The beginning of the buffer contains the location of the last-written record (the "boundary").
// Storing the records backwards makes it easy to read them in reverse order.
// Returns the LSN of the final value.

// [<boundary (int)>............[][recordN (bytes)]...[record1 (bytes)]]

func (m *Manager) Append(logRecord []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	//get the cuurent boundary
	boundary := int(m.logPage.GetInt(0))

	recordSize:= len(logRecord)
	bytesNeeded := recordSize + 4 // 4 bytes for the record size

	if bytesNeeded > m.fileManager.BlockSize()-4 {
		return 0, fmt.Errorf("log record too large to fit in a block")
	}

	if boundary - bytesNeeded < 4 {
		// not enough space for the new record, need to flush and start a new block
		if err := m.UnsafeFlush(); err != nil {
			return 0, fmt.Errorf("failed to flush log page: %v", err)
		}

		// allocate a new block for the log file and reset the log page
		var err error
		m.currentBlock, err = appendNewBlock(m.fileManager, m.logFile, m.logPage)
		if err != nil {
			return 0, fmt.Errorf("failed to append new block: %v", err)
		}
		boundary = int(m.logPage.GetInt(0))
	}

	recordPosition := boundary - bytesNeeded

	//write the record
	m.logPage.SetBytes(recordPosition, logRecord)
	//update the boundary
	m.logPage.SetInt(0, int32(recordPosition))
	
	m.latestLSN++
	return m.latestLSN, nil
}