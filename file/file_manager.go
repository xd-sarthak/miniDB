package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"errors"
)

type Manager struct {
	dbStoragePath string // db root storage path
	blocksize     int 
	isNew         bool
	mu            sync.Mutex //threadsafety
	openFiles     map[string]*os.File //cache
	blocksRead	  int
	blocksWritten int
}

func NewManager(dbDirectory string, blocksize int) (*Manager,error) {
	isNew := false

	// thought is if db doesnt exist create new db
	// os.stat checks if exists
	if _,err := os.Stat(dbDirectory); os.IsNotExist(err) {
		isNew = true
		// 0755 is basically octal code to give control to program
		if err := os.MkdirAll(dbDirectory,0755); err != nil {
			return nil, fmt.Errorf("Cannot create directory %s: %v",dbDirectory,err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("cannot access directory %s: %v",dbDirectory,err)
	}

	// cleanup of any temp files
	entries, err := os.ReadDir(dbDirectory)
	if err != nil {
		return nil,fmt.Errorf("cannot access directory %s: %v",dbDirectory,err)
	}

	for _,entry := range entries {
		if !entry.IsDir(){ //if entry not a folder
			name := entry.Name()
			if len(name) >= 5 && name[:5] == "temp_"{
				tempFilePath := filepath.Join(dbDirectory,name)
				if err := os.Remove(tempFilePath); err != nil {
					return nil,fmt.Errorf("cannot remove file %s : %v",tempFilePath,err)
				}
			}
		}
	}

	return &Manager{
		dbStoragePath: dbDirectory,
		blocksize: blocksize,
		isNew: isNew,
		openFiles: make(map[string]*os.File),
		blocksRead: 0,
		blocksWritten: 0,
	},nil
}

// reads blocks from the file into the Page (memory)
func (m *Manager) Read(block *BlockID, page *Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	file,err := m.getFile(block.Filename())
	if err != nil {
		return fmt.Errorf("cannot read block %s: %v", block.String(), err)
	}

	// so if block number is 4 and block size is 8 os reads from byte 32
	offset := int64(block.Number())*int64(m.blocksize)

	if _,err := file.Seek(offset,io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek to offset %d: %v", offset, err)
	}

	buffer := page.Contents()
	n,err := io.ReadFull(file,buffer)

	if err == nil && n == len(buffer) {
		m.blocksRead++
		return nil
	}

	// handle eof case
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		if n == 0 {
			m.blocksRead++
			return nil // EOF reached, but no data read, treat as empty block
		}
		// Partial read, fill the rest of the buffer with zeros
		return fmt.Errorf("partial read: expected %d bytes, got %d", len(buffer), n)
	}

	// 	handle other errors
	if err != nil {
		return fmt.Errorf("cannot read data: %v", err)
	}

	// handle short read case
	return fmt.Errorf("short read: expected %d bytes, got %d ", len(buffer),n)

}

// Write to the disk from the page (memory) to the file
func (m *Manager) Write(block *BlockID, page *Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.getFile(block.Filename())
	if err != nil {
		return fmt.Errorf("cannot write block %s: %v", block.String(), err)
	}

	offset := int64(block.Number()) * int64(m.blocksize)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek to offset %d: %v", offset, err)
	}

	buf := page.Contents()
	n, err := f.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write data: %v", err)
	}
	if n != len(buf) {
		return fmt.Errorf("short write: expected %d bytes, wrote %d", len(buf), n)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("cannot flush file %s to disk: %v", block.Filename(), err)
	}

	m.blocksWritten++
	return nil
}

// appends a new block to the file and return blockID
// append creates block -> returns blockID -> write into that block
/*
	File has N blocks
        ↓
	Append called
        ↓
	New block = N
        ↓
	Write empty block at offset N × blockSize
        ↓
	File now has N+1 blocks
*/
func(m *Manager) Append(filename string) (BlockID, error){
	m.mu.Lock()
	defer m.mu.Unlock()

	newBlockNumber,err := m.length(filename)
	if err != nil {
		return BlockID{},fmt.Errorf("cannot get length of %s: %v", filename, err)
	}

	block := BlockID{File: filename, BlockNum: newBlockNumber}

	f, err := m.getFile(filename)
	if err != nil {
		return BlockID{},fmt.Errorf("cannot append block %s: %v", block.String(), err)
	}

	offset := int64(block.Number()) * int64(m.blocksize)
	if _,err := f.Seek(offset,io.SeekStart); err != nil {
		return BlockID{},fmt.Errorf("cannot seek to offset %d: %v", offset, err)
	}

	b := make([]byte,m.blocksize)
	n,err := f.Write(b)

	if err != nil {
		return BlockID{},fmt.Errorf("cannot write data: %v", err)
	}

	if n != len(b) {
		return BlockID{}, fmt.Errorf("short write: expected %d bytes, wrote %d", len(b), n)
	}

	// Ensure the data is flushed to disk.
	if err := f.Sync(); err != nil {
		return BlockID{}, fmt.Errorf("cannot sync file %s: %v", filename, err)
	}

	m.blocksWritten++

	return block, nil
}


func (m *Manager) IsNew() bool {
	return m.isNew
}

// BlockSize returns the block size used by the FileMgr.
func (m *Manager) BlockSize() int {
	return m.blocksize
}

// getFile retrieves or opens a file and stores it in the openFiles map.
func (m *Manager) getFile(filename string) (*os.File, error) {
	// if in cache
	if f, ok := m.openFiles[filename]; ok {
		return f, nil
	}

	dbTable := filepath.Join(m.dbStoragePath, filename)
	f, err := os.OpenFile(dbTable, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0666)
	if err != nil {
		return nil, fmt.Errorf("cannot open file %s: %v", dbTable, err)
	}

	m.openFiles[filename] = f
	return f, nil
}

// returns the number of blocks read from disk
func (m *Manager) GetBlocksRead() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.blocksRead	
}

// returns the number of blocks written to disk
func (m *Manager) GetBlocksWritten() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.blocksWritten
}

// Length returns the number of blocks in the specified file. This method is thread-safe.
func (m *Manager) Length(filename string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.length(filename)
}

// length returns the number of blocks in the specified file. This method is not thread-safe.
func (m *Manager) length(filename string) (int, error) {
	f, err := m.getFile(filename)
	if err != nil {
		return 0, fmt.Errorf("cannot access %s: %v", filename, err)
	}

	fileInfo, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("cannot stat %s: %v", filename, err)
	}

	fileSizeInBytes := fileInfo.Size()
	return int(fileSizeInBytes / int64(m.blocksize)), nil
}