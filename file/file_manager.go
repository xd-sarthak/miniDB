package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Manager struct {
	dbStoragePath string // db root storage path
	blocksize     int 
	isNew         bool
	mu            sync.Mutex //threadsafety
	openFiles     map[string]*os.File //cache
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
			if len(name) >= 4 && name[:4] == "temp"{
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
	if err != nil {
		return fmt.Errorf("cannot read data: %v",err)
	}
	if n != len(buffer) {
		return fmt.Errorf("short read: expected %d bytes, got %d", len(buffer), n)
	}

	return nil
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