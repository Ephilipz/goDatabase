package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/Ephilipz/goDatabase/platformio"
)

type KV struct {
	Path string
	//internals
	file *os.File
	tree BTree
	mmap struct {
		fileSize int		// can be larger than db size
		totalSize int		// mmap size, can be larger thatn file size
		chunks [][]byte		// multiple mmaps, can be non continouos
	}
	page struct {
		flushed uint64		// database size in number of pages
		temp	[][]byte	// newly allocated pages
	}
}

// DB Signature is a 16 byte word used to verify the master page
const DB_SIG = "hemimetamorphous"

// the master page format.
// contains the pointer to the root and number of used pages
// | sig |	btree_root	|	page_used	|
// | 16B |		8B		|		8B		|
func masterLoad(db *KV) error {
	if db.mmap.fileSize == 0 {
		// empty file, master page will be created on first write
		db.page.flushed = 1
		return nil
	}
	// load the data of the master page (1st page in chunks[])
	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	usedPages := binary.LittleEndian.Uint64(data[24:])
	// verify the signal
	if !bytes.Equal([]byte(DB_SIG), data[:16]) {
		return errors.New("Bad signature")
	}
	// verify number of used pages is not 0 and does not exceed allowed filesize
	invalid := usedPages < 1 || usedPages > uint64(db.mmap.fileSize/BTREE_PAGE_SIZE)
	// verify the root page pointer is not 0 and not larger than used
	invalid = invalid || root < 0 || root > usedPages
	if invalid {
		return errors.New("Bad master page")
	}
	// create root node
	db.tree.root = root
	db.page.flushed = usedPages
	return nil	
}

// update the master page. This is atomic
func masterUpdate(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	// NOTE: updating using mmap is not atomic, we use write() instead
	if _, err := db.file.WriteAt(data[:], 0); err != nil {
		return fmt.Errorf("write master page failed: %w", err)
	}
	return nil
}

// the initial map can exceed the file size
// instead of using mremap to extend the mmap range, we create a new mapping for the overflow range
func extendMmap(db *KV, npages int) error {
	if db.mmap.totalSize >= npages * BTREE_PAGE_SIZE {
		return nil
	}
	// double the address space
	chunk, err := platformio.Mmap(int(db.file.Fd()), int64(db.mmap.totalSize), db.mmap.totalSize)
	if err != nil {
		return err
	}
	// update total size of mmap to double
	db.mmap.totalSize <<= 1
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func (db *KV) pageGet(ptr uint64) BNode {
	var pageStart uint64 = 0
	for _, chunk := range db.mmap.chunks {
		pageEnd := pageStart + uint64(len(chunk)) / BTREE_PAGE_SIZE
		// ptr is in the range start ... end
		if ptr < pageEnd {
			offset := BTREE_PAGE_SIZE * (ptr - pageStart)
			return BNode{chunk[offset : offset + BTREE_PAGE_SIZE]}
		}
		pageStart = pageEnd
	}
	panic("bad pointer!")
}

// create the initial mmap that covers the whole file
func mmapInit(file *os.File) (int, []byte, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}
	if fileInfo.Size() % BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("File size is not a multiple of page size")
	}
	mmapSize := 64 << 20
	assert(mmapSize % BTREE_PAGE_SIZE == 0)
	for mmapSize < int(fileInfo.Size()) {
		mmapSize <<= 1
	}
	chunk, err := platformio.Mmap(int(file.Fd()), 0, mmapSize)
	if err != nil {
		return 0, nil, err
	}
	return int(fileInfo.Size()), chunk, nil
}
