package kv_store

import (
	"os"
	"syscall"
	"github.com/saikumar1752/MyDB/data_structures"
	"github.com/saikumar1752/MyDB/data_structures/b_node"
)

type KV struct {
	Path string
	fp *os.File
	tree BTree
	mmap struct {
		file int
		total int
		chunks [][]byte
	}
	page struct{
		flushed uint64 // database size in number of pages
		temp [][]byte // newly allocated pages
	}
}

func extendMmap (db *KV, npages int) error {
	if db.mmap.total >= npages * BTREE_PAGE_SIZE {
		return nil
	}

	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total, syscall.PROT_READ | syscall.PROT_WRITE, syscall.MAP_SHARED
	)
	if err != nil {
		return fmt.Errorf("Mmap :%w", err)
	}
	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func (db *KV) pageGet(ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk)) / BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE  * (ptr-start)
			var bnode BNode
			bnode.InitializeWithData(chunk[offset : offset+data_structures.BTREE_PAGE_SIZE])
			return BNode.
		}
		start=end
	}
	panic("bad ptr")
}