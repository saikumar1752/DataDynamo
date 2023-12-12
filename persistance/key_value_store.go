package persistance

import (
	"fmt"
	"os"
	"syscall"
	"errors"
	"github.com/saikumar1752/MyDB/data_structures/b_tree"
	"github.com/saikumar1752/MyDB/data_structures"
)

type KV struct{
	Path string
	fp *os.File
	tree b_tree.BTree
	mmap struct {
		file int
		total int
		chunks [][]byte
	}
	page struct {
		flushed uint64
		temp [][]byte
	}
}

func mmapInit(fp *os.File)(int, []byte, error){
	fi, err :=fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("start: %w", err)
	}
	if fi.Size()% data_structures.BTREE_PAGE_SIZE !=0 {
		return 0, nil, errors.New("File size is not a multple of page size.")
	}

	mmapSize := 64<<20
	for mmapSize < int(fi.Size()){
		mmapSize *= mmapSize
	}
	chunk, err := syscall.Mmap(int(fp.Fd()), 0, mmapSize, syscall.PROT_READ | syscall.PROT_WRITE, syscall.MAP_SHARED,)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}
	return int(fi.Size()), chunk, nil
}

func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*data_structures.BTREE_PAGE_SIZE{
		return nil
	}
	chunk, err := syscall.Mmap(int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total, syscall.PROT_READ | syscall.PROT_WRITE, syscall.MAP_SHARED,)

	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total+=db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func (db *KV) pageGet(ptr uint64) data_structures.BNode{
	start := uint64(0)
	for _, chunk := range db.mmap.chunks{
		end := start + uint64(len(chunk))/data_structures.BTREE_PAGE_SIZE
		if ptr<end {
			offset := data_structures.BTREE_PAGE_SIZE*(ptr-start)
			var page data_structures.BNode
			page.Initialize(chunk[offset: offset+data_structures.BTREE_PAGE_SIZE])			
		}
		start=end
	}
	panic("bad ptr")
}

func (db *KV) pageNew(node data_structures.BNode) uint64{
	ptr := db.page.flushed+uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node.GetAllData())
	return ptr
}


func (db *KV) pageDel(uint64){

}

func (db *KV) extendFile(npages int) error {
	filepages := db.mmap.file / data_structures.BTREE_PAGE_SIZE
	if filepages >= npages{
		return nil
	}
	for filepages < npages{
		inc := filepages / 8
		if inc < 1{
			inc=1
		}
		filepages += inc
	}
	fileSize := filepages + data_structures.BTREE_PAGE_SIZE
}