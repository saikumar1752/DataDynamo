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
			return page
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
	err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("Fallocate: %w", err)
	}
	db.mmap.file+=	fileSize
	return nil
}

func (db *KV) Open() error {
	fp, err := os.OpenFile(db.Path, os.O_RDWR | os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("Open File: %w", err)
	}
	db.fp=fp
	//create the initial mmap
	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		goto fail
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	db.tree.Get = db.pageGet
	db.tree.New = db.pageNew
	db.tree.Del = db.pageDel

	err = masterLoad(db)
	if err != nil {
		goto fail
	}

	fail:
		db.Close()
		return fmt.Errorf("KV.Open: %w", err)
}

func (db *KV) Close(){
	for _, chunk := range db.mmap.chunks{
		syscall.Munmap(chunk)
		
	}
	_ = db.fp.Close()
}

func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return db.flushPages()
}

// func (db* KV) Del(key []byte)(bool, error){
// 	deleted := db.tree.Delete(key)
// 	return deleted, db.flushPages()
// }

func (db* KV) flushPages() error{
	if err:= db.writePages(); err != nil {
		return err
	}
	db.syncPages()
	return nil
}

func (db* KV) writePages() error {
	npages := int(db.page.flushed) + len(db.page.temp)
	if err := db.extendFile(npages); err != nil {
		return err
	}
	if err := 	extendMmap(db, npages); err != nil {
		return err
	}
	for i, page := range db.page.temp{
		ptr := db.page.flushed + uint64(i)
		copy(db.pageGet(ptr).GetAllData(), page)
	}
	return nil
}

func (db *KV) syncPages() error {
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}