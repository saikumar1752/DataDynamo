package persistance

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/saikumar1752/MyDB/data_structures"
)

const DB_SIG="BuildYourOwnDB05"

func masterLoad(db *KV) error {
	if db.mmap.file == 0{
		db.page.flushed =1
		return nil
	}
	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])
	if !bytes.Equal([]byte(DB_SIG), data[:16]){
		return errors.New("Bad Signature.")
	}
	bad := !(1<=used && used <= uint64(db.mmap.file/data_structures.BTREE_PAGE_SIZE))
	bad = bad || !(0 <= root && root<used)
	if bad{
		return errors.New("Bad master page.")
	}
	db.tree.Root = root
	db.page.flushed = used
	return nil
}


func masterStore(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.Root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("Write master page: %w", err)
	}
	return nil
}