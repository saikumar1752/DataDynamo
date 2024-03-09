package persistance

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/saikumar1752/MyDB/data_structures"
)

func mmapInit(fp *os.File)(int, []byte, error){
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("start : %w", err)
	}

	if fi.Size()%data_structures.BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("File size is not a multiple of page size.")
	}

	mmapSize := 64 << 20
	for mmapSize < int(fi.Size()){
		mmapSize *= 2
	}
	chunk, err := syscall.Mmap(
		int(fp.Fd()), 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("Mmap : %w", err)
	}

	return int(fi.Size()), chunk, nil
}