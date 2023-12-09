package data_structures

import (
	"bytes"
	"encoding/binary"
	"errors"
	""
)

type BNode struct {
	data []byte
}

// header
func (node *BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node *BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node *BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node *BNode) getPtr(idx uint16) (uint64, error) {
	if idx >= node.nkeys() {
		return 0, errors.New("Invalid pointer index.")
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:]), nil
}

func (node *BNode) setPtr(idx uint16, val uint64) error {
	if idx >= node.nkeys() {
		return errors.New("Invalid pointer index.")
	}
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
	return nil
}

//offset
func (node *BNode) offsetPos(idx uint16) (uint16, error) {
	if idx == 0 || idx >= node.nkeys() {
		return 0, errors.New("Invalid idx value")
	}
	return HEADER + 8*node.nkeys() + 2*(idx-1), nil
}

func (node *BNode) getOffset(idx uint16) (uint16, error) {
	if idx == 0 {
		return 0, nil
	}
	offset, err := node.offsetPos(idx)
	if err != nil {
		return 0, errors.New("Invalid idx value")
	}
	return binary.LittleEndian.Uint16(node.data[offset:]), nil
}

func (node *BNode) setOffset(idx uint16, offset uint16) error {
	offset, err := node.offsetPos(idx)
	if err != nil {
		return errors.New("Invalid idx value")
	}
	binary.LittleEndian.PutUint16(node.data[offset:], offset)
	return nil
}

func (node *BNode) kvPos(idx uint16) (uint16, error) {
	if idx > node.nkeys() {
		return 0, errors.New("Invalid idx value")
	}
	offset, _ := node.getOffset(idx)
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + offset, nil
}

func (node *BNode) getKey(idx uint16) ([]byte, error) {
	if idx >= node.nkeys() {
		return make([]byte, 0), errors.New("Invalid idx value")
	}
	pos, _ := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen], nil
}

func (node *BNode) getVal(idx uint16) ([]byte, error) {
	if idx >= node.nkeys() {
		return make([]byte, 0), errors.New("Invalid idx value")
	}
	pos, _ := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen], nil
}

func (node *BNode) nbytes() uint16 {
	bytes, _ := node.kvPos(node.nkeys())
	return bytes
}

func (node *BNode) nodeLookupLE(key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	for i := uint16(1); i < nkeys; i++ {
		_key, _ := node.getKey(i)
		cmp := bytes.Compare(_key, key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}
