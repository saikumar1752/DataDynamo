package data_structures

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type BNode struct {
	data []byte
}

func (node *BNode) Initialize(data []byte) {
	node.data = data
}

// header
func (node *BNode) Btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node *BNode) Nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node *BNode) SetHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node *BNode) GetPtr(idx uint16) (uint64, error) {
	if idx >= node.Nkeys() {
		return 0, errors.New("Invalid pointer index.")
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:]), nil
}

func (node *BNode) SetPtr(idx uint16, val uint64) error {
	if idx >= node.Nkeys() {
		return errors.New("Invalid pointer index.")
	}
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
	return nil
}

//offset
func (node *BNode) offsetPos(idx uint16) (uint16, error) {
	if idx == 0 || idx >= node.Nkeys() {
		return 0, errors.New("Invalid idx value")
	}
	return HEADER + 8*node.Nkeys() + 2*(idx-1), nil
}

func (node *BNode) GetOffset(idx uint16) (uint16, error) {
	if idx == 0 {
		return 0, nil
	}
	offset, err := node.offsetPos(idx)
	if err != nil {
		return 0, errors.New("Invalid idx value")
	}
	return binary.LittleEndian.Uint16(node.data[offset:]), nil
}

func (node *BNode) SetOffset(idx uint16, offset uint16) error {
	offset, err := node.offsetPos(idx)
	if err != nil {
		return errors.New("Invalid idx value")
	}
	binary.LittleEndian.PutUint16(node.data[offset:], offset)
	return nil
}

// (Key, value) pairs
func (node *BNode) KVPos(idx uint16) (uint16, error) {
	if idx > node.Nkeys() {
		return 0, errors.New("Invalid idx value")
	}
	offset, _ := node.GetOffset(idx)
	return HEADER + 8*node.Nkeys() + 2*node.Nkeys() + offset, nil
}

func (node *BNode) GetKey(idx uint16) ([]byte, error) {
	if idx >= node.Nkeys() {
		return make([]byte, 0), errors.New("Invalid idx value")
	}
	pos, _ := node.KVPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen], nil
}

func (node *BNode) getVal(idx uint16) ([]byte, error) {
	if idx >= node.Nkeys() {
		return make([]byte, 0), errors.New("Invalid idx value")
	}
	pos, _ := node.KVPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen], nil
}

func (node *BNode) Nbytes() uint16 {
	bytes, _ := node.KVPos(node.Nkeys())
	return bytes
}

// Data
func (node *BNode) CopyData(idx uint16, data []byte){
	copy(node.data[idx:], data)
}

func (node *BNode) GetData(begin uint16, end uint16) []byte {
	return node.data[begin:end]
}

func (node *BNode) GetAllData() []byte {
	return node.data
}



func (node *BNode) NodeLookupLE(key []byte) uint16 {
	nkeys := node.Nkeys()
	found := uint16(0)
	for i := uint16(1); i < nkeys; i++ {
		_key, _ := node.GetKey(i)
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

