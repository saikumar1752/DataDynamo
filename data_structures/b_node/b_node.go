package b_node

import (
	"encoding/binary"
	"github.com/saikumar1752/MyDB/data_structures"
)

type BNode struct {
	data []byte
}

func (node *BNode) Initialize() {
	node.data = make([]byte, data_structures.BTREE_PAGE_SIZE)
}

func (node *BNode) InitializeWithSize(size int) {
	node.data = make([]byte, size)
}

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

func (node *BNode) GetPtr(idx uint16) uint64 {
	pos := data_structures.HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node *BNode) SetPtr(idx uint16, val uint64) {
	pos := data_structures.HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

func (node *BNode) OffsetPos(idx uint16) uint16 {
	return data_structures.HEADER + uint16(8)*node.Nkeys() + uint16(2)*(idx-1)
}

func (node *BNode) GetOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[node.OffsetPos(idx):])
}

func (node *BNode) SetOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[node.OffsetPos(idx):], offset)
}

func (node *BNode) KvPos(idx uint16) uint16 {

	return data_structures.HEADER + 8*node.Nkeys() + 2*node.Nkeys() + node.GetOffset(idx)
}

func (node *BNode) GetKey(idx uint16) []byte {
	pos := node.KvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node *BNode) GetVal(idx uint16) []byte {
	pos := node.KvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

func (node *BNode) Nbytes() uint16 {
	return node.KvPos(node.Nkeys())
}

func (node *BNode) GetData() []byte {
	return node.data
}
