package b_node

import (
	"bytes"
	"encoding/binary"
	"github.com/saikumar1752/MyDB/data_structures"
)

func NodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.Nkeys()
	found := uint16(0)

	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.GetKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

func LeafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.SetHeader(data_structures.BNODE_LEAF, uint16(old.Nkeys()+uint16(1)))
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendKV(new, idx, 0, key, val)
	NodeAppendRange(new, old, idx+1, idx, old.Nkeys()-idx)
}

func LeafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.SetHeader(data_structures.BNODE_LEAF, old.Nkeys())
	if idx > 0 {
		NodeAppendRange(new, old, 0, 0, idx)
	}
	NodeAppendKV(new, idx, 0, key, val)
	NodeAppendRange(new, old, idx+1, idx+1, old.Nkeys()-idx-1)
}

func NodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	if n <= uint16(0) {
		return
	}

	for i := uint16(0); i < n; i++ {
		new.SetPtr(dstNew+i, old.GetPtr(srcOld+i))
	}

	dstBegin := new.GetOffset(dstNew)
	srcBegin := old.GetOffset(srcOld)

	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + old.GetOffset(srcOld+i) - srcBegin
		new.SetOffset(dstNew+i, offset)
	}
	begin := old.KvPos(srcOld)
	end := old.KvPos(srcOld + n)
	copy(new.data[new.KvPos(dstNew):], old.data[begin:end])
}

func NodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) BNode {
	new.SetPtr(idx, ptr)
	pos := new.KvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	new.SetOffset(idx+1, new.GetOffset(idx)+4+uint16((len(key)+len(val))))
	return new
}

func LeafDelete(new BNode, old BNode, idx uint16) {
	new.SetHeader(data_structures.BNODE_LEAF, old.Nkeys()-1)
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendRange(new, old, idx, idx+1, old.Nkeys()-(idx+1))
}

func NodeMerge(new BNode, left BNode, right BNode){
	new.SetHeader(left.Btype(), left.Nkeys()+right.Nkeys())
	NodeAppendRange(new, left, 0, 0, left.Nkeys())
	NodeAppendRange(new, right, left.Nkeys(), 0, right.Nkeys())
}