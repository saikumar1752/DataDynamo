package b_tree

import (
	"bytes"
	"github.com/saikumar1752/MyDB/data_structures"
	"github.com/saikumar1752/MyDB/data_structures/b_node"
)

type BTree struct {
	root uint64
	Get  func(uint64) b_node.BNode
	New  func(b_node.BNode) uint64
	Del  func(uint64)
}

func (tree *BTree) TreeInsert(node b_node.BNode, key []byte, val []byte) b_node.BNode {
	var new b_node.BNode
	new.InitializeWithSize(2 * data_structures.BTREE_PAGE_SIZE)

	idx := b_node.NodeLookupLE(node, key)

	switch node.Btype() {
	case data_structures.BNODE_LEAF:
		if bytes.Equal(key, node.GetKey(idx)) {
			b_node.LeafUpdate(new, node, idx, key, val)
		} else {
			b_node.LeafInsert(new, node, idx+1, key, val)
		}
	case data_structures.BNODE_NODE:
		tree.nodeInsert(new, node, idx, key, val)
	}
	return new
}

func (tree *BTree) nodeReplaceKidN(new b_node.BNode, old b_node.BNode, idx uint16, kids ...b_node.BNode) {
	inc := uint16(len(kids))
	new.SetHeader(data_structures.BNODE_NODE, old.Nkeys()+inc-1)
	b_node.NodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		b_node.NodeAppendKV(new, idx+uint16(i), tree.New(node), node.GetKey(0), nil)
	}
	b_node.NodeAppendRange(new, old, idx+inc, idx+1, old.Nkeys()-(idx+1))
}

func (tree *BTree) nodeSplit2(left b_node.BNode, right b_node.BNode, old b_node.BNode, sz_limit int) {
	var sz uint16 = 4
	var key_count uint16 = 0
	for i := uint16(0); i < old.Nkeys(); i++ {
		key := old.GetKey(i)
		val := old.GetVal(i)
		if sz+uint16(len(key))+uint16(len(val))+14 >= data_structures.BTREE_PAGE_SIZE {
			break
		}
		sz = sz + uint16(len(key)) + uint16(len(val)) + 14
		key_count++
	}
	var idx uint16 = 0
	left.SetHeader(old.Btype(), key_count)
	for i := uint16(0); i < key_count; idx, i = idx+1, i+1 {
		b_node.NodeAppendKV(left, i, old.GetPtr(idx), old.GetKey(idx), old.GetVal(idx))
	}
	right.SetHeader(old.Btype(), old.Nkeys()-key_count)
	for i := uint16(0); idx < old.Nkeys(); idx, i = idx+1, i+1 {
		b_node.NodeAppendKV(right, i, old.GetPtr(idx), old.GetKey(idx), old.GetVal(idx))
	}
}

func (tree *BTree) nodeSplit(old b_node.BNode) (uint16, [3]b_node.BNode) {
	if old.Nbytes() <= data_structures.BTREE_PAGE_SIZE {
		return 1, [3]b_node.BNode{old}
	}
	var left, right b_node.BNode
	left.Initialize()
	right.Initialize()
	tree.nodeSplit2(left, right, old, data_structures.BTREE_PAGE_SIZE)
	return 2, [3]b_node.BNode{left, right}
}

func (tree *BTree) nodeInsert(new b_node.BNode, node b_node.BNode, idx uint16, key []byte, val []byte) {
	kptr := node.GetPtr(idx)
	knode := tree.Get(kptr)

	tree.Del(kptr)

	knode = tree.TreeInsert(knode, key, val)
	nsplit, splited := tree.nodeSplit(knode)
	tree.nodeReplaceKidN(new, node, idx, splited[:nsplit]...)
}

func (tree *BTree) InsertKey(key []byte, val []byte) {
	if tree.root == 0 {
		var root b_node.BNode
		root.Initialize()
		root.SetHeader(data_structures.BNODE_LEAF, 2)
		b_node.NodeAppendKV(root, 0, 0, nil, nil)
		b_node.NodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.New(root)
		return
	}
	node := tree.Get(tree.root)
	tree.Del(tree.root)

	node = tree.TreeInsert(node, key, val)
	nsplit, splitted := tree.nodeSplit(node)

	if nsplit > 1 {
		var root b_node.BNode
		root.Initialize()
		root.SetHeader(data_structures.BNODE_NODE, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.New(knode), knode.GetKey(0)
			b_node.NodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.New(root)
	} else {
		tree.root = tree.New(splitted[0])
	}
}

func (tree *BTree) search(node b_node.BNode, key []byte) ([]byte, bool) {
	idx := b_node.NodeLookupLE(node, key)
	switch node.Btype() {
	case data_structures.BNODE_LEAF:
		if !bytes.Equal(key, node.GetKey(idx)) {
			return []byte{}, false
		} else {
			return node.GetVal(idx), true
		}
	case data_structures.BNODE_NODE:
		kptr := node.GetPtr(idx)
		child_node := tree.Get(kptr)
		return tree.search(child_node, key)
	}
	return []byte{}, false
}

func (tree *BTree) SearchKey(key []byte) ([]byte, bool) {
	return tree.search(tree.Get(tree.root), key)
}
