package b_tree

import (
	"github.com/saikumar1752/MyDB/data_structures"
)

type BTree struct{
	root uint64
	get func(uint64) data_structures.BNode
	new func(data_structures.BNode) uint64
	del func(uint64)
}

func (tree *BTree) Delete(key [] byte) bool {
	if tree.root == 0{
		return false
	}
	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.GetData()) == 0{
		return false
	}
	tree.del(tree.root)
	if updated.Btype()== data_structures.BNODE_NODE && updated.Nkeys()==1{
		ptr, _ := updated.GetPtr(0)
		tree.root = ptr

	} else {
		tree.root = tree.new(updated)
	}
	return true
}

func (tree *BTree) Insert(key [] byte, val [] byte) {
	if tree.root == 0{
		var root data_structures.BNode
		root.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
		root.SetHeader(data_structures.BNODE_LEAF, 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 0, 0, key, val)
		tree.root = tree.new(root)
		return
	}
	node := tree.get(tree.root)
	tree.del(tree.root)
	node = treeInsert(tree, node, key, val)
	nsplit, splitted := nodeSplit3(node)
	if nsplit >1 {
		var root data_structures.BNode
		root.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
		root.SetHeader(data_structures.BNODE_LEAF, nsplit)
		for i, knode := range splitted[:nsplit]{
			key, _ := knode.GetKey(0)
			ptr := tree.new(knode)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
	}
	
}