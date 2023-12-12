package b_tree

import (
	"github.com/saikumar1752/MyDB/data_structures"
)

type BTree struct{
	Root uint64
	get func(uint64) data_structures.BNode
	new func(data_structures.BNode) uint64
	del func(uint64)
}

func (tree *BTree) Delete(key [] byte) bool {
	if tree.Root == 0{
		return false
	}
	updated := treeDelete(tree, tree.get(tree.Root), key)
	if len(updated.GetData()) == 0{
		return false
	}
	tree.del(tree.Root)
	if updated.Btype()== data_structures.BNODE_NODE && updated.Nkeys()==1{
		ptr, _ := updated.GetPtr(0)
		tree.Root = ptr

	} else {
		tree.Root = tree.new(updated)
	}
	return true
}

func (tree *BTree) Insert(key [] byte, val [] byte) {
	if tree.Root == 0{
		var Root data_structures.BNode
		Root.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
		Root.SetHeader(data_structures.BNODE_LEAF, 2)
		nodeAppendKV(Root, 0, 0, nil, nil)
		nodeAppendKV(Root, 0, 0, key, val)
		tree.Root = tree.new(Root)
		return
	}
	node := tree.get(tree.Root)
	tree.del(tree.Root)
	node = treeInsert(tree, node, key, val)
	nsplit, splitted := nodeSplit3(node)
	if nsplit >1 {
		var Root data_structures.BNode
		Root.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
		Root.SetHeader(data_structures.BNODE_LEAF, nsplit)
		for i, knode := range splitted[:nsplit]{
			key, _ := knode.GetKey(0)
			ptr := tree.new(knode)
			nodeAppendKV(Root, uint16(i), ptr, key, nil)
		}
	}
	
}