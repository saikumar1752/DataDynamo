package b_tree

import (
	"fmt"
	"github.com/saikumar1752/MyDB/data_structures"
)

// type BTree struct {
// 	Root uint64
// 	Get  func(uint64) data_structures.BNode
// 	New  func(data_structures.BNode) uint64
// 	Del  func(uint64)
// }

// func (tree *BTree) Delete(key [] byte) bool {
// 	if tree.Root == 0{
// 		return false
// 	}
// 	updated := treeDelete(tree, tree.Get(tree.Root), key)
// 	if len(updated.GetData()) == 0{
// 		return false
// 	}
// 	tree.Del(tree.Root)
// 	if updated.Btype()== data_structures.BNODE_NODE && updated.Nkeys()==1{
// 		ptr, _ := updated.GetPtr(0)
// 		tree.Root = ptr

// 	} else {
// 		tree.Root = tree.New(updated)
// 	}
// 	return true
// }

type BTree struct {
	Root uint64
	KV map[uint64]data_structures.BNode
	// Get  func(uint64) data_structures.BNode
	// New  func(data_structures.BNode) uint64
	// Del  func(uint64)
}

func (tree *BTree) Get(ptr uint64) data_structures.BNode{

	if val, ok := tree.KV[ptr]; ok{
		return val
	}
	return data_structures.BNode{}
}

func (tree *BTree) New(b_node data_structures.BNode)uint64{
	var max_key_val uint64 = 0
	for key := range tree.KV{
		if max_key_val < key {
			max_key_val++;
		}
	}
	max_key_val+=1
	tree.KV[max_key_val] = b_node
	return max_key_val
}

func (tree *BTree) Del(ptr uint64){
	delete(tree.KV, ptr)
}

func (tree *BTree) Insert(key []byte, val []byte) {
	fmt.Println("tree.Root", tree.Root==0)
	if tree.Root == 0 {
		var Root data_structures.BNode
		Root.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
		Root.SetHeader(data_structures.BNODE_LEAF, 2)
		nodeAppendKV(Root, 0, 0, nil, nil)
		nodeAppendKV(Root, 0, 0, key, val)
		tree.Root = tree.New(Root)
		return
	}
	node := tree.Get(tree.Root)
	tree.Del(tree.Root)
	node = treeInsert(tree, node, key, val)
	nsplit, splitted := nodeSplit3(node)
	if nsplit > 1 {
		var Root data_structures.BNode
		Root.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
		Root.SetHeader(data_structures.BNODE_LEAF, nsplit)
		for i, knode := range splitted[:nsplit] {
			key, _ := knode.GetKey(0)
			ptr := tree.New(knode)
			nodeAppendKV(Root, uint16(i), ptr, key, nil)
		}
	}

}
