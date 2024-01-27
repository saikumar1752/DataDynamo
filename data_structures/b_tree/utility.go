package b_tree

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/saikumar1752/MyDB/data_structures"
)

func leafInsert(new_node data_structures.BNode, old_node data_structures.BNode, idx uint16, key []byte, val []byte) {
	new_node.SetHeader(data_structures.BNODE_LEAF, old_node.Nkeys()+1)
	nodeAppendRange(new_node, old_node, 0, 0, idx)
	nodeAppendKV(new_node, idx, 0, key, val)
	store := old_node.Nkeys()
	fmt.Println(store)
	nodeAppendRange(new_node, old_node, idx+1, idx, old_node.Nkeys()-idx)
}

func leafUpdate(new_node data_structures.BNode, old_node data_structures.BNode, idx uint16, key []byte, val []byte) {
	new_node.SetHeader(data_structures.BNODE_LEAF, old_node.Nkeys())
	nodeAppendRange(new_node, old_node, 0, 0, idx-1)
	nodeAppendKV(new_node, idx, 0, key, val)
	
	nodeAppendRange(new_node, old_node, idx+1, idx+1, old_node.Nkeys()-idx)
}

func nodeAppendRange(new_node data_structures.BNode, old_node data_structures.BNode, dstNew uint16, srcOld uint16, n uint16) {
	if n == 0 {
		return
	}
	// Set pointers
	for i := uint16(0); i < n; i++ {
		ptr, _ := old_node.GetPtr(srcOld + i)
		new_node.SetPtr(dstNew+i, ptr)
	}
	// Set offsets
	dstBegin, _ := new_node.GetOffset(dstNew)
	srcBegin, _ := old_node.GetOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		old_offset, _ := old_node.GetOffset(srcOld + i)
		offset := dstBegin + old_offset - srcBegin
		new_node.SetOffset(dstNew+i, offset)
	}

	// Set (key, value) pairs
	old_begin, _ := old_node.KVPos(srcOld)
	old_end, _ := old_node.KVPos(srcOld + n)
	ptr, _ := new_node.KVPos(dstNew)
	copy(new_node.Data[ptr:], old_node.Data[old_begin:old_end])


	
}

func nodeAppendKV(new_node data_structures.BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	new_node.SetPtr(idx, ptr)
	pos, _ := new_node.KVPos(idx)
	binary.LittleEndian.PutUint16(new_node.Data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new_node.Data[pos+2:], uint16(len(val)))
	copy(new_node.Data[pos+4:], key)
	copy(new_node.Data[pos+4+uint16(len(key)):], val)
	next_offset, _ := new_node.GetOffset(idx)
	fmt.Println(next_offset+4+uint16((len(key)+len(val))))
	new_node.SetOffset(idx+1, next_offset+4+uint16((len(key)+len(val))))
}

func treeInsert(tree *BTree, node data_structures.BNode, key []byte, val []byte) data_structures.BNode {
	var new_node data_structures.BNode
	new_node.Initialize(make([]byte, 2*data_structures.BTREE_PAGE_SIZE))
	idx := node.NodeLookupLE(key)

	switch node.Btype() {
	case data_structures.BNODE_LEAF:
		current_key, _ := node.GetKey(idx)
		if bytes.Equal(key, current_key) {
			leafUpdate(new_node, node, idx, key, val) // TODO: Check the logic once again.
		} else {
			leafInsert(new_node, node, idx+1, key, val)
		}
	case data_structures.BNODE_NODE:
		nodeInsert(tree, new_node, node, idx, key, val)
	default:
		panic("Bad node!")
	}
	return new_node
}

func nodeInsert(tree *BTree, new_node data_structures.BNode, node data_structures.BNode, idx uint16, key []byte, val []byte) {
	kptr, _ := node.GetPtr(idx)
	knode := tree.Get(kptr)
	tree.Del(kptr)

	knode = treeInsert(tree, knode, key, val)
	nsplit, splited := nodeSplit3(knode)
	nodeReplaceKidN(tree, new_node, node, idx, splited[:nsplit]...)


}

func nodeSplit2(left data_structures.BNode, right data_structures.BNode, old_node data_structures.BNode) {
	var lim uint16
	if old_node.Nbytes() >= uint16(2* data_structures.BTREE_PAGE_SIZE) {
		lim = 2*data_structures.BTREE_PAGE_SIZE
	} else{
		lim = data_structures.BTREE_PAGE_SIZE
	}
	var num_kv uint16 = 1
	for i := uint16(1); ;i++{
		sz, _ := old_node.KVPos(i)
		if (sz>= lim) {
			break
		}
		num_kv=i
	}

	nodeAppendRange(left, old_node, 0, 0, num_kv)
	nodeAppendRange(right, old_node, 0, num_kv, old_node.Nkeys()-num_kv)
}

func nodeSplit3(old_node data_structures.BNode)(uint16, [3]data_structures.BNode){
	if old_node.Nbytes() <= data_structures.BTREE_PAGE_SIZE{
		return 1, [3]data_structures.BNode{old_node}
	}
	var left, right data_structures.BNode
	left.Initialize(make([]byte, 2*data_structures.BTREE_PAGE_SIZE))
	right.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old_node)
	if left.Nbytes() <= data_structures.BTREE_PAGE_SIZE {
		return 2, [3]data_structures.BNode{left, right}
	}

	var leftleft, middle data_structures.BNode
	leftleft.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
	middle.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	return 3, [3]data_structures.BNode{leftleft, middle, right}
}

func nodeReplaceKidN(tree *BTree, new_node data_structures.BNode, old_node data_structures.BNode, idx uint16, kids ...data_structures.BNode){
	inc := uint16(len(kids))
	new_node.SetHeader(data_structures.BNODE_NODE, old_node.Nkeys()+inc-1)
	nodeAppendRange(new_node, old_node, 0, 0, idx)
	for i, node := range kids {
		key, _ := node.GetKey((0))
		nodeAppendKV(new_node, idx+uint16(i), tree.New(node), key, nil)

	}
	nodeAppendRange(new_node, old_node, idx+inc, idx+1, old_node.Nkeys()-(idx+1))
}

func  leafDelete(new_node data_structures.BNode, old_node data_structures.BNode, idx uint16){
	new_node.SetHeader(data_structures.BNODE_LEAF, old_node.Nkeys()-1)
	nodeAppendRange(new_node, old_node, 0, 0, idx)
	nodeAppendRange(new_node, old_node, idx, idx+1, old_node.Nkeys()-(idx+1))
}

func nodeMerge(new_node data_structures.BNode, left data_structures.BNode, right data_structures.BNode){
	new_node.SetHeader(left.Btype(), left.Nkeys()+right.Nkeys())
	nodeAppendRange(new_node, left, 0, 0, left.Nkeys())
	nodeAppendRange(new_node, right, left.Nkeys(), 0, right.Nkeys())
}

func shouldMerge(tree *BTree, node data_structures.BNode, idx uint16, updated_node data_structures.BNode)(int, data_structures.BNode){
	if updated_node.Nkeys()> data_structures.BTREE_PAGE_SIZE/4{
		return 0, data_structures.BNode{}
	}
	if idx > 0{
		node_ptr, _ := node.GetPtr(idx-1)
		sibling := tree.Get(node_ptr)
		merged := sibling.Nbytes()+updated_node.Nbytes()-data_structures.HEADER
		if merged <= data_structures.BTREE_PAGE_SIZE{
			return -1, sibling
		}
	}
	if idx+1<node.Nkeys(){
		node_ptr, _ := node.GetPtr(idx+1)
		sibling := tree.Get(node_ptr)
		merged := sibling.Nbytes()+updated_node.Nbytes()-data_structures.HEADER
		if merged <= data_structures.BTREE_PAGE_SIZE{
			return 1, sibling
		}
	}
	return 0, data_structures.BNode{}
}

func nodeReplace2KidN(new_node data_structures.BNode, old_node data_structures.BNode, idx uint16, ptr uint64, key []byte){
	new_node.SetHeader(data_structures.BNODE_NODE, old_node.Nkeys()-1)
	nodeAppendRange(new_node, old_node, 0, 0, idx-1)
	nodeAppendKV(new_node, idx, ptr, key, nil)
	nodeAppendRange(new_node, old_node, idx+1, idx+1, old_node.Nkeys()-(idx+1)) // TODO Check the condition again
}

func nodeDelete(tree *BTree, node data_structures.BNode, idx uint16, key []byte) data_structures.BNode{
	kptr, _ := node.GetPtr(idx)	
	updated := treeDelete(tree, tree.Get(kptr), key)
	updated_data := updated.GetAllData()
	if len(updated_data) ==0{
		return data_structures.BNode{}
	}
	tree.Del(kptr)
	var new_node data_structures.BNode
	new_node.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir<0: 
		var merged data_structures.BNode
		merged.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		sibling_ptr, _ := node.GetPtr(idx-1)
		tree.Del(sibling_ptr)
		merged_0_idx_key, _ := merged.GetKey(0)
		nodeReplace2KidN(new_node, node, idx, tree.New(merged), merged_0_idx_key)
	case mergeDir>0: 
		var merged data_structures.BNode
		merged.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		sibling_ptr, _ := node.GetPtr(idx+1)
		tree.Del(sibling_ptr)
		merged_0_idx_key, _ := merged.GetKey(0)
		nodeReplace2KidN(new_node, node, idx, tree.New(merged), merged_0_idx_key)
	case mergeDir ==0:
		nodeReplaceKidN(tree, new_node, node, idx, updated)
	}
	return new_node
}

func treeDelete(tree *BTree, node data_structures.BNode, key []byte) data_structures.BNode{
	idx := node.NodeLookupLE(key)
	switch node.Btype(){
	case data_structures.BNODE_LEAF: 
		node_key, _ := node.GetKey(idx)
		if !bytes.Equal(key, node_key){
			return data_structures.BNode{}
		}
		var new_node data_structures.BNode
		new_node.Initialize(make([]byte, data_structures.BTREE_PAGE_SIZE))
		leafDelete(new_node, node, idx)
		return new_node
	case data_structures.BNODE_NODE:  
		return nodeDelete(tree, node, idx, key)
	default:
		panic("Bad node!")
	}
}