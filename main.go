package main

import (
	"fmt"
	"math/rand"
	"unsafe"

	// "github.com/saikumar1752/MyDB/data_structures"
	"github.com/saikumar1752/MyDB/data_structures/b_node"
	"github.com/saikumar1752/MyDB/data_structures/b_tree"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

type C struct {
	tree  b_tree.BTree
	ref   map[string]string
	pages map[uint64]b_node.BNode
}

func newC(pages map[uint64]b_node.BNode) *C {
	return &C{
		tree: b_tree.BTree{
			Get: func(ptr uint64) b_node.BNode {
				node := pages[ptr]
				return node
			},
			New: func(node b_node.BNode) uint64 {
				key := uint64(uintptr(unsafe.Pointer(&node.GetData()[0])))
				pages[key] = node
				return key
			},
			Del: func(ptr uint64) {
				delete(pages, ptr)
			},
		},
	}
}

func main() {
	pages := map[uint64]b_node.BNode{}
	c := newC(pages)
	store := []string{}
	for i := 0; i < 1000; i++ {
		key := RandomString((20))
		store = append(store, key)
		c.tree.InsertKey([]byte(key), []byte(RandomString(30)))
	}
	fmt.Println("Total keys before deleting", c.tree.TotalKeys())
	fmt.Println("Total nodes before deleing", c.tree.TotalNodes())
	deleted_cnt := 0
	non_deleted_cnt := 0
	for idx := range store {
		if idx%2 == 0 {
			c.tree.Delete([]byte(store[idx]))
		}
	}
	for idx := range store {
		if idx%2 == 0 {
			_, ok := c.tree.SearchKey([]byte(store[idx]))
			if !ok {
				deleted_cnt++
			}
		}
	}
	fmt.Println("Total keys", c.tree.TotalKeys())
	fmt.Println("total nodes", c.tree.TotalNodes())
	fmt.Println("Deleted keys", deleted_cnt)
	fmt.Println("Non deleted nodes", non_deleted_cnt)
	// var node_1 b_node.BNode
	// node_1.Initialize()
	// fmt.Println("Size and keys", node_1.Nbytes(), node_1.Nkeys())
	// node_1.SetHeader(data_structures.BNODE_LEAF, 3)
	// b_node.NodeAppendKV(node_1, 0, 0, []byte("hello"), []byte("world"))
	// fmt.Println("Size and keys", node_1.Nbytes(), node_1.Nkeys(), node_1.GetOffset(1))

	// fmt.Println("Size and keys", node_1.GetOffset(1), node_1.Nbytes(), node_1.Nkeys())

	// b_node.NodeAppendKV(node_1, 1, 0, []byte("hello"), []byte("world"))
	// fmt.Println("Gotchar Ofaaaafset values", node_1.GetOffset(0), node_1.GetOffset(1), node_1.Nkeys(), node_1.OffsetPos(1))
	// fmt.Println("Ofaaaafset values", node_1.GetOffset(0), node_1.GetOffset(1), node_1.Nkeys(), node_1.OffsetPos(1))

	// fmt.Println(node_1.Nbytes(), node_1.Nkeys())

	// b_node.NodeAppendKV(node_1, 2, 0, []byte("hello"), []byte("world"))

	// fmt.Println("gotcha")
}
