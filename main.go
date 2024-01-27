package main

import (
	"fmt"
	"github.com/saikumar1752/MyDB/data_structures"
	"github.com/saikumar1752/MyDB/data_structures/b_tree"
)

func main(){
	var btree b_tree.BTree = b_tree.BTree{KV: make(map[uint64]data_structures.BNode)}
	var key string = "Hello world"
	var val string = "Gotcha!!"
	btree.Insert([]byte(key), []byte(val))
	var key1 string = "sai kumar"
	var val1 string = "Kundlapelli"
	btree.Insert([]byte(key1), []byte(val1))
	fmt.Println(btree)
	
	// if node.Btype() == data_structures.BNODE_LEAF{
	// 	fmt.Println("This is a leaf!!")
	// } else {
	// 	fmt.Println("This is a root!!")
	// }
	// fmt.Println(node.Btype())	
}