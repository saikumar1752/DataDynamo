package main

import (
	// "fmt"
	"github.com/saikumar1752/MyDB/data_structures"
	"github.com/saikumar1752/MyDB/data_structures/b_tree"
	"math/rand"
	"time"
)

func RandString(length int) string {
	rand.Seed(time.Now().UnixNano())

	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

func main(){
	var btree b_tree.BTree = b_tree.BTree{KV: make(map[uint64]data_structures.BNode)}
	// for i := 0; i < 1000; i++{
	// 	var key string = RandString(20)
	// 	var val string = RandString(20)
	// 	fmt.Println("Inserting", key, val)
	// 	btree.Insert([]byte(key), []byte(val))
	// }
	btree.Insert([]byte("Hello world"), []byte("asdfasdf"))
	btree.Insert([]byte("Hello wasdforld"), []byte("asasedfdfasdf"))
	// if node.Btype() == data_structures.BNODE_LEAF{
	// 	fmt.Println("This is a leaf!!")
	// } else {
	// 	fmt.Println("This is a root!!")
	// }
	// fmt.Println(node.Btype())	
}