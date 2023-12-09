package b_tree

import (
	"data_structures/b_node"
)

type BTree struct{
	root uint64
	get func(uint64) BNode
	new func(BNode) uint64
	del func(uint64)
}

