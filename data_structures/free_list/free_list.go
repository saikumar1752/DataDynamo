package persistance


type FreeList struct{
	head uint64
	get func(uint64) BNode
	new func(BNode) uint64
	use func(uint64 BNode)
}