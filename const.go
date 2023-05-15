package main

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

const (
	HEADER             = 4
	BTREE_PAGE_SIZE    = 4096 // page size is 4K bytes
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)
