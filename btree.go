package main

import (
	"bytes"
	"encoding/binary"
)

type BTree struct {
	root uint64 // pointer to a nonzero page number

	// callbacks for on-disk pages
	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1max <= BTREE_PAGE_SIZE)
}

// insert a KV into a node, the result might be split into 2 nodes
// the caller is responsible for deallocating the input node and splitting and allocating result nodes
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node. It's allowed to be larger than 1 page, if so it will be split
	newNode := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	// where to insert the key?
	idx := nodeLookupLeaf(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it
			leafUpdate(newNode, node, idx, key, val)
		} else {
			// key not found, insert it
			leafInsert(newNode, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node
		nodeInsert(tree, newNode, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return newNode
}

// KV insertion into an internal node
func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	// get and deallocate the kid node
	kidPtr := node.getPtr(idx)
	kidNode := tree.get(kidPtr)
	tree.del(kidPtr)

	// recursive insertion to the kid node
	kidNode = treeInsert(tree, kidNode, key, val)
	// split the result
	nsplit, splitted := nodeSplit3(kidNode)
	// update the kid links
	updateSplitNodesLinks(tree, new, node, idx, splitted[:nsplit]...)
}

// updates the node pointers to include the new kid nodes
func updateSplitNodesLinks(tree *BTree, new BNode, old BNode, idx uint16, nodes ...BNode) {
	nodesCnt := uint16(len(nodes))
	new.setHeader(BNODE_NODE, old.nkeys()+nodesCnt-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range nodes {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+nodesCnt, idx+1, old.nkeys()-(idx+1))
}

// split a node into two nodes. Left may still be larger than the max page size
func nodeSplit2(left BNode, right BNode, node BNode) {
	// left will have a minimum of nkeys / 2. If nkeys is odd, left should have more elements
	leftNKeys := (node.nkeys() + 1) / 2
	left.setHeader(BNODE_LEAF, leftNKeys)
	nodeAppendRange(left, node, 0, 0, leftNKeys)

	// append until remaining bytes can fit in one page
	for node.nbytes()-left.nbytes() <= BTREE_PAGE_SIZE {
		// increase left node key size by 1
		leftNKeys++
		binary.LittleEndian.PutUint16(left.data[2:4], leftNKeys)
		nodeAppendRange(left, node, leftNKeys-1, leftNKeys-1, 1)
	}
	nodeAppendRange(right, node, 0, leftNKeys, node.nkeys()-leftNKeys)
}

// split a node if it's too big. the results are 1~3 nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	// fits in 1 page, don't split
	if old.nbytes() <= BTREE_PAGE_SIZE {
		return 1, [3]BNode{old}
	}

	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)} // might be split later
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)

	// fits in 2 pages
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	// left is still too large
	leftLeft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	leftRight := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftLeft, leftRight, left)
	assert(leftLeft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftLeft, leftRight, right}
}
