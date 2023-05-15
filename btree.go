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

// returns the first kid node whose range intersects with the key
// TODO: bisect
func nodeLookupLeaf(parent BNode, key []byte) uint16 {
	var result uint16 = 0
	// the first key is skipped since it's a copy from the parent node
	for i := uint16(1); i < parent.nkeys(); i++ {
		cmp := bytes.Compare(parent.getKey(i), key)
		// keys[i] is smaller than or equal to key
		if cmp <= 0 {
			result = i
		}
		// keys[i] is larger than or equal to key
		if cmp >= 0 {
			break
		}
	}
	return result
}

// add a new KV pair to a leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// update a KV pair in a leaf node
func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// copy multiple KVs into the position
func nodeAppendRange(new BNode, old BNode, newPos uint16, oldPos uint16, count uint16) {
	assert(oldPos+count <= old.nkeys())
	assert(newPos+count <= new.nkeys())
	assert(count > 0)

	// copy pointers
	for i := uint16(0); i < count; i++ {
		new.setPtr(newPos+i, old.getPtr(oldPos+i))
	}

	// copy offsets
	oldOffsetBegin := old.getOffset(oldPos)
	newOffsetBegin := new.getOffset(newPos)
	// range is 1 to n (inclusive) for offsets
	for i := uint16(1); i <= count; i++ {
		oldOffset := old.getOffset(oldPos+i) - oldOffsetBegin
		new.setOffset(newPos+i, newOffsetBegin+oldOffset)
	}

	// copy KV pairs
	oldKVBegin := old.kvPos(oldPos)
	oldKVEnd := old.kvPos(oldPos + count)
	copy(new.data[new.kvPos(newPos):], old.data[oldKVBegin:oldKVEnd])
}

// copy KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// pointers
	new.setPtr(idx, ptr)

	// KVs
	pos := new.kvPos(idx)
	klen := uint16(len(key))
	vlen := uint16(len(val))
	binary.LittleEndian.PutUint16(new.data[pos:], klen)
	binary.LittleEndian.PutUint16(new.data[pos+2:], vlen)
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+klen:], val)

	// set offset of the next key to the end of the newly inserted key
	currentOffset := new.getOffset(idx)
	new.setOffset(idx+1, currentOffset+4+klen+vlen)
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
	assert(node.nkeys() >= 2)
	nodeKeys := node.nkeys()
	nodeBytes := node.nbytes()
	// original guess for left size
	leftKeys := nodeKeys / 2
	leftBytes := func() uint16 {
		return HEADER + 8*leftKeys + 2*leftKeys + node.getOffset(leftKeys)
	}
	// try to fit left in one page
	for leftBytes() > BTREE_PAGE_SIZE {
		leftKeys--
	}

	// try to fit right in one page
	rightBytes := func() uint16 {
		return HEADER + nodeBytes - leftBytes()
	}
	for rightBytes() > BTREE_PAGE_SIZE {
		leftKeys++
	}
	rightKeys := nodeKeys - leftKeys

	// split
	left.setHeader(BNODE_LEAF, leftKeys)
	right.setHeader(BNODE_LEAF, rightKeys)
	nodeAppendRange(left, node, 0, 0, leftKeys)
	nodeAppendRange(right, node, 0, leftKeys, rightKeys)
	assert(right.nbytes() <= BTREE_PAGE_SIZE)
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
