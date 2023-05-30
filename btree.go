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

// Delete deletes the key from the tree. Returns false if the key was not found
func (tree *BTree) Delete(key []byte) bool {
	assert(len(key) > 0)
	assert(len(key) <= BTREE_MAX_KEY_SIZE)
	if tree.root == 0 {
		return false
	}
	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false // not found
	}
	tree.del(tree.root)
	// deleted from internal node and only one key is left
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		// remove a level (updated will be removed)
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}

func (tree *BTree) Insert(key []byte, val []byte) {
	assert(len(key) > 0)
	assert(len(key) <= BTREE_MAX_KEY_SIZE)
	assert(len(val) <= BTREE_MAX_VAL_SIZE)
	if tree.root == 0 {
		// create the first node
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		// dummy key since the first key is a copy from the parent but this has none
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}
	root := tree.get(tree.root)
	tree.del(tree.root)
	updatedRoot := treeInsert(tree, root, key, val)
	nsplit, splitted := nodeSplit3(updatedRoot)
	if nsplit > 1 {
		// the root was split. Add a new level
		newRoot := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		newRoot.setHeader(BNODE_NODE, nsplit)
		// add kid nodes as pointers to root
		for i, kid := range splitted[:nsplit] {
			ptr, kidKey := tree.new(kid), kid.getKey(0)
			nodeAppendKV(newRoot, uint16(i), ptr, kidKey, nil)
		}
		tree.root = tree.new(newRoot)
	} else {
		tree.root = tree.new(splitted[0])
	}
}

// returns the index of the first kid node whose range intersects with the key
// todo: bisect
func nodeLookup(node BNode, key []byte) uint16 {
	var result uint16
	// the first key is skipped since it's a copy from the parent node
	for i := uint16(1); i < node.nkeys(); i++ {
		cmp := bytes.Compare(node.getKey(i), key)
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

// delete a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

// copy multiple KVs into the position
func nodeAppendRange(new BNode, old BNode, newPos uint16, oldPos uint16, count uint16) {
	assert(oldPos+count <= old.nkeys())
	assert(newPos+count <= new.nkeys())
	if count == 0 {
		return
	}
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
	idx := nodeLookup(node, key)
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
		// internal node, insert it to a kid node recursively
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
	nodeAddKidLinks(tree, new, node, idx, splitted[:nsplit]...)
}

// updates the parent node pointers to include the new kid nodes
func nodeAddKidLinks(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	nodesCnt := uint16(len(kids))
	if nodesCnt == 1 && bytes.Equal(kids[0].getKey(0), old.getKey(idx)) {
		nodeReplace1Ptr(new, old, idx, tree.new(kids[0]))
		return
	}
	new.setHeader(BNODE_NODE, old.nkeys()+nodesCnt-1)
	// append old pointers until idx
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		// add pointer of the kids[i] to the new node
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	// append old pointers after idx
	nodeAppendRange(new, old, idx+nodesCnt, idx+1, old.nkeys()-(idx+1))
}

// replaces a link to a kid with the same key but a different pointer
func nodeReplace1Ptr(new BNode, old BNode, idx uint16, ptr uint64) {
	// copy all data from old to new
	copy(new.data, old.data[:old.nbytes()])
	// only the pointer at idx changes
	new.setPtr(idx, ptr)
}

// merges 2 child pointers into 1. This is called after a merge occurs
func nodeMerge2Links(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	// append pointers until idx
	nodeAppendRange(new, old, 0, 0, idx)
	// append current pointer at idx
	nodeAppendKV(new, idx, ptr, key, nil)
	// append all pointers after idx+1 (skip one pointer)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
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
	left.setHeader(node.btype(), leftKeys)
	right.setHeader(node.btype(), rightKeys)
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

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// where to find the key?
	idx := nodeLookup(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}
		}
		// delete the key in the leaf
		updated := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(updated, node, idx)
		return updated
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("bad node")
	}
}

// deletes an internal node from the tree recursively
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse until you get a leaf node
	kidPtr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kidPtr), key)
	if len(updated.data) == 0 {
		return BNode{} // not found
	}
	tree.del(kidPtr)

	newNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	mergeDirection, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDirection < 0: // left
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeMerge2Links(newNode, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDirection > 0: // right
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeMerge2Links(newNode, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDirection == 0:
		assert(updated.nkeys() > 0)
		nodeAddKidLinks(tree, newNode, node, idx, updated)
	}
	return newNode
}

// merges 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

// determines if the nodes should merge and the direction of that merge
func shouldMerge(tree *BTree, old BNode, idx uint16, new BNode) (int, BNode) {
	if new.nbytes() > BTREE_MIN_SIZE {
		return 0, BNode{} // no merge needed
	}
	// merge to the left
	if idx > 0 {
		sibling := tree.get(old.getPtr(idx - 1))
		mergedBytes := sibling.nbytes() + new.nbytes() - HEADER
		if mergedBytes <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	// merge to the right
	if idx+1 < old.nkeys() {
		sibling := tree.get(old.getPtr(idx + 1))
		mergedBytes := sibling.nbytes() + new.nbytes() - HEADER
		if mergedBytes <= BTREE_PAGE_SIZE {
			return 1, sibling
		}
	}
	return 0, BNode{}
}
