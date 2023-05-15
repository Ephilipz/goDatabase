package main

import (
	"bytes"
	"encoding/binary"
)

// each node is an array of bytes. The array is split up as follows:
// the type is node or leaf.
// nkeys is the number of keys.
// pointers are to child nodes.
// offsets point to the position of KV pairs relative to the first KV pair (first offset is always 0)
//
//	| type	| nkeys | pointers 		| offsets 		| key-values
//	| 2B 	| 2B 	| nkeys * 8B 	| nkeys * 2B 	| ...
//
// each KV pair has the following format
// | klen 	| vlen 	| key | val |
// | 2B		| 2B 	| ... | ... |
type BNode struct {
	data []byte
}

// header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data[0:2])
}
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}
func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset
func (node BNode) offsetPos(idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := node.offsetPos(idx)
	return binary.LittleEndian.Uint16(node.data[pos:])
}
func (node BNode) setOffset(idx uint16, offset uint16) {
	pos := node.offsetPos(idx)
	binary.LittleEndian.PutUint16(node.data[pos:], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}
func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}
func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
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
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx)
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
