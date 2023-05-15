package main

import (
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
