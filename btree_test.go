package main

import (
	"bytes"
	"sort"
	"testing"
	"unsafe"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

// returns a list of all keys and values in the tree. key[i] corresponds to val[i]
func (c *C) dump() ([]string, []string) {
	keys := []string{}
	vals := []string{}

	var nodeDump func(uint64)
	nodeDump = func(ptr uint64) {
		node := c.tree.get(ptr)
		nkeys := node.nkeys()
		if node.btype() == BNODE_LEAF {
			for i := uint16(0); i < nkeys; i++ {
				keys = append(keys, string(node.getKey(i)))
				vals = append(vals, string(node.getVal(i)))
			}
		} else {
			for i := uint16(0); i < nkeys; i++ {
				ptr := node.getPtr(i)
				nodeDump(ptr)
			}
		}
	}

	nodeDump(c.tree.root)
	assert(keys[0] == "")
	assert(vals[0] == "")
	return keys[1:], vals[1:]
}

// helper for keeping the map that holds kv pairs sorted
type sortIF struct {
	len  int
	less func(i, j int) bool
	swap func(i, j int)
}

func (s sortIF) Len() int {
	return s.len
}
func (s sortIF) Less(i, j int) bool {
	return s.less(i, j)
}
func (s sortIF) Swap(i, j int) {
	s.swap(i, j)
}

// verifies that the tree is correctly sorted
func (c *C) verify(t *testing.T) {
	keys, vals := c.dump()

	refKeys, refVals := make([]string, len(c.ref)), make([]string, len(c.ref))
	i := 0
	for k, v := range c.ref {
		keys[i] = k
		vals[i] = v
		i++
	}
	assert(len(refKeys) == len(refVals))
	// sort keys
	sort.Stable(sortIF{
		len:  len(refKeys),
		less: func(i, j int) bool { return refKeys[i] < refKeys[j] },
		swap: func(i, j int) {
			k, v := refKeys[i], refVals[i]
			refKeys[i], refVals[i] = refKeys[j], refVals[j]
			refKeys[j], refVals[j] = k, v
		},
	})
	assert(refKeys == keys)
	// recursive function to verify internal nodes
	var nodeVerify func(BNode)
	nodeVerify = func(node BNode) {
		nkeys := node.nkeys()
		assert(nkeys > 0)
		if node.btype() == BNODE_LEAF {
			return
		}
		for i := uint16(0); i < nkeys; i++ {
			key := node.getKey(i)
			kid := c.tree.get(node.getPtr(i))
			assert(bytes.Compare(key, kid.getKey(0)) == 0)
			if kid.btype() == BNODE_NODE {
				nodeVerify(kid)
			}
		}
	}
}

func NewC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert(ok)
				return node
			},
			new: func(node BNode) uint64 {
				assert(node.nbytes() <= BTREE_PAGE_SIZE)
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				assert(pages[key].data == nil)
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				assert(ok)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func TestBTreeCRUD(t *testing.T) {
	c := NewC()
	c.add("k", "v")
	c.verify(t)
}
