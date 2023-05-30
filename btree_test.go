package main

import (
	"bytes"
	"fmt"
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
		refKeys[i] = k
		refVals[i] = v
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
	// verify that the keys and values of the tree equal the sorted keys and values
	assertArrEqual(refKeys, keys)
	assertArrEqual(refVals, vals)
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
			nodeVerify(kid)
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

// generates a long shuffled number from h
func fmix(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

func TestBTreeCRUD(t *testing.T) {
	c := NewC()

	// insert
	for i := uint32(0); i < 250000; i++ {
		key := fmt.Sprintf("key%d", fmix(i))
		val := fmt.Sprintf("vvv%d", fmix(-i))
		c.add(key, val)
	}
	c.verify(t)

	// delete
	assert(!c.del("kk")) // delete should return false for a non-existent key
	for i := uint32(2000); i < 250000; i++ {
		key := fmt.Sprintf("key%d", fmix(i))
		assert(c.del(key))
	}
	c.verify(t)

	// update
	for i := uint32(0); i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix(i))
		val := fmt.Sprintf("vvv%d", fmix(i))
		c.add(key, val)
	}
	c.verify(t)

	// delete remaining
	for i := uint32(0); i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix(i))
		c.del(key)
	}
	c.add("k", "v")
	c.verify(t)
	assert(len(c.pages) == 1)
}
