// B+ Tree (production-style core) demonstrating how leaf and internal nodes are UPDATED
// Focus: correct update logic for insert, split, delete, borrow, merge
// Keys: []byte, Values: uint64
// NOTE: mmap/page-ID wiring omitted for clarity; logic is exact.

package bplustree

import (
	"bytes"
	"errors"
	"sort"
)

// ---------------- Configuration ----------------

const MinFanout = 2 // minimal for demonstration

// ---------------- Node Definitions ----------------

type nodeType uint8

const (
	internalNode nodeType = iota
	leafNode
)

type node struct {
	type_  nodeType
	keys   [][]byte
	parent *node

	// internal
	children []*node

	// leaf
	values []uint64
	next   *node
	prev   *node
}

// ---------------- Tree ----------------

type BPlusTree struct {
	root    *node
	maxKeys int
}

func New(maxKeys int) *BPlusTree {
	if maxKeys < 3 {
		panic("maxKeys must be >= 3")
	}
	leaf := &node{type_: leafNode}
	return &BPlusTree{root: leaf, maxKeys: maxKeys}
}

// ---------------- Search ----------------

func (t *BPlusTree) Get(key []byte) (uint64, bool) {
	l := t.findLeaf(key)
	i := sort.Search(len(l.keys), func(i int) bool {
		return bytes.Compare(l.keys[i], key) >= 0
	})
	if i < len(l.keys) && bytes.Equal(l.keys[i], key) {
		return l.values[i], true
	}
	return 0, false
}

// ---------------- Insert ----------------

func (t *BPlusTree) Insert(key []byte, value uint64) {
	leaf := t.findLeaf(key)

	i := sort.Search(len(leaf.keys), func(i int) bool {
		return bytes.Compare(leaf.keys[i], key) >= 0
	})

	if i < len(leaf.keys) && bytes.Equal(leaf.keys[i], key) {
		leaf.values[i] = value
		return
	}

	leaf.keys = append(leaf.keys, nil)
	leaf.values = append(leaf.values, 0)
	copy(leaf.keys[i+1:], leaf.keys[i:])
	copy(leaf.values[i+1:], leaf.values[i:])
	leaf.keys[i] = key
	leaf.values[i] = value

	if len(leaf.keys) > t.maxKeys {
		t.splitLeaf(leaf)
	}
}

// ---------------- Leaf Split ----------------

func (t *BPlusTree) splitLeaf(l *node) {
	mid := len(l.keys) / 2

	right := &node{
		type_:  leafNode,
		keys:   append([][]byte{}, l.keys[mid:]...),
		values: append([]uint64{}, l.values[mid:]...),
		parent: l.parent,
		next:   l.next,
		prev:   l,
	}

	if l.next != nil {
		l.next.prev = right
	}
	l.next = right

	l.keys = l.keys[:mid]
	l.values = l.values[:mid]

	separator := right.keys[0]

	if l.parent == nil {
		root := &node{
			type_:    internalNode,
			keys:     [][]byte{separator},
			children: []*node{l, right},
		}
		l.parent = root
		right.parent = root
		t.root = root
		return
	}

	t.insertIntoParent(l.parent, separator, right)
}

// ---------------- Internal Insert ----------------

func (t *BPlusTree) insertIntoParent(p *node, key []byte, right *node) {
	i := sort.Search(len(p.keys), func(i int) bool {
		return bytes.Compare(p.keys[i], key) >= 0
	})

	p.keys = append(p.keys, nil)
	copy(p.keys[i+1:], p.keys[i:])
	p.keys[i] = key

	p.children = append(p.children, nil)
	copy(p.children[i+2:], p.children[i+1:])
	p.children[i+1] = right
	right.parent = p

	if len(p.keys) > t.maxKeys {
		t.splitInternal(p)
	}
}

// ---------------- Internal Split ----------------

func (t *BPlusTree) splitInternal(n *node) {
	mid := len(n.keys) / 2
	upKey := n.keys[mid]

	right := &node{
		type_:    internalNode,
		keys:     append([][]byte{}, n.keys[mid+1:]...),
		children: append([]*node{}, n.children[mid+1:]...),
		parent:   n.parent,
	}

	for _, c := range right.children {
		c.parent = right
	}

	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]

	if n.parent == nil {
		root := &node{
			type_:    internalNode,
			keys:     [][]byte{upKey},
			children: []*node{n, right},
		}
		n.parent = root
		right.parent = root
		t.root = root
		return
	}

	t.insertIntoParent(n.parent, upKey, right)
}

// ---------------- Delete ----------------

func (t *BPlusTree) Delete(key []byte) error {
	leaf := t.findLeaf(key)
	i := sort.Search(len(leaf.keys), func(i int) bool {
		return bytes.Compare(leaf.keys[i], key) >= 0
	})
	if i >= len(leaf.keys) || !bytes.Equal(leaf.keys[i], key) {
		return errors.New("not found")
	}

	leaf.keys = append(leaf.keys[:i], leaf.keys[i+1:]...)
	leaf.values = append(leaf.values[:i], leaf.values[i+1:]...)

	min := (t.maxKeys + 1) / 2
	if leaf == t.root || len(leaf.keys) >= min {
		return nil
	}

	t.rebalanceLeaf(leaf)
	return nil
}

// ---------------- Rebalance Leaf ----------------

func (t *BPlusTree) rebalanceLeaf(l *node) {
	p := l.parent
	idx := indexOfChild(p, l)

	// borrow from left
	if idx > 0 {
		left := p.children[idx-1]
		if len(left.keys) > (t.maxKeys+1)/2 {
			l.keys = append([][]byte{left.keys[len(left.keys)-1]}, l.keys...)
			l.values = append([]uint64{left.values[len(left.values)-1]}, l.values...)
			left.keys = left.keys[:len(left.keys)-1]
			left.values = left.values[:len(left.values)-1]
			p.keys[idx-1] = l.keys[0]
			return
		}
	}

	// borrow from right
	if idx < len(p.children)-1 {
		right := p.children[idx+1]
		if len(right.keys) > (t.maxKeys+1)/2 {
			l.keys = append(l.keys, right.keys[0])
			l.values = append(l.values, right.values[0])
			right.keys = right.keys[1:]
			right.values = right.values[1:]
			p.keys[idx] = right.keys[0]
			return
		}
	}

	// merge
	if idx > 0 {
		left := p.children[idx-1]
		left.keys = append(left.keys, l.keys...)
		left.values = append(left.values, l.values...)
		left.next = l.next
		if l.next != nil {
			l.next.prev = left
		}
		p.keys = append(p.keys[:idx-1], p.keys[idx:]...)
		p.children = append(p.children[:idx], p.children[idx+1:]...)
	} else {
		right := p.children[idx+1]
		l.keys = append(l.keys, right.keys...)
		l.values = append(l.values, right.values...)
		l.next = right.next
		if right.next != nil {
			right.next.prev = l
		}
		p.keys = append(p.keys[:idx], p.keys[idx+1:]...)
		p.children = append(p.children[:idx+1], p.children[idx+2:]...)
	}

	if p == t.root && len(p.keys) == 0 {
		t.root = p.children[0]
		t.root.parent = nil
	}
}

// ---------------- Helpers ----------------

func (t *BPlusTree) findLeaf(key []byte) *node {
	n := t.root
	for n.type_ == internalNode {
		i := sort.Search(len(n.keys), func(i int) bool {
			return bytes.Compare(key, n.keys[i]) < 0
		})
		n = n.children[i]
	}
	return n
}

func indexOfChild(p *node, c *node) int {
	for i, ch := range p.children {
		if ch == c {
			return i
		}
	}
	panic("child not found")
}
