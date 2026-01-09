package stream_core

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
	"sync"

	"github.com/edsrzf/mmap-go"
)

const (
	PageSize = 4096
	Degree   = 64
)

const (
	pageLeaf uint8 = 1
	pageInt  uint8 = 2
)

type leafEntry struct {
	key   []byte
	value uint64
}

type BeeTree struct {
	f    *os.File
	lock sync.Mutex
	root uint64
	data mmap.MMap
}

func NewBeeTree(fname string) (*BeeTree, error) {
	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	if size == 0 {
		if err := f.Truncate(PageSize * 1024); err != nil {
			return nil, err
		}
	}

	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	return &BeeTree{
		f,
		sync.Mutex{},
		0,
		m,
	}, nil
}

func (t *BeeTree) page(id uint64) []byte {
	return t.data[id*PageSize : (id+1)*PageSize]
}

func (t *BeeTree) fitsLeaf(e []leafEntry) bool {
	size := 11
	for _, x := range e {
		size += 2 + len(x.key) + 8
	}
	return size <= PageSize
}

func (t *BeeTree) writeLeaf(p []byte, e []leafEntry) {
	binary.LittleEndian.PutUint16(p[1:], uint16(len(e)))
	off := 11
	for _, x := range e {
		binary.LittleEndian.PutUint16(p[off:], uint16(len(x.key)))
		off += 2
		copy(p[off:], x.key)
		off += len(x.key)
		binary.LittleEndian.PutUint64(p[off:], x.value)
		off += 8
	}
}

func (t *BeeTree) readLeafEntries(p []byte, cnt int) []leafEntry {
	off := 11
	out := make([]leafEntry, 0, cnt)
	for i := 0; i < cnt; i++ {
		klen := int(binary.LittleEndian.Uint16(p[off:]))
		off += 2
		k := append([]byte{}, p[off:off+klen]...)
		off += klen
		v := binary.LittleEndian.Uint64(p[off:])
		off += 8
		out = append(out, leafEntry{k, v})
	}
	return out
}

func (t *BeeTree) Insert(key []byte, value uint64) {
	t.insert(t.root, key, value)
	// promo, right, split := t.insert(t.root, key, value)
	// if split {
	// 	newRoot := t.allocInt()
	// 	p := t.page(newRoot)
	// 	binary.LittleEndian.PutUint16(p[1:], 1)
	// 	off := 11
	// 	binary.LittleEndian.PutUint16(p[off:], uint16(len(promo)))
	// 	off += 2
	// 	copy(p[off:], promo)
	// 	off += len(promo)
	// 	binary.LittleEndian.PutUint64(p[off:], right)
	// 	t.root = newRoot
	// }
}

func (t *BeeTree) insert(pid uint64, key []byte, value uint64) ([]byte, uint64, bool) {
	p := t.page(pid)
	cnt := int(binary.LittleEndian.Uint16(p[1:]))

	off := 11

	if p[0] == pageLeaf {
		entries := t.readLeafEntries(p, cnt)
		entries = append(entries, leafEntry{key, value})
		sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })

		if t.fitsLeaf(entries) {
			t.writeLeaf(p, entries)
			return nil, 0, false
		}

		mid := len(entries) / 2
		left := entries[:mid]
		rightEntries := entries[mid:]

		rightPid := t.allocLeaf()
		writeLeaf(p, left)
		writeLeaf(t.page(rightPid), rightEntries)

		binary.LittleEndian.PutUint64(t.page(rightPid)[3:], binary.LittleEndian.Uint64(p[3:]))
		binary.LittleEndian.PutUint64(p[3:], rightPid)

		return rightEntries[0].key, rightPid, true
	}

	// internal node
	children := readIntEntries(p, cnt)
	idx := 0
	for idx < len(children) && bytes.Compare(key, children[idx].key) >= 0 {
		idx++
	}

	promo, rightChild, split := t.insertRec(children[idx].child, key, value)
	if !split {
		return nil, 0, false
	}

	children = insertInt(children, idx, promo, rightChild)
	if fitsInt(children) {
		writeInt(p, children)
		return nil, 0, false
	}

	mid := len(children) / 2
	promoKey := children[mid].key

	rightPid := t.allocInt()
	writeInt(p, children[:mid])
	writeInt(t.page(rightPid), children[mid+1:])

	return promoKey, rightPid, true
}

func (d *BeeTree) Close() error {
	var err error

	err = d.data.Flush()
	if err != nil {
		return err
	}

	err = d.data.Unmap()
	if err != nil {
		return err
	}

	err = d.f.Close()
	return err
}
