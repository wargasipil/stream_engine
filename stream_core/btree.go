package stream_core

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"sort"
	"sync"

	"github.com/edsrzf/mmap-go"
)

/*
FILE STRUCTURE

[ Metadata Database ]
| Magic (8 bytes) | PagesCount ( 8 bytes ) | FileSize ( 8 bytes )

[ File Pages ]


PAGES STRUCTURE

|	pageType (1 byte)	|	keyCount (2 byte / 16bit)	|	page size (2 byte / 16 bit)	|	nextOffset	(8 byte / 64bit)	| repeated key value 										|
																						| 	key_len (2 byte / 16 bit) 		| value uint64 (8 byte / 64bit) |

*/

var Magic = [8]byte{0x53, 0x61, 0x6e, 0x74, 0x6f, 0x73, 0x6f, 0x20}

const (
	PageSize         = 4096 // 16,384 = 16kb / 4096 = 4kb / 1024 = kilo
	PageMetadataSize = 13
	BeeMetadataSize  = 32
)

const (
	pageUnknown = iota
	pageLeaf
	pageInternal
)

type relOffset int

func (r relOffset) Offset(off int) int {
	return int(r) + off
}

type bpage struct {
	offset int
	data   []byte
}

func (p *bpage) bytes() []byte {
	return p.data[p.offset : p.offset+PageSize]
}

func (p *bpage) keyCount() int16 {
	return int16(binary.LittleEndian.Uint16(p.data[p.offset+1 : p.offset+3]))
}

func (p *bpage) pageSize() int16 {
	return int16(binary.LittleEndian.Uint16(p.data[p.offset+3 : p.offset+5]))
}

func (p *bpage) pageType() int8 {
	return int8(p.data[p.offset])
}

func (p *bpage) getLeafEntry() []leafEntry {
	cnt := p.keyCount()
	res := []leafEntry{}

	var c int16 = 0
	var klen int16
	off := p.offset + PageMetadataSize
	for c < cnt {
		klen = int16(binary.LittleEndian.Uint16(p.data[off : off+2]))
		leaf := leafEntry{
			key: make([]byte, klen),
			val: binary.LittleEndian.Uint64(p.data[off+2+int(klen) : off+2+int(klen)+8]),
		}
		copy(leaf.key, p.data[off+2:off+2+int(klen)])
		res = append(res, leaf)
		off += int(leaf.size())
		c++
	}
	return res
}

func (p *bpage) writeLeafEntry(entries []leafEntry) {
	// set page key count
	var entryCount int16 = int16(len(entries))
	binary.LittleEndian.PutUint16(p.data[p.offset+1:p.offset+3], uint16(entryCount))

	// set data
	off := p.offset + PageMetadataSize
	for _, entry := range entries {
		copy(p.data[off:], entry.data())
		off += int(entry.size())
	}

	// set page size
	pagesize := off - p.offset
	binary.LittleEndian.PutUint16(p.data[p.offset+3:p.offset+5], uint16(pagesize))
}

type leafEntry struct {
	key []byte
	val uint64
}

func (l *leafEntry) data() []byte {
	data := make([]byte, l.size())
	// set keylen
	klen := int16(len(l.key))
	binary.LittleEndian.PutUint16(data[:2], uint16(klen))
	copy(data[2:], l.key)
	binary.LittleEndian.PutUint64(data[2+klen:2+klen+8], l.val)
	return data
}

func (l *leafEntry) size() int16 {
	return int16(len(l.key)) + 2 + 8 // 2 is keylen and 8 is uint64 size
}

// --------------------------- bagian btree --------------------------

type BeeTree struct {
	f    *os.File
	lock sync.Mutex
	data mmap.MMap

	root uint64
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
	var isCreateMeta bool
	if size == 0 {
		isCreateMeta = true
		if err := f.Truncate(PageSize * 1024); err != nil {
			return nil, err
		}

	}

	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	bee := &BeeTree{
		f,
		sync.Mutex{},
		m,
		0,
	}

	if isCreateMeta {
		bee.createMetadata(PageSize * 1024)
	}

	return bee, nil
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

func (t *BeeTree) fileSise() uint64 {
	return binary.LittleEndian.Uint64(t.data[16:32])
}

func (t *BeeTree) pageCount() uint64 {
	return binary.LittleEndian.Uint64(t.data[8:16])
}

func (t *BeeTree) increaseSize() {
	err := t.data.Flush()
	if err != nil {
		log.Fatal(err)
	}

	err = t.data.Unmap()
	if err != nil {
		log.Fatal(err)
	}
	fileSize := t.fileSise() * 2
	binary.LittleEndian.PutUint64(t.data[16:32], fileSize)
	err = t.f.Truncate(int64(fileSize))
	if err != nil {
		log.Fatal(err)
	}

	t.data, err = mmap.Map(t.f, mmap.RDWR, 0)
	if err != nil {
		log.Fatal(err)
	}
}

func (t *BeeTree) createMetadata(fsize uint64) {
	// set magic file
	copy(t.data[:8], Magic[:])
	// set initial pages count
	binary.LittleEndian.PutUint64(t.data[8:16], 1)
	// set filesize
	binary.LittleEndian.PutUint64(t.data[16:32], fsize)
	// set
	var page relOffset = BeeMetadataSize
	t.data[page.Offset(0)] = pageLeaf
}

func (t *BeeTree) insert(pageId int, key []byte, val uint64) ([]byte, uint64, bool) {
	leaf := leafEntry{
		key: key,
		val: val,
	}

	// checking jika filesize kurang besar harus tumbuh
	if (t.pageCount()*PageSize + uint64(leaf.size())) >= t.fileSise() {
		t.increaseSize()
	}

	page := bpage{
		offset: (pageId * PageSize) + BeeMetadataSize,
		data:   t.data,
	}

	if page.pageType() == pageLeaf {

		entries := page.getLeafEntry()
		keyCount := page.keyCount()
		i := sort.Search(int(page.keyCount()), func(i int) bool {
			return bytes.Compare(entries[i].key, key) >= 0
		})

		// jika size cukup
		if (page.pageSize() + leaf.size()) < PageSize {
			if i == int(keyCount) { // jika key tidak ada
				entries = append(entries, leaf)
				// set key count
			} else {
				entries[i].val = val
			}

			// tulis leaf
			page.writeLeafEntry(entries)
			log.Println(page.keyCount(), "keycount")
			log.Println(page.getLeafEntry())
		}

		// jika size tidak cukup

	}

	panic("internal page not implemented")

	return []byte{}, 0, false

}

func (t *BeeTree) Insert(key string, value uint64) {
	t.insert(0, []byte(key), value)
}
