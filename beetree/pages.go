package beetree

import (
	"encoding/binary"
)

/*
FILE STRUCTURE

[ Metadata Database ]
| Magic (8 byte) | PagesCount (8 byte) | FileSize (8 byte) | rootpage (8 byte)

[ File Pages ]


PAGES STRUCTURE

|	pageType (1 byte)			| pageID uint64 (8 byte / 64bit) | keyCount (2 byte / 16bit)	|	page size (2 byte / 16 bit)	|	nextpage (8 byte / 64bit) | prevpage (8 byte / 64bit) | parentpage (8 byte / 64bit) | repeated key value |

Data Pages Ada 2 struktur

| 	key_len (2 byte / 16 bit)	| value uint64 (8 byte / 64bit) |


*/

const (
	PageSize         = 4096 // 16,384 = 16kb / 4096 = 4kb / 1024 = kilo
	PageMetadataSize = 1 + 8 + 2 + 2 + 8 + 8 + 8
)

type bpage struct {
	offset int
	data   []byte
}

func newBpage(pageID int, pageType int, data []byte) bpage {
	page := bpage{
		offset: (pageID * PageSize) + BeeMetadataSize,
		data:   data,
	}

	page.putPageType(pageType)
	page.putPageID(pageID)

	return page
}

func (p *bpage) pageType() int8 {
	return int8(p.data[p.offset])
}

func (p *bpage) putPageType(pageType int) {
	p.data[p.offset] = byte(pageType)
}

func (p *bpage) pageID() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+1 : p.offset+9])
	return int(d)
}

func (p *bpage) putPageID(pageID int) {
	binary.LittleEndian.PutUint64(p.data[p.offset+1:p.offset+9], uint64(pageID))
}

func (p *bpage) keyCount() int16 {
	return int16(binary.LittleEndian.Uint16(p.data[p.offset+9 : p.offset+11]))
}

func (p *bpage) putKeyCount(keyCount int16) {
	binary.LittleEndian.PutUint16(p.data[p.offset+9:p.offset+11], uint16(keyCount))
}

func (p *bpage) pageSize() int16 {
	return int16(binary.LittleEndian.Uint16(p.data[p.offset+11 : p.offset+13]))
}
func (p *bpage) putPageSize(pageSize int16) {
	binary.LittleEndian.PutUint16(p.data[p.offset+11:p.offset+13], uint16(pageSize))
}

func (p *bpage) next() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+13 : p.offset+21])
	return int(d)
}

func (p *bpage) putNext(pageID int) {
	binary.LittleEndian.PutUint64(p.data[p.offset+13:p.offset+21], uint64(pageID))
}

func (p *bpage) prev() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+21 : p.offset+29])
	return int(d)
}

func (p *bpage) putPrev(pageID int) {
	binary.LittleEndian.PutUint64(p.data[p.offset+21:p.offset+29], uint64(pageID))
}

func (p *bpage) parent() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+29 : p.offset+37])
	return int(d)
}

func (p *bpage) putParent(pageID int) {
	binary.LittleEndian.PutUint64(p.data[p.offset+29:p.offset+37], uint64(pageID))
}

func (p *bpage) bytes() []byte {
	return p.data[p.offset : p.offset+PageSize]
}

func (p *bpage) getEntry() entryList {
	cnt := p.keyCount()
	res := entryList{}

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
		res = append(res, &leaf)
		off += int(leaf.size())
		c++
	}
	return res
}

func (p *bpage) getInternalEntry() []*internalEntry {
	cnt := p.keyCount()
	res := []*internalEntry{}

	var c int16 = 0
	var klen int16
	off := p.offset + PageMetadataSize
	for c < cnt {
		klen = int16(binary.LittleEndian.Uint16(p.data[off : off+2]))
		d := binary.LittleEndian.Uint64(p.data[off+2+int(klen) : off+2+int(klen)+8])
		entry := internalEntry{
			key:    make([]byte, klen),
			pageId: int(d),
		}
		copy(entry.key, p.data[off+2:off+2+int(klen)])
		res = append(res, &entry)
		off += int(entry.size())
		c++
	}
	return res
}

func (p *bpage) writeLeafEntry(entries []*leafEntry) {
	// sort.Slice(entries, func(i, j int) bool {
	// 	return bytes.Compare(entries[i].key, entries[j].key) < 0
	// })

	// set page key count
	var entryCount int16 = int16(len(entries))
	p.putKeyCount(entryCount)

	// set data
	off := p.offset + PageMetadataSize
	for _, entry := range entries {
		copy(p.data[off:], entry.data())
		off += int(entry.size())
	}

	// set page size
	pagesize := off - p.offset
	p.putPageSize(int16(pagesize))
}

func (p *bpage) writeInternalEntry(entries []*internalEntry) {
	// sort.Slice(entries, func(i, j int) bool {
	// 	return bytes.Compare(entries[i].key, entries[j].key) < 0
	// })

	// set page key count
	var entryCount int16 = int16(len(entries))
	p.putKeyCount(entryCount)

	// set data
	off := p.offset + PageMetadataSize
	for _, entry := range entries {
		copy(p.data[off:], entry.data())
		off += int(entry.size())
	}

	// set page size
	pagesize := off - p.offset
	p.putPageSize(int16(pagesize))
}
