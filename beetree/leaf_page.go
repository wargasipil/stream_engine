package beetree

import (
	"encoding/binary"
	"fmt"
)

type leafPage struct {
	offset int
	data   []byte
}

func getLeafPage(pageID int, data []byte) *leafPage {
	page := leafPage{
		offset: (pageID * PageSize) + BeeMetadataSize,
		data:   data,
	}

	if page.pageType() != pageLeaf {
		err := fmt.Errorf("pageId: %d is not leaf", pageID)
		panic(err)
	}

	return &page
}

func newLeafPage(pageID int, data []byte) *leafPage {
	page := leafPage{
		offset: (pageID * PageSize) + BeeMetadataSize,
		data:   data,
	}

	if page.pageType() != pageUnknown {
		err := fmt.Errorf("pageId: %d is initiated", pageID)
		panic(err)
	}

	page.putPageType(pageLeaf)
	page.putPageID(pageID)

	return &page

}

func (p *leafPage) pageType() int8 {
	return int8(p.data[p.offset])
}

func (p *leafPage) putPageType(pageType int) {
	p.data[p.offset] = byte(pageType)
}

func (p *leafPage) pageID() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+1 : p.offset+9])
	return int(d)
}

func (p *leafPage) putPageID(pageID int) {
	binary.LittleEndian.PutUint64(p.data[p.offset+1:p.offset+9], uint64(pageID))
}

func (p *leafPage) keyCount() int16 {
	return int16(binary.LittleEndian.Uint16(p.data[p.offset+9 : p.offset+11]))
}

func (p *leafPage) putKeyCount(keyCount int16) {
	binary.LittleEndian.PutUint16(p.data[p.offset+9:p.offset+11], uint16(keyCount))
}

func (p *leafPage) pageSize() int16 {
	return int16(binary.LittleEndian.Uint16(p.data[p.offset+11 : p.offset+13]))
}
func (p *leafPage) putPageSize(pageSize int16) {
	binary.LittleEndian.PutUint16(p.data[p.offset+11:p.offset+13], uint16(pageSize))
}

func (p *leafPage) next() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+13 : p.offset+21])
	return int(d)
}

func (p *leafPage) putNext(pageID int) {
	binary.LittleEndian.PutUint64(p.data[p.offset+13:p.offset+21], uint64(pageID))
}

func (p *leafPage) prev() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+21 : p.offset+29])
	return int(d)
}

func (p *leafPage) putPrev(pageID int) {
	binary.LittleEndian.PutUint64(p.data[p.offset+21:p.offset+29], uint64(pageID))
}

func (p *leafPage) parent() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+29 : p.offset+37])
	return int(d)
}

func (p *leafPage) putParent(pageID int) {
	if pageID == 0 {
		panic("pageId cannot 0")
	}
	binary.LittleEndian.PutUint64(p.data[p.offset+29:p.offset+37], uint64(pageID))
}

func (p *leafPage) bytes() []byte {
	return p.data[p.offset : p.offset+PageSize]
}

func (p *leafPage) getEntry() leafEntryList {
	cnt := p.keyCount()
	res := leafEntryList{}

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

func (p *leafPage) writeEntry(entries leafEntryList) {
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

func (p *leafPage) PrintDebug() {
	fmt.Printf("pageId: %d parentId: %d type: %d offset: %d keyCount: %d next: %d\n", p.pageID(), p.parent(), p.pageType(), p.offset, p.keyCount(), p.next())
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

type leafEntryList []*leafEntry

func (l leafEntryList) Print() {
	for _, l := range l {
		fmt.Printf("key: %s value: %d\n", string(l.key), l.val)
	}
}

func (l leafEntryList) PrintMinMax() {
	if len(l) > 0 {
		fmt.Printf("\tmin: %s max: %s\n", string(l[0].key), string(l[len(l)-1].key))
	}
}
