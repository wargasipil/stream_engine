package beetree

import (
	"encoding/binary"
	"fmt"
)

func isInternalPage(pageId int, data []byte) bool {
	offset := (pageId * PageSize) + BeeMetadataSize
	pageType := int8(data[offset])
	return pageType == pageInternal
}

type internalPage struct {
	offset int
	data   []byte
}

func getInternalPage(pageID int, data []byte) *internalPage {
	page := internalPage{
		offset: (pageID * PageSize) + BeeMetadataSize,
		data:   data,
	}

	if page.pageType() != pageInternal {
		err := fmt.Errorf("pageId: %d is not internal", pageID)
		panic(err)
	}

	return &page
}

func newInternalPage(pageID int, data []byte) *internalPage {
	page := internalPage{
		offset: (pageID * PageSize) + BeeMetadataSize,
		data:   data,
	}

	if page.pageType() != pageUnknown {
		err := fmt.Errorf("pageId: %d is initiated", pageID)
		panic(err)
	}

	page.putPageType(pageInternal)
	page.putPageID(pageID)

	return &page

}

func (p *internalPage) pageType() int8 {
	return int8(p.data[p.offset])
}

func (p *internalPage) putPageType(pageType int) {
	p.data[p.offset] = byte(pageType)
}

func (p *internalPage) pageID() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+1 : p.offset+9])
	return int(d)
}

func (p *internalPage) putPageID(pageID int) {
	binary.LittleEndian.PutUint64(p.data[p.offset+1:p.offset+9], uint64(pageID))
}

func (p *internalPage) keyCount() int16 {
	return int16(binary.LittleEndian.Uint16(p.data[p.offset+9 : p.offset+11]))
}

func (p *internalPage) putKeyCount(keyCount int16) {
	binary.LittleEndian.PutUint16(p.data[p.offset+9:p.offset+11], uint16(keyCount))
}

func (p *internalPage) pageSize() int16 {
	return int16(binary.LittleEndian.Uint16(p.data[p.offset+11 : p.offset+13]))
}
func (p *internalPage) putPageSize(pageSize int16) {
	binary.LittleEndian.PutUint16(p.data[p.offset+11:p.offset+13], uint16(pageSize))
}

func (p *internalPage) next() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+13 : p.offset+21])
	return int(d)
}

func (p *internalPage) putNext(pageID int) {
	binary.LittleEndian.PutUint64(p.data[p.offset+13:p.offset+21], uint64(pageID))
}

func (p *internalPage) prev() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+21 : p.offset+29])
	return int(d)
}

func (p *internalPage) putPrev(pageID int) {
	binary.LittleEndian.PutUint64(p.data[p.offset+21:p.offset+29], uint64(pageID))
}

func (p *internalPage) parent() int {
	d := binary.LittleEndian.Uint64(p.data[p.offset+29 : p.offset+37])
	return int(d)
}

func (p *internalPage) putParent(pageID int) {
	if pageID == 0 {
		panic("pageId cannot 0")
	}
	binary.LittleEndian.PutUint64(p.data[p.offset+29:p.offset+37], uint64(pageID))
}

func (p *internalPage) bytes() []byte {
	return p.data[p.offset : p.offset+PageSize]
}

func (p *internalPage) getEntry() internalEntryList {
	cnt := p.keyCount()
	res := internalEntryList{}

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

func (p *internalPage) writeEntry(entries []*internalEntry) {
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

func (p *internalPage) PrintDebug() {
	fmt.Printf("pageId: %d parentId: %d type: %d offset: %d keyCount: %d next: %d\n", p.pageID(), p.parent(), p.pageType(), p.offset, p.keyCount(), p.next())
}

type internalEntry struct {
	key    []byte
	pageId int
}

func (l *internalEntry) size() int16 {
	return int16(len(l.key)) + 2 + 8 // 2 is keylen and 8 is uint64 size
}

func (l *internalEntry) data() []byte {
	data := make([]byte, l.size())
	// set keylen
	klen := int16(len(l.key))
	binary.LittleEndian.PutUint16(data[:2], uint16(klen))
	copy(data[2:], l.key)
	binary.LittleEndian.PutUint64(data[2+klen:2+klen+8], uint64(l.pageId))
	return data
}

type internalEntryList []*internalEntry

func (l internalEntryList) Print() {
	for _, l := range l {
		fmt.Printf("\t\tkey: %s pageId: %d\n", string(l.key), l.pageId)
	}
}

func (l internalEntryList) PrintMinMax() {
	if len(l) > 0 {
		fmt.Printf("\tmin: %s in pageId: %d | max: %s in pageId: %d\n", string(l[0].key), l[0].pageId, string(l[len(l)-1].key), l[len(l)-1].pageId)
	}
}
