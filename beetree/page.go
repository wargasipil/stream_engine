package beetree

import (
	"encoding/binary"
	"fmt"
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

func getPageType(pageId int, data []byte) int8 {
	offset := (pageId * PageSize) + BeeMetadataSize
	return int8(data[offset])

}

func getPage(pageID int, data []byte) *bpage {
	if pageID == 0 {
		panic("pageId cannot 0")
	}

	page := bpage{
		offset: (pageID * PageSize) + BeeMetadataSize,
		data:   data,
	}
	return &page
}

func (p *bpage) pageType() int8 {
	return int8(p.data[p.offset])
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
	if pageID == 0 {
		panic("pageId cannot 0")
	}
	binary.LittleEndian.PutUint64(p.data[p.offset+29:p.offset+37], uint64(pageID))
}

func (p *bpage) bytes() []byte {
	return p.data[p.offset : p.offset+PageSize]
}

func (p *bpage) PrintDebug() {
	fmt.Printf("pageId: %d parentId: %d type: %d offset: %d keyCount: %d next: %d\n", p.pageID(), p.parent(), p.pageType(), p.offset, p.keyCount(), p.next())
}
