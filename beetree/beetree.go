package beetree

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

/*
FILE STRUCTURE

[ Metadata Database ]
| Magic (8 byte) | PagesCount (8 byte) | FileSize (8 byte) | rootpage (8 byte)

[ File Pages ]


PAGES STRUCTURE

|	pageType (1 byte)			|	keyCount (2 byte / 16bit)	|	page size (2 byte / 16 bit)	|	nextpage (8 byte / 64bit) | (8 byte / 64bit) | prevpage repeated key value 										|

| 	key_len (2 byte / 16 bit)	| value uint64 (8 byte / 64bit) |

*/

var Magic = [8]byte{0x53, 0x61, 0x6e, 0x74, 0x6f, 0x73, 0x6f, 0x20}

const (
	pageUnknown = iota
	pageLeaf
	pageInternal
)

const (
	BeeMetadataSize = 32
)

type relOffset int

func (r relOffset) Offset(off int) int {
	return int(r) + off
}

type BeeTree struct {
	f     *os.File
	lock  sync.RWMutex
	data  mmap.MMap
	debug bool
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
		sync.RWMutex{},
		m,
		false,
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

func (t *BeeTree) pageCount() int {
	return int(binary.LittleEndian.Uint64(t.data[8:16]))
}

func (t *BeeTree) nextPageCount() int {
	page := t.pageCount() + 1
	binary.LittleEndian.PutUint64(t.data[8:16], uint64(page))
	return page
}

func (t *BeeTree) fileSize() uint64 {
	return binary.LittleEndian.Uint64(t.data[16:24])
}

func (t *BeeTree) putFileSize(fileSize uint64) {
	binary.LittleEndian.PutUint64(t.data[16:24], fileSize)
}

func (t *BeeTree) rootPage() int {
	d := binary.LittleEndian.Uint64(t.data[24:32])
	return int(d)
}

func (t *BeeTree) putRootPage(root int) int {
	binary.LittleEndian.PutUint64(t.data[24:32], uint64(root))
	return root
}

func (t *BeeTree) nextPageId() int {
	next := t.nextPageCount()

	return next
}

// hati hati tidak di lock
func (t *BeeTree) increaseSize() {
	// t.lock.Lock()
	// defer t.lock.Unlock()

	err := t.data.Flush()
	if err != nil {
		log.Fatal(err)
	}

	fileSize := t.fileSize() * 2
	err = t.data.Unmap()
	if err != nil {
		log.Fatal(err)
	}

	err = t.f.Truncate(int64(fileSize))
	if err != nil {
		log.Fatal(err)
	}

	t.data, err = mmap.Map(t.f, mmap.RDWR, 0)
	if err != nil {
		log.Fatal(err)
	}

	t.putFileSize(fileSize)
}

func (t *BeeTree) createMetadata(fsize uint64) {
	// set magic file
	copy(t.data[:8], Magic[:])
	// set initial pages count
	binary.LittleEndian.PutUint64(t.data[8:16], 1)
	// set filesize
	binary.LittleEndian.PutUint64(t.data[16:24], fsize)

	rootId := t.nextPageId()
	root := newLeafPage(rootId, t.data)
	root.writeEntry([]*leafEntry{
		{
			key: []byte{0x0},
			val: 255,
		},
	})
	t.putRootPage(rootId)
}

func LogJson(v ...any) {
	for _, item := range v {
		data, _ := json.MarshalIndent(item, "", "  ")
		log.Println(string(data))
	}

}
