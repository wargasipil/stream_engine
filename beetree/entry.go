package beetree

import (
	"encoding/binary"
	"fmt"
)

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

type entryList []*leafEntry

func (l entryList) Print() {
	for _, l := range l {
		fmt.Printf("key: %s value: %d\n", string(l.key), l.val)
	}
}
