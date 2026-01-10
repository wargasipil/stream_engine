package beetree

import (
	"bytes"
	"sort"
)

func (t *BeeTree) Get(key []byte) (uint64, bool) {
	l := t.findLeaf(key)

	page := bpage{
		offset: (l * PageSize) + BeeMetadataSize,
		data:   t.data,
	}

	entries := page.getEntry()
	entriesLen := len(entries)

	i := sort.Search(entriesLen, func(i int) bool {
		return bytes.Compare(entries[i].key, key) >= 0
	})
	if i < entriesLen && bytes.Equal(entries[i].key, key) {
		return entries[i].val, true
	}
	return 0, false
}
