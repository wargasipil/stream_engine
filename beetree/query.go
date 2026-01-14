package beetree

import (
	"bytes"
	"sort"
)

func (t *BeeTree) GetKeyString(key string) (uint64, bool) {
	return t.Get([]byte(key))
}

func (t *BeeTree) Get(key []byte) (uint64, bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	t.Log("getting key: %s\n", string(key))

	l := t.findLeaf(key)

	page := getLeafPage(l, t.data)

	entries := page.getEntry()
	entriesLen := len(entries)

	// if t.debug {
	// 	page.PrintDebug()
	// }

	i := sort.Search(entriesLen, func(i int) bool {
		return bytes.Compare(entries[i].key, key) >= 0
	})

	if i < entriesLen && bytes.Equal(entries[i].key, key) {
		return entries[i].val, true
	}

	// log.Println("errorrr", string(key))
	// entries.Print()
	// log.Println("-----------------------")
	// next := bpage{
	// 	offset: page.next(),
	// 	data:   t.data,
	// }
	// next.getEntry().Print()

	return 0, false
}
