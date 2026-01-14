package beetree

import (
	"bytes"
	"log"
	"sort"
)

func (t *BeeTree) findLeaf(key []byte) int {
	n := t.rootPage()
	page := getPage(n, t.data)

	deep := 0

	switch page.pageType() {
	case pageLeaf:
		return page.pageID()
	case pageInternal:

		var intPage *internalPage

		for isInternalPage(n, t.data) {
			intPage = getInternalPage(n, t.data)
			entries := intPage.getEntry()

			if t.debug {
				log.Println("key", string(key), "deep", deep, "root", t.rootPage())
				page.PrintDebug()
				entries.Print()
			}

			i := sort.Search(len(entries), func(i int) bool {
				return bytes.Compare(entries[i].key, key) > 0
			})

			if len(entries) == i {
				n = entries[len(entries)-1].pageId
			} else {
				// if i == 0 {
				// 	log.Println(string(key))
				// 	page.PrintDebug()
				// 	entries.Print()
				// }
				// t.VerifyPage()

				i--
				n = entries[i].pageId
				deep++

			}
		}

		return n

	default:
		panic("unknown page type")
	}
}

// func (t *BeeTree) pageIdOffset(pageID int) int {
// 	return pageID*PageSize + BeeMetadataSize
// }

func (t *BeeTree) splitLeaf(left *leafPage, entries leafEntryList) (int, int) {
	mid := len(entries) / 2

	rightPageId := t.nextPageId()
	right := newLeafPage(rightPageId, t.data)

	// navigasi leaf
	right.putNext(left.next())
	right.putPrev(left.pageID())

	if left.next() != 0 {
		nextPage := bpage{
			offset: (left.next() * PageSize) + BeeMetadataSize,
			data:   t.data,
		}
		nextPage.putPrev(right.pageID())
		left.putNext(right.pageID())
	}

	// rewriting data
	rentries := entries[mid:]
	right.writeEntry(entries[mid:])
	left.writeEntry(entries[:mid])

	// handle parent
	if left.parent() == 0 {
		rootPageId := t.nextPageId()
		root := newInternalPage(rootPageId, t.data)
		// updating root to metadata
		t.putRootPage(rootPageId)
		// add parent to left and right
		left.putParent(rootPageId)
		right.putParent(rootPageId)

		// writing internal key to parent
		internalEntries := []*internalEntry{
			{
				key:    entries[0].key,
				pageId: left.pageID(),
			},
			{
				key:    entries[mid].key,
				pageId: right.pageID(),
			},
		}
		root.writeEntry(internalEntries)
		return left.pageID(), right.pageID()
	}

	right.putParent(left.parent())

	separator := rentries[0].key
	parentPage := getInternalPage(left.parent(), t.data)

	entry := internalEntry{
		key:    separator,
		pageId: rightPageId,
	}

	t.insertIntoParent(parentPage, &entry)

	return left.pageID(), right.pageID()
}

// ---------------- Internal Insert ----------------

func (t *BeeTree) insertIntoParent(page *internalPage, entry *internalEntry) {
	entries := page.getEntry()
	// entrieslen := len(entries)

	i := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].key, entry.key) >= 0
	})

	// if i < entrieslen && bytes.Equal(entries[i].key, entry.key) {
	// 	panic("index corrupted internal entry duplicated")
	// }

	entries = append(entries, nil)
	copy(entries[i+1:], entries[i:])
	entries[i] = entry

	// log.Println("------------writing parent")
	// entries.Print()
	// log.Println("------------writing parent")

	if (page.pageSize() + entry.size()) > PageSize {
		t.splitInternal(page, entries)
		return
	}

	page.writeEntry(entries)
}

// ---------------- Internal Split ----------------

func (t *BeeTree) splitInternal(left *internalPage, entries []*internalEntry) {
	mid := len(entries) / 2

	rightPageId := t.nextPageId()
	right := newInternalPage(rightPageId, t.data)

	rentries := entries[mid:]
	right.writeEntry(rentries)
	left.writeEntry(entries[:mid])

	if left.parent() == 0 {
		rootPageId := t.nextPageId()
		root := newInternalPage(rootPageId, t.data)
		// updating root to metadata
		t.putRootPage(rootPageId)
		// add parent to left and right
		left.putParent(rootPageId)
		right.putParent(rootPageId)

		for _, entry := range rentries {
			getPage(entry.pageId, t.data).putParent(rightPageId)
			// getInternalPage(entry.pageId, t.data).putParent(rightPageId)
		}

		internalEntry := []*internalEntry{
			{
				key:    entries[0].key,
				pageId: left.pageID(),
			},
			{
				key:    entries[mid].key,
				pageId: right.pageID(),
			},
		}
		root.writeEntry(internalEntry)

		return
	}

	separator := rentries[0].key
	parentPage := getInternalPage(left.parent(), t.data)

	entry := internalEntry{
		key:    separator,
		pageId: rightPageId,
	}

	right.putParent(left.parent())
	for _, entry := range rentries {
		getPage(entry.pageId, t.data).putParent(rightPageId)
		// getInternalPage(entry.pageId, t.data).putParent(rightPageId)
	}

	t.insertIntoParent(parentPage, &entry)
}

func (t *BeeTree) InsertKeyString(key string, value uint64) {
	t.Insert([]byte(key), value)
}

func (t *BeeTree) Insert(key []byte, value uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// checking file size
	if (((t.pageCount() + 8) * PageSize) + BeeMetadataSize) > int(t.fileSize()) {

		t.increaseSize()
	}

	entry := leafEntry{
		key: key,
		val: value,
	}

	leaf := t.findLeaf(key)

	page := getLeafPage(leaf, t.data)

	entries := page.getEntry()
	entrieslen := len(entries)

	i := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].key, key) >= 0
	})

	if i < entrieslen && bytes.Equal(entries[i].key, key) {
		entries[i].val = value
		page.writeEntry(entries)
		return
	}

	entries = append(entries, nil)
	copy(entries[i+1:], entries[i:])
	entries[i] = &entry

	if (page.pageSize() + entry.size()) > PageSize {
		t.splitLeaf(page, entries)
		return
	}

	page.writeEntry(entries)
}
