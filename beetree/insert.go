package beetree

import (
	"bytes"
	"sort"
)

func (t *BeeTree) findLeaf(key []byte) int {
	n := t.rootPage()
	var page *bpage = &bpage{
		offset: (int(n) * PageSize) + BeeMetadataSize,
		data:   t.data,
	}

	for page.pageType() == pageInternal {
		// if page.pageType() != pageInternal {
		// 	panic("unknown page type")
		// }

		// page = &bpage{
		// 	offset: (int(page.pageID()) * PageSize) + BeeMetadataSize,
		// 	data:   t.data,
		// }

		entries := page.getInternalEntry()

		i := sort.Search(len(entries), func(i int) bool {
			return bytes.Compare(entries[i].key, key) > 0
		})

		if i > 0 {
			i--
		}

		if len(entries) == i {
			n = entries[0].pageId
		} else {
			// if entries[i] == nil {
			// 	panic("data corupted cannot find leaf node")
			// }

			n = int(entries[i].pageId)
		}

		page = &bpage{
			offset: (int(n) * PageSize) + BeeMetadataSize,
			data:   t.data,
		}

	}

	// entries := page.getEntry()
	// if len(entries) > 0 {
	// 	log.Println("leaf finding", string(key), page.pageType())
	// 	log.Println("leaf min", string(entries[0].key), "max", string(entries[len(entries)-1].key))
	// }

	return page.pageID()
}

func (t *BeeTree) pageIdOffset(pageID int) int {
	return pageID*PageSize + BeeMetadataSize
}

func (t *BeeTree) splitLeaf(left *bpage, entries entryList) {
	mid := len(entries) / 2

	rightPageId := t.nextPageId()

	right := newBpage(rightPageId, pageLeaf, t.data)

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
	right.writeLeafEntry(entries[mid:])
	left.writeLeafEntry(entries[:mid])

	// handle parent
	if left.parent() == 0 {
		rootPageId := t.nextPageId()
		root := newBpage(rootPageId, pageInternal, t.data)
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
		root.writeInternalEntry(internalEntries)
		return
	}

	separator := rentries[0].key
	parentPage := &bpage{
		offset: (left.parent() * PageSize) + BeeMetadataSize,
		data:   t.data,
	}

	entry := internalEntry{
		key:    separator,
		pageId: rightPageId,
	}

	t.insertIntoParent(parentPage, &entry)
}

// ---------------- Internal Insert ----------------

func (t *BeeTree) insertIntoParent(page *bpage, entry *internalEntry) {
	entries := page.getInternalEntry()
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

	if (page.pageSize() + entry.size()) > PageSize {
		t.splitInternal(page, entries)
		return
	}

	page.writeInternalEntry(entries)
}

// ---------------- Internal Split ----------------

func (t *BeeTree) splitInternal(left *bpage, entries []*internalEntry) {
	mid := len(entries) / 2

	rightPageId := t.nextPageId()
	right := newBpage(rightPageId, pageInternal, t.data)

	rentries := entries[mid:]
	right.writeInternalEntry(rentries)
	left.writeInternalEntry(entries[:mid])

	if left.parent() == 0 {
		rootPageId := t.nextPageId()
		root := newBpage(rootPageId, pageInternal, t.data)
		// updating root to metadata
		t.putRootPage(rootPageId)
		// add parent to left and right
		left.putParent(rootPageId)
		right.putParent(rootPageId)

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
		root.writeInternalEntry(internalEntry)
		return
	}

	separator := rentries[0].key
	parentPage := &bpage{
		offset: (left.parent() * PageSize) + BeeMetadataSize,
		data:   t.data,
	}

	entry := internalEntry{
		key:    separator,
		pageId: rightPageId,
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

	page := bpage{
		offset: (int(leaf) * PageSize) + BeeMetadataSize,
		data:   t.data,
	}

	entries := page.getEntry()
	entrieslen := len(entries)

	i := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].key, key) >= 0
	})

	if i < entrieslen && bytes.Equal(entries[i].key, key) {
		entries[i].val = value
		page.writeLeafEntry(entries)
		return
	}

	entries = append(entries, nil)
	copy(entries[i+1:], entries[i:])
	entries[i] = &entry

	if (page.pageSize() + entry.size()) > PageSize {
		t.splitLeaf(&page, entries)
		return
	}

	page.writeLeafEntry(entries)
}
