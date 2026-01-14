package beetree

import (
	"bytes"
	"fmt"
	"log"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	bee := &BeeTree{
		nil,
		sync.RWMutex{},
		make([]byte, PageSize*PageSize),
		false,
	}

	bee.createMetadata(PageSize * PageSize)

	t.Run("test splitpage", func(t *testing.T) {
		rootPageId := bee.rootPage()
		page := getLeafPage(rootPageId, bee.data)

		page.writeEntry([]*leafEntry{
			// {
			// 	key: []byte{0x0},
			// 	val: 0,
			// },
			{
				key: []byte("b"),
				val: 1,
			},
			{
				key: []byte("c"),
				val: 2,
			},
			{
				key: []byte("x"),
				val: 3,
			},
			{
				key: []byte("z"),
				val: 4,
			},
		})

		t.Run("testing split leaf", func(t *testing.T) {
			entries := page.getEntry()
			entry := &leafEntry{
				key: []byte("d"),
				val: 5,
			}
			entries = append(entries, entry)

			slices.SortFunc(entries, func(a, b *leafEntry) int {
				// log.Println(bytes.Compare(a.key, b.key) >= 0, a.key, b.key, string(a.key), string(b.key))
				return bytes.Compare(a.key, b.key)
			})

			_, rightPage := bee.splitLeaf(page, entries)

			right := getLeafPage(rightPage, bee.data)
			assert.Len(t, right.getEntry(), 3)

			assert.Equal(t, string(entries[2].key), string(right.getEntry()[0].key))
			assert.Equal(t, page.parent(), right.parent())

			parent := getInternalPage(page.parent(), bee.data)

			internals := parent.getEntry()
			assert.Len(t, internals, 2)

			// log.Println("parent")
			// parent.getInternalEntry().Print()
			// log.Println("left")
			// page.getEntry().Print()
			// log.Println("right")
			// right.getEntry().Print()

			t.Run("testing split ketiga kali", func(t *testing.T) {
				entry := &leafEntry{
					key: []byte("y"),
					val: 5,
				}

				entries := right.getEntry()
				entries = append(entries, entry)

				slices.SortFunc(entries, func(a, b *leafEntry) int {
					// log.Println(bytes.Compare(a.key, b.key) >= 0, a.key, b.key, string(a.key), string(b.key))
					return bytes.Compare(a.key, b.key)
				})

				_, right2Id := bee.splitLeaf(right, entries)
				right2 := getLeafPage(right2Id, bee.data)

				assert.NotEqual(t, 0, right2.parent())

				log.Println("parent", parent.pageID())
				parent.getEntry().Print()
				log.Println("left", page.pageID())
				page.getEntry().Print()
				log.Println("right", right.pageID())
				right.getEntry().Print()
				log.Println("right2", right2.pageID())
				right2.getEntry().Print()

			})

			t.Run("testing get tidak ada", func(t *testing.T) {
				off, ok := bee.Get([]byte("unknown_data"))
				assert.False(t, ok)
				assert.Equal(t, uint64(0), off)
			})

			// internals = parent.getInternalEntry()
			// assert.Len(t, internals, 3)
			// log.Println("parent")
			// parent.getInternalEntry().Print()
			// log.Println("left")
			// page.getEntry().Print()
			// log.Println("right")
			// right.getEntry().Print()

		})

		bee.VerifyPage()

		t.Run("getting leaf", func(t *testing.T) {
			pageId := bee.findLeaf([]byte("d"))
			assert.Equal(t, 3, pageId)

			pageId = bee.findLeaf([]byte("y"))
			assert.Equal(t, 5, pageId)

			pageId = bee.findLeaf([]byte("b"))
			assert.Equal(t, 2, pageId)

			pageId = bee.findLeaf([]byte("c"))
			assert.Equal(t, 2, pageId)

		})

	})

	// errcount := 0

	// // bee.Insert("accounting_pkey", 123)

	// for c := 0; c < 300; c++ {
	// 	key := fmt.Sprintf("%d_key", c)
	// 	bee.Insert(key, uint64(c))

	// 	off, ok := bee.Get([]byte(key))
	// 	if !ok {
	// 		errcount++
	// 	}
	// 	assert.True(t, ok)
	// 	assert.Equal(t, c, int(off))

	// }
	// off, ok := bee.Get([]byte("accounting_pkey"))
	// assert.True(t, ok)
	// assert.Equal(t, 123, int(off))

	// assert.Equal(t, 0, errcount)

	// off, ok = bee.Get([]byte("5ec8f0ff-f262-4ca5-a276-32e45721aafb"))
	// assert.True(t, ok)
	// assert.Equal(t, 123, int(off))

}

func TestManyKey(t *testing.T) {
	bee := &BeeTree{
		nil,
		sync.RWMutex{},
		make([]byte, PageSize*PageSize),
		false,
	}

	bee.createMetadata(PageSize * PageSize)

	errcount := 0

	bee.InsertKeyString("accounting_pkey", 123)

	for c := 0; c < 400; c++ {
		key := fmt.Sprintf("%d_key", c)
		bee.InsertKeyString(key, uint64(c))

		off, ok := bee.Get([]byte(key))
		if !ok {
			errcount++
		}
		assert.True(t, ok)
		assert.Equal(t, c, int(off))

	}

	for c := 0; c < 400; c++ {
		key := fmt.Sprintf("%d_key", c)
		off, ok := bee.Get([]byte(key))
		if !ok {
			errcount++
		}
		assert.True(t, ok, key)
		assert.Equal(t, c, int(off), key)

	}

	off, ok := bee.Get([]byte("accounting_pkey"))
	assert.True(t, ok)
	assert.Equal(t, 123, int(off))

	assert.Equal(t, 0, errcount)

}
