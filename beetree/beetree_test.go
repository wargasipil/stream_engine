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
		sync.Mutex{},
		make([]byte, PageSize*PageSize),
	}

	bee.createMetadata(PageSize * PageSize)

	t.Run("test splitpage", func(t *testing.T) {
		bee.rootPage()
		page := bpage{
			offset: bee.pageIdOffset(0),
			data:   bee.data,
		}
		page.putPageType(pageLeaf)
		page.putPageID(0)

		page.writeLeafEntry([]*leafEntry{
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

			bee.splitLeaf(&page, entries)

			right := bpage{
				offset: bee.pageIdOffset(1),
				data:   bee.data,
			}

			assert.Equal(t, string(entries[2].key), string(right.getEntry()[0].key))
			assert.Equal(t, page.parent(), right.parent())

			parent := bpage{
				offset: bee.pageIdOffset(page.parent()),
				data:   bee.data,
			}

			internals := parent.getInternalEntry()
			assert.Len(t, internals, 2)

			log.Println("parent")
			parent.getInternalEntry().Print()
			log.Println("left")
			page.getEntry().Print()
			log.Println("right")
			right.getEntry().Print()

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

				bee.splitLeaf(&right, entries)

				right2 := bpage{
					offset: bee.pageIdOffset(3),
					data:   bee.data,
				}
				log.Println("parent")
				parent.getInternalEntry().Print()
				log.Println("right2")
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

		t.Run("getting leaf", func(t *testing.T) {
			pageId := bee.findLeaf([]byte("d"))
			assert.Equal(t, 1, pageId)

			pageId = bee.findLeaf([]byte("y"))
			assert.Equal(t, 3, pageId)

			pageId = bee.findLeaf([]byte("b"))
			assert.Equal(t, 0, pageId)

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
		sync.Mutex{},
		make([]byte, PageSize*PageSize),
	}

	bee.createMetadata(PageSize * PageSize)

	errcount := 0

	bee.Insert("accounting_pkey", 123)

	for c := 0; c < 400; c++ {
		key := fmt.Sprintf("%d_key", c)
		bee.Insert(key, uint64(c))

		off, ok := bee.Get([]byte(key))
		if !ok {
			errcount++
		}
		assert.True(t, ok)
		assert.Equal(t, c, int(off))

	}
	off, ok := bee.Get([]byte("accounting_pkey"))
	assert.True(t, ok)
	assert.Equal(t, 123, int(off))

	assert.Equal(t, 0, errcount)

}
