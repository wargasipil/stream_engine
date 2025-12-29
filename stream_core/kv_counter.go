package stream_core

import (
	"encoding/binary"
	"sync/atomic"
	"time"
	"unsafe"

	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
	"github.com/google/btree"
)

const (
	SlotSize = 16 // 8 bytes counter + 8 bytes timestamp
	MaxSlots = 1_000_000
	// CounterOffset   = 8
	TimestampOffset = 8
)

type KeyValueCounter struct {
	lock  sync.Mutex
	data  mmap.MMap
	slots uint64
	index *btree.BTree
	next  uint64
	// wal   *WAL
}

func NewKeyValueCounter(cfg *CoreConfig) (*KeyValueCounter, error) {
	size := int(cfg.KVCounterSlots * SlotSize)

	f, err := os.OpenFile(cfg.KVCounterPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	if err := f.Truncate(int64(size)); err != nil {
		return nil, err
	}

	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	// // creating WAL
	// wal, err := OpenWAL(cfg.WalDir)
	// if err != nil {
	// 	return nil, err
	// }

	index := btree.New(BTreeDegree)

	return &KeyValueCounter{
		sync.Mutex{},
		m,
		cfg.KVCounterSlots,
		index,
		0,
	}, nil
}

func (kvc *KeyValueCounter) MergeKey(fieldName string, key ...KeyValueCounter) {
	panic("not implemented")
}

func (kvc *KeyValueCounter) getKeyOffset(key CounterKey) uint64 {
	item := KVItem{Key: []byte(key)}
	var off uint64
	existing := kvc.index.Get(item)

	if existing != nil {
		off = existing.(KVItem).Off

		return off

	} else {
		off = kvc.next
		kvc.next += SlotSize
		kvc.index.ReplaceOrInsert(KVItem{Key: []byte(key), Off: off})

		return off
	}
}

func (kvc *KeyValueCounter) IncInt(key CounterKey, delta int64) int64 {

	// if writeWal {
	// 	kvc.wal.Append(&wal_message.CounterUint{
	// 		Key:   key,
	// 		Value: delta,
	// 	})
	// }
	counterKeys := key.Iterate()

	kvc.lock.Lock()
	defer kvc.lock.Unlock()

	var off uint64
	var val int64

	t := time.Now().UnixMilli()
	ts := uint64(t)

	for _, ckey := range counterKeys {
		item := KVItem{Key: []byte(ckey)}

		existing := kvc.index.Get(item)

		if existing != nil {
			off = existing.(KVItem).Off

			base := unsafe.Pointer(&kvc.data[0])
			ptr := (*int64)(unsafe.Add(base, off))
			val = atomic.AddInt64(ptr, delta)

		} else {
			off = kvc.next
			kvc.next += SlotSize
			val = delta
			kvc.index.ReplaceOrInsert(KVItem{Key: []byte(ckey), Off: off})

			binary.LittleEndian.PutUint64(kvc.data[off:off+8], uint64(delta))
			binary.LittleEndian.PutUint64(kvc.data[off:off+8], uint64(delta))
			val = delta
		}

		binary.LittleEndian.PutUint64(kvc.data[off+TimestampOffset:off+TimestampOffset+8], ts)

	}

	return val
}

func (kvc *KeyValueCounter) GetInt(key CounterKey) (int64, int64) {
	kvc.lock.Lock()
	defer kvc.lock.Unlock()

	search := KVItem{Key: []byte(key)}

	item := kvc.index.Get(search)
	if item == nil {
		return 0, 0
	}

	kv := item.(KVItem)
	counter := binary.LittleEndian.Uint64(kvc.data[kv.Off : kv.Off+8])
	timestamp := binary.LittleEndian.Uint64(kvc.data[kv.Off+TimestampOffset : kv.Off+TimestampOffset+8])
	return int64(counter), int64(timestamp)

}

func (kvc *KeyValueCounter) Snapshot(t time.Time, handler func(key CounterKey, value int64) error) error {
	kvc.lock.Lock()
	defer kvc.lock.Unlock()

	tsFilter := uint64(t.UnixMilli())

	kvc.index.Ascend(func(item btree.Item) bool {
		kv := item.(KVItem)
		ts := binary.LittleEndian.Uint64(kvc.data[kv.Off+TimestampOffset : kv.Off+TimestampOffset+8])
		if ts < tsFilter {
			return true // skip
		}
		value := binary.LittleEndian.Uint64(kvc.data[kv.Off : kv.Off+8])
		if err := handler(CounterKey(kv.Key), int64(value)); err != nil {
			return false // stop iteration on error
		}

		return true
	})

	// tree.Ascend(func(item btree.Item) bool {
	// 	kv := item.(KVItem)
	// 	if kv.Key > "users/1" {
	// 		return false // stop iteration
	// 	}
	// 	fmt.Println(kv.Key)
	// 	return true
	// })

	// 	tree.AscendRange(
	// 	KVItem{Key: "users/1/"},
	// 	KVItem{Key: "users/1/\xff"},
	// 	func(item btree.Item) bool {
	// 		kv := item.(KVItem)
	// 		fmt.Println(kv.Key)
	// 		return true
	// 	},
	// )

	// for hkey, slot := range kvc.index {
	// 	itemts := binary.LittleEndian.Uint64(kvc.data[slot*SlotSize+TimestampOffset:])
	// 	if itemts < ts {
	// 		continue
	// 	}

	// 	value := binary.LittleEndian.Uint64(kvc.data[slot*SlotSize+CounterOffset:])
	// 	strKey := strconv.FormatUint(hkey, 10)
	// 	if err := handler(strKey, value); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func (kvc *KeyValueCounter) Close() error {
	return kvc.data.Unmap()
}
