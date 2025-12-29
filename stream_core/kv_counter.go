package stream_core

import (
	"encoding/binary"
	"hash/fnv"
	"strings"
	"time"

	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

const (
	SlotSize        = 32 // 8 bytes key hash + 8 parent key hash + 8 bytes counter + 8 bytes timestamp
	MaxSlots        = 1_000_000
	ParentKeyOffset = 8
	CounterOffset   = 16
	TimestampOffset = 24
)

type CounterKey string

func (k CounterKey) Iterate() []CounterKey {
	time.Now().UnixMilli()

	keys := strings.Split(string(k), "/")

	result := []CounterKey{}

	iterstring := []string{}
	for _, item := range keys {
		iterstring = append(iterstring, item)
		key := strings.Join(iterstring, "/")
		result = append(result, CounterKey(key))

	}

	return result

}

func (k CounterKey) Hash() uint64 {
	h := fnv.New64a()
	h.Write([]byte(k))
	return h.Sum64()
}

type KeyValueCounter struct {
	lock  sync.Mutex
	data  mmap.MMap
	slots uint64
	index map[uint64]uint64
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

	return &KeyValueCounter{
		sync.Mutex{},
		m,
		cfg.KVCounterSlots,
		make(map[uint64]uint64),
		// wal,
	}, nil
}

func (kvc *KeyValueCounter) IncUint(key CounterKey, delta uint64) uint64 {

	// if writeWal {
	// 	kvc.wal.Append(&wal_message.CounterUint{
	// 		Key:   key,
	// 		Value: delta,
	// 	})
	// }
	counterKeys := key.Iterate()

	kvc.lock.Lock()
	defer kvc.lock.Unlock()

	var result uint64
	var parentKey CounterKey

	for _, ckey := range counterKeys {
		hkey := ckey.Hash()

		if elem, ok := kvc.index[hkey]; ok {
			result = atomic.AddUint64((*uint64)(unsafe.Pointer(&kvc.data[elem*SlotSize+CounterOffset])), delta)
			continue
		}

		slot := uint64(len(kvc.index))
		offset := slot * SlotSize
		binary.LittleEndian.PutUint64(kvc.data[offset:], hkey)
		binary.LittleEndian.PutUint64(kvc.data[offset+CounterOffset:], delta)

		if parentKey != "" {
			phkey := parentKey.Hash()
			binary.LittleEndian.PutUint64(kvc.data[offset+ParentKeyOffset:], phkey)
		}

		ts := uint64(time.Now().UnixMilli())
		binary.LittleEndian.PutUint64(kvc.data[offset+TimestampOffset:], ts)

		kvc.index[hkey] = slot

		parentKey = ckey
	}

	return result
}

func (kvc *KeyValueCounter) GetUint(key CounterKey) uint64 {
	hkey := key.Hash()
	kvc.lock.Lock()
	defer kvc.lock.Unlock()
	if elem, ok := kvc.index[hkey]; ok {
		value := binary.LittleEndian.Uint64(kvc.data[elem*SlotSize+CounterOffset:])
		return value
	}
	return 0
}

func (kvc *KeyValueCounter) IncInt(key string, delta int64) error {
	panic("unimplemented")
}

func (kvc *KeyValueCounter) IncFloat(key string, delta float64) error {
	panic("unimplemented")
}

func (kvc *KeyValueCounter) GetInt(key string) (int64, error) {
	panic("unimplemented")
}

func (kvc *KeyValueCounter) GetFloat(key string) (float64, error) {
	panic("unimplemented")
}

func (kvc *KeyValueCounter) Snapshot(t time.Time, handler func(keyid string, value uint64) error) error {
	kvc.lock.Lock()
	defer kvc.lock.Unlock()

	ts := uint64(t.UnixMilli())

	for hkey, slot := range kvc.index {
		itemts := binary.LittleEndian.Uint64(kvc.data[slot*SlotSize+TimestampOffset:])
		if itemts < ts {
			continue
		}

		value := binary.LittleEndian.Uint64(kvc.data[slot*SlotSize+CounterOffset:])
		strKey := strconv.FormatUint(hkey, 10)
		if err := handler(strKey, value); err != nil {
			return err
		}
	}
	return nil
}

func (kvc *KeyValueCounter) Close() error {
	// err := kvc.wal.Close()
	// if err != nil {
	// 	return err
	// }
	return kvc.data.Unmap()
}
