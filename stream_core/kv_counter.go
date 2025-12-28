package stream_core

import (
	"encoding/binary"
	"hash/fnv"
	"kvcounter/proto_core/wal_message/v1"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

const (
	SlotSize = 16 // 8 bytes key hash + 8 bytes counter
	MaxSlots = 1_000_000
)

type KeyValueCounter struct {
	lock  sync.Mutex
	data  mmap.MMap
	slots uint64
	index map[uint64]uint64
	wal   *WAL
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

	// creating WAL
	wal, err := OpenWAL(cfg.WalDir)
	if err != nil {
		return nil, err
	}

	return &KeyValueCounter{
		sync.Mutex{},
		m,
		cfg.KVCounterSlots,
		make(map[uint64]uint64),
		wal,
	}, nil
}

func (kvc *KeyValueCounter) IncUint(key string, delta uint64) uint64 {

	kvc.wal.Append(&wal_message.CounterUint{
		Key:   key,
		Value: delta,
	})
	hkey := hashKey(key)

	kvc.lock.Lock()
	defer kvc.lock.Unlock()

	if elem, ok := kvc.index[hkey]; ok {
		return atomic.AddUint64((*uint64)(unsafe.Pointer(&kvc.data[elem*SlotSize+8])), delta)
	}

	slot := uint64(len(kvc.index))
	offset := slot * SlotSize
	binary.LittleEndian.PutUint64(kvc.data[offset:], hkey)
	binary.LittleEndian.PutUint64(kvc.data[offset+8:], delta)

	kvc.index[hkey] = slot

	return delta
}

func (kvc *KeyValueCounter) GetUint(key string) uint64 {
	hkey := hashKey(key)
	kvc.lock.Lock()
	defer kvc.lock.Unlock()
	if elem, ok := kvc.index[hkey]; ok {
		value := binary.LittleEndian.Uint64(kvc.data[elem*SlotSize+8:])
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

func (kvc *KeyValueCounter) Snapshot(handler func(keyid string, value uint64) error) error {
	kvc.lock.Lock()
	defer kvc.lock.Unlock()

	for hkey, slot := range kvc.index {
		value := binary.LittleEndian.Uint64(kvc.data[slot*SlotSize+8:])
		strKey := strconv.FormatUint(hkey, 10)
		if err := handler(strKey, value); err != nil {
			return err
		}
	}
	return nil
}

func (kvc *KeyValueCounter) Close() error {
	err := kvc.wal.Close()
	if err != nil {
		return err
	}
	return kvc.data.Unmap()
}

func hashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}
