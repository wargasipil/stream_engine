package stream_core

import (
	"encoding/binary"
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
)

/*
structured dynamic value
current offset
| 8 byte offset | body dynamic

body dynamic
| 8 byte key_length | 8 byte data legth | data dynamic

structured hashmap counter metadata
| 8 byte key_count | hashmap data

structured hashmap counter
| 8 byte type_key | 8 byte pointer to dynamic value | 8 byte counter value | 8 byte for timestamp

note:
	- type_key: is counter_key or dynamic_key

*/

const (
	HASHMAP_SLOT_SIZE     = 32
	HASHMAP_METADATA_SIZE = 8
	TYPE_KEY_OFFSET       = 0
	KEY_POINTER_OFFSET    = 8
	COUNTER_OFFSET        = 16
	TIMESTAMP_OFFSET      = 24
)

const (
	UnknownKeyType = iota
	CounterKeyType
	MergeKeyType
	DynamicKeyType
)

type HashMapCounter struct {
	lock         sync.Mutex
	hash         *hashKey
	dynamicValue *DynamicValue
	f            *os.File
	data         mmap.MMap
	keyCount     uint64
}

func NewHashMapCounter(cfg *CoreConfig) (*HashMapCounter, error) {
	size := int(cfg.HashMapCounterSlots*HASHMAP_SLOT_SIZE) + HASHMAP_METADATA_SIZE

	f, err := os.OpenFile(cfg.HashMapCounterPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
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

	dynamic, err := NewDynamicValue(cfg)
	if err != nil {
		return nil, err
	}

	hash := &hashKey{cfg}

	fsize := info.Size()
	var isnew bool
	if fsize == 0 {
		isnew = true
	}

	var currKeyCount uint64
	if isnew {
		currKeyCount = 0
		setCurrentCount(m, currKeyCount)

	} else {
		currKeyCount = getCurrentCount(m)
	}

	return &HashMapCounter{
		sync.Mutex{},
		hash,
		dynamic,
		f,
		m,
		currKeyCount,
	}, nil
}

func (hm *HashMapCounter) IncInt(key string, delta int64) int64 {

	hm.lock.Lock()
	defer hm.lock.Unlock()

	t := time.Now().UnixMilli()
	ts := uint64(t)
	hkey := hm.hash.hash(key)
	offset := hkey + HASHMAP_METADATA_SIZE // offset + current count metadata
	// log.Printf("offset %d\n", offset)

	lastts := binary.LittleEndian.Uint64(hm.data[offset+TIMESTAMP_OFFSET : offset+TIMESTAMP_OFFSET+8])
	// log.Println("inc", time.UnixMilli(int64(lastts)).String())
	if lastts == 0 {
		hm.keyCount += 1
		setCurrentCount(hm.data, hm.keyCount)

		// set timestamp
		binary.LittleEndian.PutUint64(hm.data[offset+TIMESTAMP_OFFSET:offset+TIMESTAMP_OFFSET+8], uint64(ts))
		// set counter
		binary.LittleEndian.PutUint64(hm.data[offset+COUNTER_OFFSET:offset+COUNTER_OFFSET+8], uint64(delta))
		// set type key
		binary.LittleEndian.PutUint64(hm.data[offset+TYPE_KEY_OFFSET:offset+TYPE_KEY_OFFSET+8], uint64(CounterKeyType))
		// set key pointer
		var counter byte = CounterKeyType
		keyOffset, err := hm.dynamicValue.Write(key, hkey, []byte{counter})
		if err != nil {
			panic(err)
		}
		binary.LittleEndian.PutUint64(hm.data[offset+KEY_POINTER_OFFSET:offset+KEY_POINTER_OFFSET+8], uint64(keyOffset))
		return delta
	}

	// jika sudah ada
	binary.LittleEndian.PutUint64(hm.data[offset+TIMESTAMP_OFFSET:offset+TIMESTAMP_OFFSET+8], uint64(ts))
	// counter := binary.LittleEndian.Uint64(hm.data[offset+COUNTER_OFFSET : offset+COUNTER_OFFSET+8])
	prevVal := int64(binary.LittleEndian.Uint64(hm.data[offset+COUNTER_OFFSET : offset+COUNTER_OFFSET+8]))
	nextVal := prevVal + delta
	binary.LittleEndian.PutUint64(hm.data[offset+COUNTER_OFFSET:offset+COUNTER_OFFSET+8], uint64(nextVal))
	// log.Println(prevVal, "-->", nextVal)
	return nextVal
}

func (hm *HashMapCounter) GetInt(key string) int64 {
	offset := hm.hash.hash(key) + HASHMAP_METADATA_SIZE
	counter := binary.LittleEndian.Uint64(hm.data[offset+COUNTER_OFFSET : offset+COUNTER_OFFSET+8])
	return int64(counter)
}

func (d *HashMapCounter) Close() error {
	err := d.data.Flush()
	if err != nil {
		return err
	}

	err = d.data.Unmap()
	if err != nil {
		return err
	}

	err = d.f.Close()
	return err
}

func (hm *HashMapCounter) Snapshot(t time.Time, handler func(key string, value int64) error) error {
	var err error

	hm.lock.Lock()
	defer hm.lock.Unlock()

	tsFilter := uint64(t.UnixMilli())
	err = hm.dynamicValue.Iterate(func(key string, khash int64, data []byte) error {
		// log.Println(khash, "offset hash")
		offset := khash + HASHMAP_METADATA_SIZE
		ts := binary.LittleEndian.Uint64(hm.data[offset+TIMESTAMP_OFFSET : offset+TIMESTAMP_OFFSET+8])

		if ts < tsFilter {
			// log.Println(key, time.UnixMilli(int64(ts)).String())
			return nil
		}

		value := binary.LittleEndian.Uint64(hm.data[offset+COUNTER_OFFSET : offset+COUNTER_OFFSET+8])
		err = handler(key, int64(value))

		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func getCurrentCount(m mmap.MMap) uint64 {
	offset := m[0:8]
	return binary.LittleEndian.Uint64(offset)
}

func setCurrentCount(m mmap.MMap, count uint64) {
	binary.LittleEndian.PutUint64(m[0:8], count)
}
