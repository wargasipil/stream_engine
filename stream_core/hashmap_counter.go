package stream_core

import (
	"encoding/binary"
	"math"
	"os"
	"reflect"
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
| 1 byte for data_type | 8 byte type_key | 8 byte pointer to dynamic value | 8 byte counter value | 8 byte for timestamp

note:
	- type_key: is counter_key or dynamic_key
	- data_type: type counter like float64 or int64 or uint64

*/

const (
	HASHMAP_SLOT_SIZE           = 33
	HASHMAP_TYPE_COUNTER_OFFSET = 0
	HASHMAP_METADATA_SIZE       = 9
	TYPE_KEY_OFFSET             = 1
	KEY_POINTER_OFFSET          = 8
	COUNTER_OFFSET              = 17
	TIMESTAMP_OFFSET            = 25
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

func (d *HashMapCounter) Close() error {
	err := d.dynamicValue.Close()
	if err != nil {
		return err
	}

	err = d.data.Flush()
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

func (hm *HashMapCounter) Snapshot(t time.Time, handler func(key string, kind reflect.Kind, value any) error) error {
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

		// getting value
		value := binary.LittleEndian.Uint64(hm.data[offset+COUNTER_OFFSET : offset+COUNTER_OFFSET+8])

		// getting type key
		typeKey := reflect.Kind(hm.data[offset+HASHMAP_TYPE_COUNTER_OFFSET])
		switch typeKey {
		case reflect.Uint64:
			err = handler(key, reflect.Uint64, value)
		case reflect.Int64:
			err = handler(key, reflect.Int64, int64(value))
		case reflect.Float64:
			err = handler(key, reflect.Float64, math.Float64frombits(value))
		default:
			err = handler(key, reflect.Uint64, value)
		}

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
