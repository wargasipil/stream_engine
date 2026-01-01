package stream_core

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

/*
structured dynamic value
current offset
| 8 byte offset | body dynamic

body dynamic
| 8 byte key_length | 8 byte data length | 8 byte key hash | data dynamic

structured hashmap counter
| 8 byte type_key | 8 byte pointer to dynamic value | 8 byte counter value | 8 byte for timestamp

note:
	- type_key: is counter_key or dynamic_key

*/

const (
	FILE_SIZE_INCREASE    = 1_000_000 * 5
	DYNAMIC_METADATA_SIZE = 8
	KEY_METADATA_SIZE     = 24
	KEY_LEN_OFFSET        = 0
	DATA_LEN_OFFSET       = 8
	KEY_HASH_OFFSET       = 16
	DATA_OFFSET           = 24
)

var ErrBreakDynamicRead = errors.New("break dynamic read")

type DynamicValue struct {
	filesize      int64
	currentOffset int64
	// hash          *hashKey
	f    *os.File
	lock sync.Mutex
	data mmap.MMap
}

func NewDynamicValue(cfg *CoreConfig) (*DynamicValue, error) {
	f, err := os.OpenFile(cfg.DynamicValuePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	var isnew bool
	if size == 0 {
		isnew = true
		size = FILE_SIZE_INCREASE
		err = f.Truncate(size)
		if err != nil {
			return nil, err
		}
	}

	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	var currentOffset int64
	if isnew {
		setCurrentOffset(m, 8)
		currentOffset = 8
	} else {
		currentOffset = getCurrentOffset(m)
	}

	return &DynamicValue{
		size,
		currentOffset,
		// &hashKey{cfg},
		f,
		sync.Mutex{},
		m,
	}, nil
}

func (d *DynamicValue) Iterate(handler func(key string, hash int64, data []byte) error) error {
	var offset int64 = DYNAMIC_METADATA_SIZE
	var err error

	for {
		keylen := int64(binary.LittleEndian.Uint64(d.data[offset+KEY_LEN_OFFSET : offset+KEY_LEN_OFFSET+8]))
		datalen := int64(binary.LittleEndian.Uint64(d.data[offset+DATA_LEN_OFFSET : offset+DATA_LEN_OFFSET+8]))

		nextOffset := offset + KEY_METADATA_SIZE + keylen + datalen
		// log.Println("asdasdddds", d.data[offset+KEY_LEN_OFFSET:offset+KEY_LEN_OFFSET+8])
		// log.Printf("offset %d next offset %d\n", offset, nextOffset)
		if nextOffset > d.currentOffset {
			break
		}

		key := d.data[offset+DATA_OFFSET : offset+DATA_OFFSET+keylen]
		keyhash := binary.LittleEndian.Uint64(d.data[offset+KEY_HASH_OFFSET : offset+KEY_HASH_OFFSET+8])
		data := d.data[offset+DATA_OFFSET+keylen : offset+DATA_OFFSET+keylen+datalen]

		// log.Printf("[%d] ddkey : %s %s\n", offset, string(key), data)
		err = handler(string(key), int64(keyhash), data)
		if err != nil {
			if errors.Is(err, ErrBreakDynamicRead) {
				return nil
			}
			return err
		}

		offset = nextOffset
	}

	return nil
}

func (d *DynamicValue) Get(offset int64) (string, []byte) {
	keylenbin := d.data[offset+KEY_LEN_OFFSET : offset+KEY_LEN_OFFSET+8]
	keylen := int64(binary.LittleEndian.Uint64(keylenbin))

	dlenbin := d.data[offset+DATA_LEN_OFFSET : offset+DATA_LEN_OFFSET+8]

	dlen := int64(binary.LittleEndian.Uint64(dlenbin))

	key := d.data[offset+DATA_OFFSET : offset+DATA_OFFSET+keylen]
	data := d.data[offset+DATA_OFFSET+keylen : offset+DATA_OFFSET+keylen+dlen]
	return string(key), data
}

func (d *DynamicValue) GetData(offset int64) []byte {
	return d.data[offset+DATA_LEN_OFFSET : offset+DATA_LEN_OFFSET+8]
}

func (d *DynamicValue) Write(key string, keyhash int64, data []byte) (int64, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	currentoffset := d.currentOffset
	// log.Println(currentoffset)

	keylen := int64(len(key))
	datalen := int64(len(data))

	if d.currentOffset+keylen+datalen+10000 > d.filesize {
		err := d.increaseSize()
		if err != nil {
			return 0, err
		}
	}

	binary.LittleEndian.PutUint64(d.data[d.currentOffset+KEY_LEN_OFFSET:d.currentOffset+KEY_LEN_OFFSET+8], uint64(keylen))
	binary.LittleEndian.PutUint64(d.data[d.currentOffset+DATA_LEN_OFFSET:d.currentOffset+DATA_LEN_OFFSET+8], uint64(datalen))
	binary.LittleEndian.PutUint64(d.data[d.currentOffset+KEY_HASH_OFFSET:d.currentOffset+KEY_HASH_OFFSET+8], uint64(keyhash))
	d.currentOffset += KEY_METADATA_SIZE

	// writing key
	for _, b := range []byte(key) {
		d.data[d.currentOffset] = b
		d.currentOffset++

	}

	for _, b := range data {
		d.data[d.currentOffset] = b
		d.currentOffset++
	}

	setCurrentOffset(d.data, d.currentOffset)
	return currentoffset, nil
}

func (d *DynamicValue) increaseSize() error {
	err := d.data.Flush()
	if err != nil {
		return err
	}

	err = d.data.Unmap()
	if err != nil {
		return err
	}

	d.filesize += FILE_SIZE_INCREASE
	err = d.f.Truncate(d.filesize)
	if err != nil {
		return err
	}

	d.data, err = mmap.Map(d.f, mmap.RDWR, 0)
	if err != nil {
		return err
	}

	return nil
}

func (d *DynamicValue) Close() error {
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

func getCurrentOffset(m mmap.MMap) int64 {
	offset := m[0:8]
	return int64(binary.LittleEndian.Uint64(offset))
}

func setCurrentOffset(m mmap.MMap, offset int64) {
	binary.LittleEndian.PutUint64(m[0:8], uint64(offset))
}
