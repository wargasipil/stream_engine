package counter

import (
	"encoding/binary"
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
)

/*
metadata
offset (8byte /64bit) | lastUpdatedOffset (8byte /64bit) | reserved (16byte /64bit) | body dynamic

body counter
counter (8byte /64bit) | timestamp (8byte /64bit) | next (8byte /64bit) | prev (8byte /64bit) | keylen (4 byte /32bit) | keystring


*/

const (
	OFFSET_COUNTER_META_SIZE = 32
	OFFSET_COUNTER_SIZE      = 36
	OFFSET_START_SIZE        = 4 * 1_000_000 // 4MB
)

type OffsetCounter struct {
	lock     sync.Mutex
	filesize int64
	f        *os.File
	data     mmap.MMap
}

func NewOffsetCounter(fname string) (*OffsetCounter, error) {
	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	var isnew bool
	size := info.Size()

	if size == 0 {
		isnew = true
		size = OFFSET_START_SIZE
		err = f.Truncate(size)
		if err != nil {
			return nil, err
		}
	}

	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	count := OffsetCounter{sync.Mutex{}, size, f, m}
	if isnew {
		count.putOffset(OFFSET_COUNTER_META_SIZE)
		count.putLastUpdated(&counter{
			offset: OFFSET_COUNTER_META_SIZE,
			data:   count.data,
		})
	}

	return &count, nil
}

func (c *OffsetCounter) offset() uint64 {
	d := binary.LittleEndian.Uint64(c.data[:8])
	return d
}

func (c *OffsetCounter) putOffset(offset uint64) {
	binary.LittleEndian.PutUint64(c.data[:8], offset)
}

func (c *OffsetCounter) lastUpdated() *counter {
	offset := binary.LittleEndian.Uint64(c.data[8:16])
	return &counter{
		offset: offset,
		data:   c.data,
	}
}

func (c *OffsetCounter) putLastUpdated(cc *counter) {
	binary.LittleEndian.PutUint64(c.data[8:16], cc.offset)
}

func (c *OffsetCounter) nextOffset(size uint64) uint64 {
	offset := c.offset()

	nextOffset := offset + size
	c.putOffset(nextOffset)
	return offset
}

func (c *OffsetCounter) NewCounter(key string) *counter {
	c.lock.Lock()
	defer c.lock.Unlock()

	size := len(key) + OFFSET_COUNTER_SIZE
	return &counter{c.nextOffset(uint64(size)), c.data}
}

func (c *OffsetCounter) Offset(offset uint64) *counter {
	return &counter{offset, c.data}
}

func (c *OffsetCounter) UpdateValue(offset uint64, value uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	cc := counter{offset, c.data}

	cc.putTimestamp(time.Now())
	cc.putValue(value)

	// change previous
	if cc.next() != nil {
		cc.prev().putNext(cc.next())
	} else {
		cc.prev().putNext(c.lastUpdated())
	}

	// change current counter previous
	cc.putPrev(c.lastUpdated())
	c.putLastUpdated(&cc)

}

func (c *OffsetCounter) Close() error {
	err := c.data.Flush()
	if err != nil {
		return err
	}

	err = c.data.Unmap()
	if err != nil {
		return err
	}

	err = c.f.Close()
	return err
}

// -------------------------- counter data ---------------------------------------

type counter struct {
	offset uint64
	data   []byte
}

func (c *counter) value() uint64 {
	return binary.LittleEndian.Uint64(c.data[c.offset : c.offset+8])
}

func (c *counter) putValue(value uint64) {
	binary.LittleEndian.PutUint64(c.data[c.offset:c.offset+8], value)
}

func (c *counter) timestamp() time.Time {
	micro := binary.LittleEndian.Uint64(c.data[c.offset+8 : c.offset+16])
	return time.UnixMicro(int64(micro))
}

func (c *counter) putTimestamp(t time.Time) {
	d := t.UnixMicro()
	binary.LittleEndian.PutUint64(c.data[c.offset:c.offset+8], uint64(d))
}

func (c *counter) next() *counter {
	offset := binary.LittleEndian.Uint64(c.data[c.offset+16 : c.offset+24])
	if offset == 0 {
		return nil
	}
	return &counter{
		offset: offset,
		data:   c.data,
	}
}

func (c *counter) putNext(nc *counter) {
	binary.LittleEndian.PutUint64(c.data[c.offset+16:c.offset+24], nc.offset)
}

func (c *counter) prev() *counter {
	offset := binary.LittleEndian.Uint64(c.data[c.offset+24 : c.offset+32])
	return &counter{
		offset: offset,
		data:   c.data,
	}
}

func (c *counter) putPrev(pc *counter) {
	binary.LittleEndian.PutUint64(c.data[c.offset+24:c.offset+32], pc.offset)
}

func (c *counter) keylen() uint32 {
	return binary.LittleEndian.Uint32(c.data[c.offset+32 : c.offset+36])
}

func (c *counter) putKeylen(l uint32) {
	binary.LittleEndian.PutUint32(c.data[c.offset+32:c.offset+36], l)
}

func (c *counter) key() string {
	klen := c.keylen()
	data := c.data[c.offset+36 : c.offset+36+uint64(klen)]
	return string(data)
}

func (c *counter) putKey(key string) {
	klen := len(key)
	c.putKeylen(uint32(klen))
	copy(c.data[c.offset+36:c.offset+36+uint64(klen)], []byte(key))
}
