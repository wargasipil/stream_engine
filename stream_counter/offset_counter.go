package stream_counter

import (
	"encoding/binary"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
)

/*
metadata
offset (8byte /64bit) | head list (8byte /64bit) | tail list (8byte /64bit) | reserved (8 byte /64bit) | body dynamic

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
	filesize int
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

	count := OffsetCounter{sync.Mutex{}, int(size), f, m}
	if isnew {
		count.putOffset(OFFSET_COUNTER_META_SIZE)
	}

	return &count, nil
}

func (c *OffsetCounter) offset() int {
	d := binary.LittleEndian.Uint64(c.data[:8])
	return int(d)
}

func (c *OffsetCounter) putOffset(offset int) {
	binary.LittleEndian.PutUint64(c.data[:8], uint64(offset))
}

func (c *OffsetCounter) head() *counter {
	offset := binary.LittleEndian.Uint64(c.data[8:16])
	if offset == 0 {
		return nil
	}

	return newCounter(int(offset), c.data)

}

func (c *OffsetCounter) putHead(cc *counter) {
	if cc == nil {
		return
	}

	if cc.offset == 0 {
		panic("invalid offset")
	}
	cc.putPrev(nil)
	binary.LittleEndian.PutUint64(c.data[8:16], uint64(cc.offset))
}

func (c *OffsetCounter) tail() *counter {

	offset := binary.LittleEndian.Uint64(c.data[16:24])
	if offset == 0 {
		return nil
	}

	return newCounter(int(offset), c.data)

}

func (c *OffsetCounter) putTail(cc *counter) {
	if cc == nil {
		return
	}

	if cc.offset == 0 {
		panic("invalid offset")
	}
	cc.putNext(nil)
	binary.LittleEndian.PutUint64(c.data[16:24], uint64(cc.offset))
}

func (c *OffsetCounter) createCounter(key string) *counter {
	offset := c.offset()

	cc := newCounter(offset, c.data)
	cc.putKey(key)

	nextOffset := offset + OFFSET_COUNTER_SIZE + len(key)
	c.putOffset(nextOffset)

	// log.Println("oldoffset", offset, "newoffset", nextOffset, "keylen", len(key), "uint", uint64(len(key)), key)

	return cc
}

func (c *OffsetCounter) NewCounter(key string) *counter {
	c.lock.Lock()
	defer c.lock.Unlock()

	size := len(key) + 2000
	nextSize := c.offset() + size

	if nextSize > int(c.filesize) {
		err := c.increaseSize()
		if err != nil {
			panic("cannot increase filesize offset key")
		}
	}

	return c.createCounter(key)
}

func (c *OffsetCounter) Offset(offset int) *counter {
	return newCounter(offset, c.data)
}

func (c *OffsetCounter) UpdateValue(offset int, value uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	cc := counter{offset, c.data}

	cc.putTimestamp(time.Now())
	cc.putValue(value)

	c.remove(&cc)
	c.append(&cc)
	// c.chain(&cc)

}

func (c *OffsetCounter) remove(n *counter) {
	prev := n.prev()
	next := n.next()

	n.putPrev(nil)
	n.putNext(nil)

	if next != nil && prev != nil {
		prev.putNext(next)
		next.putPrev(prev)
		return
	}

	if next != nil {
		next.putPrev(nil)
		return
	}

	if prev != nil {
		prev.putNext(nil)
		return
	}

}

func (c *OffsetCounter) append(n *counter) {
	tail := c.tail()

	if tail == nil {
		c.putTail(n)
		c.putHead(n)
		return
	}

	n.putPrev(tail)
	tail.putNext(n)
	c.putTail(n)

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

func (c *OffsetCounter) increaseSize() error {
	err := c.data.Flush()
	if err != nil {
		return err
	}

	err = c.data.Unmap()
	if err != nil {
		return err
	}

	c.filesize = c.filesize * 2
	err = c.f.Truncate(int64(c.filesize))
	if err != nil {
		return err
	}

	c.data, err = mmap.Map(c.f, mmap.RDWR, 0)
	if err != nil {
		return err
	}

	return nil
}

func (c *OffsetCounter) Debug(handler func() error) error {
	var err error

	startOffset := OFFSET_COUNTER_META_SIZE
	for startOffset < int(c.filesize) {
		cc := counter{startOffset, c.data}
		keylen := cc.keylen()
		if keylen == 0 {
			break
		}

		log.Println(
			"key_len", keylen,
			"startOffset", startOffset,
			"prevOffset", cc.prevOffset(),
			"nextOffset", cc.nextOffset(),
			"key", cc.key(),
		)

		if strings.Contains(cc.key(), "team_account/65/reven") {
			break
		}

		err = handler()
		if err != nil {
			return err
		}
		startOffset += OFFSET_COUNTER_SIZE + int(keylen)
	}

	dd := newCounter(23617928, c.data)
	// dd := newCounter(7888, c.data)

	log.Println("\n\n", dd.dataMap())

	return nil
}
