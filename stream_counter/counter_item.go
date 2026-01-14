package stream_counter

import (
	"encoding/binary"
	"time"
)

type counter struct {
	offset int
	data   []byte
}

func newCounter(offset int, data []byte) *counter {

	cc := &counter{offset, data}
	return cc
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
	binary.LittleEndian.PutUint64(c.data[c.offset+8:c.offset+16], uint64(d))
}

func (c *counter) nextOffset() int {
	offset := binary.LittleEndian.Uint64(c.data[c.offset+16 : c.offset+24])
	return int(offset)

}

func (c *counter) next() *counter {
	offset := int(binary.LittleEndian.Uint64(c.data[c.offset+16 : c.offset+24]))
	if offset == 0 {
		return nil
	}
	return newCounter(offset, c.data)

}

func (c *counter) putNext(nc *counter) {
	if nc == nil {
		var zero int = 0
		binary.LittleEndian.PutUint64(c.data[c.offset+16:c.offset+24], uint64(zero))
		return
	}

	binary.LittleEndian.PutUint64(c.data[c.offset+16:c.offset+24], uint64(nc.offset))
}

func (c *counter) prevOffset() int {
	offset := binary.LittleEndian.Uint64(c.data[c.offset+24 : c.offset+32])
	return int(offset)

}

func (c *counter) prev() *counter {
	offset := int(binary.LittleEndian.Uint64(c.data[c.offset+24 : c.offset+32]))
	if offset == 0 {
		return nil
	}

	return newCounter(offset, c.data)

}

func (c *counter) putPrev(pc *counter) {

	if pc == nil {
		var zero int = 0
		binary.LittleEndian.PutUint64(c.data[c.offset+24:c.offset+32], uint64(zero))
		return
	}

	binary.LittleEndian.PutUint64(c.data[c.offset+24:c.offset+32], uint64(pc.offset))
}

// func (c *counter) isCounter(cc *counter) bool {
// 	return c.offset == cc.offset
// }

func (c *counter) keylen() int {
	l := binary.LittleEndian.Uint32(c.data[c.offset+32 : c.offset+36])

	return int(l)
}

func (c *counter) putKeylen(l uint32) {
	binary.LittleEndian.PutUint32(c.data[c.offset+32:c.offset+36], l)
}

func (c *counter) key() string {
	klen := c.keylen()
	data := c.data[c.offset+36 : c.offset+36+int(klen)]
	return string(data)
}

func (c *counter) putKey(key string) {
	klen := len(key)
	if c.keylen() != 0 {
		panic("key is not empty maybe overwrited")
	}
	c.putKeylen(uint32(klen))

	off := c.offset + OFFSET_COUNTER_SIZE
	for i, v := range []byte(key) {
		c.data[off+i] = v
	}

	// copy(c.data[:c.offset+36+uint64(klen)], []byte(key))
}

func (c *counter) dataMap() map[string]any {
	return map[string]any{
		"offset":    c.offset,
		"value":     c.value(),
		"timestamp": c.timestamp(),
		"next":      c.nextOffset(),
		"prev":      c.prevOffset(),
		"keylen":    c.keylen(),
		"key":       c.key(),
	}
}
