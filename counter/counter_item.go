package counter

import (
	"encoding/binary"
	"log"
	"time"
)

type counter struct {
	offset uint64
	data   []byte
}

func newCounter(offset uint64, data []byte) *counter {
	if offset > 32000000 {
		log.Println(offset)
		panic("key is too long")
	}
	return &counter{offset, data}
}

func (c *counter) valueByte() []byte {
	return c.data[c.offset : c.offset+8]
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

func (c *counter) next() *counter {
	offset := binary.LittleEndian.Uint64(c.data[c.offset+16 : c.offset+24])
	if offset == 0 {
		return nil
	}
	return newCounter(offset, c.data)

}

func (c *counter) putNext(nc *counter) {
	if nc == nil {
		binary.LittleEndian.PutUint64(c.data[c.offset+16:c.offset+24], 0)
		return
	}

	if nc.offset > 32000000 {
		log.Println(nc.offset)
		return
	}

	binary.LittleEndian.PutUint64(c.data[c.offset+16:c.offset+24], nc.offset)
}

func (c *counter) prev() *counter {
	offset := binary.LittleEndian.Uint64(c.data[c.offset+24 : c.offset+32])
	if offset == 0 {
		return nil
	}

	return newCounter(offset, c.data)

}

func (c *counter) putPrev(pc *counter) {

	if pc == nil {
		binary.LittleEndian.PutUint64(c.data[c.offset+24:c.offset+32], 0)
		return
	}

	if pc.offset > 32000000 {
		log.Println(pc.offset)
		return
	}
	binary.LittleEndian.PutUint64(c.data[c.offset+24:c.offset+32], pc.offset)
}

// func (c *counter) isCounter(cc *counter) bool {
// 	return c.offset == cc.offset
// }

func (c *counter) keylen() uint32 {
	l := binary.LittleEndian.Uint32(c.data[c.offset+32 : c.offset+36])
	if l > 32000000 {
		log.Println(string(c.data[c.offset+32 : c.offset+36]))
		panic("key is too long")
	}
	return l
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
	if c.offset == 8149 {
		log.Println(klen)
	}
	c.putKeylen(uint32(klen))
	copy(c.data[c.offset+36:c.offset+36+uint64(klen)], []byte(key))
}
