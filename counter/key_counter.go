package counter

import (
	"math"
	"path"
	"time"

	"github.com/wargasipil/stream_engine/beetree"
)

type KeyCounter struct {
	cdata *OffsetCounter
	index *beetree.BeeTree
}

func NewKeyCounter(datadir string) *KeyCounter {
	offsetCounter, err := NewOffsetCounter(path.Join(datadir, "counter_data"))
	if err != nil {
		panic(err)
	}

	index, err := beetree.NewBeeTree(path.Join(datadir, "counter_key"))
	if err != nil {
		panic(err)
	}

	return &KeyCounter{offsetCounter, index}
}

// ResetCounter implements stream_core.KeyStore.
func (k *KeyCounter) ResetCounter() error {
	panic("not implemented") // TODO: Implement)
}

// PutInt64 implements stream_core.KeyStore.
func (k *KeyCounter) PutInt64(key string, value int64) int64 {
	var offset int

	existOffset, ok := k.index.Get([]byte(key))

	if !ok {
		counter := k.cdata.NewCounter(key)
		k.index.Insert(key, uint64(counter.offset))
		offset = counter.offset
	} else {
		offset = int(existOffset)
	}

	k.cdata.UpdateValue(offset, uint64(value))
	return value
}

// GetInt64 implements stream_core.KeyStore.
func (k *KeyCounter) GetInt64(key string) int64 {
	offset, ok := k.index.Get([]byte(key))
	if !ok {
		return 0
	}

	v := k.cdata.Offset(int(offset)).value()
	return int64(v)
}

// GetFloat64 implements stream_core.KeyStore.
func (k *KeyCounter) GetFloat64(key string) float64 {
	offset, ok := k.index.Get([]byte(key))
	if !ok {
		return 0
	}

	v := k.cdata.Offset(int(offset)).value()
	return math.Float64frombits(v)
}

// GetUint64 implements stream_core.KeyStore.
func (k *KeyCounter) GetUint64(key string) uint64 {
	offset, ok := k.index.Get([]byte(key))
	if !ok {
		return 0
	}

	v := k.cdata.Offset(int(offset)).value()
	return v
}

// IncFloat64 implements stream_core.KeyStore.
func (k *KeyCounter) IncFloat64(key string, delta float64) float64 {
	old := k.GetFloat64(key)
	new := old + delta
	k.PutFloat64(key, new)
	return new
}

// IncInt64 implements stream_core.KeyStore.
func (k *KeyCounter) IncInt64(key string, delta int64) int64 {
	old := k.GetInt64(key)
	new := old + delta
	k.PutInt64(key, new)
	return new
}

// IncUint64 implements stream_core.KeyStore.
func (k *KeyCounter) IncUint64(key string, delta uint64) uint64 {
	old := k.GetUint64(key)
	new := old + delta
	k.PutUint64(key, new)
	return new
}

// PutFloat64 implements stream_core.KeyStore.
func (k *KeyCounter) PutFloat64(key string, value float64) float64 {
	var offset int

	existOffset, ok := k.index.Get([]byte(key))

	if !ok {
		counter := k.cdata.NewCounter(key)
		k.index.Insert(key, uint64(counter.offset))
		offset = counter.offset
	} else {
		offset = int(existOffset)
	}

	// log.Println("offset key", offset)

	uval := math.Float64bits(value)
	k.cdata.UpdateValue(offset, uval)
	return value
}

// PutUint64 implements stream_core.KeyStore.
func (k *KeyCounter) PutUint64(key string, value uint64) uint64 {
	var offset int

	existOffset, ok := k.index.Get([]byte(key))

	if !ok {
		counter := k.cdata.NewCounter(key)
		k.index.Insert(key, uint64(counter.offset))
		offset = counter.offset
	} else {
		offset = int(existOffset)
	}
	k.cdata.UpdateValue(offset, value)
	return value
}

func (k *KeyCounter) Close() error {
	err := k.index.Close()
	if err != nil {
		return err
	}

	err = k.cdata.Close()
	if err != nil {
		return err
	}
	return nil
}

func (k *KeyCounter) LastUpdated(start time.Time, handler func(key string, value uint64) error) error {
	var err error
	c := k.cdata.tail()
	// head := k.cdata.head()
	// log.Println("tail", c.key(), "head", head.key())

	for c != nil {
		// log.Println(c.key(), c.timestamp(), c.value())
		prev := c.prev()
		if prev == nil {
			break
		}

		err = handler(c.key(), c.value())
		if err != nil {
			return err
		}

		// log.Println("prev", prev.key(), prev.offset)
		// next := c.next()
		// if next != nil {
		// 	log.Println("next", next.key(), next.offset)
		// }

		c = prev

		// time.Sleep(time.Second)
	}

	return nil
}
