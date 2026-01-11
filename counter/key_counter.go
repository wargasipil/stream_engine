package counter

import (
	"path"

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

// PutInt64 implements stream_core.KeyStore.
func (k *KeyCounter) PutInt64(key string, value int64) int64 {
	var offset uint64

	offset, ok := k.index.Get([]byte(key))

	if !ok {
		counter := k.cdata.NewCounter(key)
		counter.putKey(key)
		k.index.Insert(key, counter.offset)
		offset = counter.offset
	}

	k.cdata.UpdateValue(offset, uint64(value))
	return value
}

// GetInt64 implements stream_core.KeyStore.
func (k *KeyCounter) GetInt64(key string) int64 {
	var offset uint64

	offset, ok := k.index.Get([]byte(key))
	if !ok {
		return 0
	}

	v := k.cdata.Offset(offset).value()
	return int64(v)
}

// GetFloat64 implements stream_core.KeyStore.
func (k *KeyCounter) GetFloat64(key string) float64 {
	panic("unimplemented")
}

// GetUint64 implements stream_core.KeyStore.
func (k *KeyCounter) GetUint64(key string) uint64 {
	panic("unimplemented")
}

// IncFloat64 implements stream_core.KeyStore.
func (k *KeyCounter) IncFloat64(key string, delta float64) float64 {
	panic("unimplemented")
}

// IncInt64 implements stream_core.KeyStore.
func (k *KeyCounter) IncInt64(key string, delta int64) int64 {
	panic("unimplemented")
}

// IncUint64 implements stream_core.KeyStore.
func (k *KeyCounter) IncUint64(key string, delta uint64) uint64 {
	panic("unimplemented")
}

// PutFloat64 implements stream_core.KeyStore.
func (k *KeyCounter) PutFloat64(key string, value float64) float64 {
	panic("unimplemented")
}

// PutUint64 implements stream_core.KeyStore.
func (k *KeyCounter) PutUint64(key string, value uint64) uint64 {
	panic("unimplemented")
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
