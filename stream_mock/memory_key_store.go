package stream_mock

import (
	"time"
)

type InMemoryKeyStore struct {
	data map[string]any
}

// Close implements stream_core.KeyStore.
func (i *InMemoryKeyStore) Close() error {
	return nil
}

// GetFloat64 implements stream_core.KeyStore.
func (i *InMemoryKeyStore) GetFloat64(key string) float64 {
	if i.data[key] == nil {
		return 0
	}
	return i.data[key].(float64)
}

// GetInt64 implements stream_core.KeyStore.
func (i *InMemoryKeyStore) GetInt64(key string) int64 {
	if i.data[key] == nil {
		return 0
	}
	return i.data[key].(int64)
}

// GetUint64 implements stream_core.KeyStore.
func (i *InMemoryKeyStore) GetUint64(key string) uint64 {
	if i.data[key] == nil {
		return 0
	}
	return i.data[key].(uint64)
}

// IncFloat64 implements stream_core.KeyStore.
func (i *InMemoryKeyStore) IncFloat64(key string, delta float64) float64 {
	return i.PutFloat64(key, i.GetFloat64(key)+delta)
}

// IncInt64 implements stream_core.KeyStore.
func (i *InMemoryKeyStore) IncInt64(key string, delta int64) int64 {
	return i.PutInt64(key, i.GetInt64(key)+delta)
}

// IncUint64 implements stream_core.KeyStore.
func (i *InMemoryKeyStore) IncUint64(key string, delta uint64) uint64 {
	return i.PutUint64(key, i.GetUint64(key)+delta)
}

// PutFloat64 implements stream_core.KeyStore.
func (i *InMemoryKeyStore) PutFloat64(key string, value float64) float64 {
	i.data[key] = value
	return i.data[key].(float64)
}

// PutInt64 implements stream_core.KeyStore.
func (i *InMemoryKeyStore) PutInt64(key string, value int64) int64 {
	i.data[key] = value
	return i.data[key].(int64)
}

// PutUint64 implements stream_core.KeyStore.
func (i *InMemoryKeyStore) PutUint64(key string, value uint64) uint64 {
	i.data[key] = value
	return i.data[key].(uint64)
}

// ResetCounter implements stream_core.KeyStore.
func (i *InMemoryKeyStore) ResetCounter() error {
	panic("unimplemented")
}

// UpdatedKey implements stream_core.KeyStore.
func (i *InMemoryKeyStore) UpdatedKey(last time.Time, handler func(key string) error) error {
	var err error
	for key := range i.data {
		err = handler(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewInMemoryKeyStore() *InMemoryKeyStore {
	return &InMemoryKeyStore{
		data: make(map[string]any),
	}
}
