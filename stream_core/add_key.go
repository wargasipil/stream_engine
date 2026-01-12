package stream_core

import (
	"fmt"
	"reflect"
)

type Transaction struct {
	hm *HashMapCounter
}

func (hm *HashMapCounter) Transaction(handler func(tx *Transaction) error) error {
	hm.lock.Lock()
	defer hm.lock.Unlock()

	tx := Transaction{hm}

	return handler(&tx)
}

func (tx *Transaction) IncFloat64(key string, delta float64) float64 {
	return tx.hm.apply(key, delta, false).(float64)
}

func (tx *Transaction) IncUint64(key string, delta uint64) uint64 {
	return tx.hm.apply(key, delta, false).(uint64)
}

func (tx *Transaction) IncInt64(key string, delta int64) int64 {
	return tx.hm.apply(key, delta, false).(int64)
}

func (tx *Transaction) PutUint64(key string, value uint64) uint64 {
	return tx.hm.apply(key, value, true).(uint64)
}

func (tx *Transaction) PutInt64(key string, value int64) int64 {
	return tx.hm.apply(key, value, true).(int64)
}

func (tx *Transaction) PutFloat64(key string, value float64) float64 {
	return tx.hm.apply(key, value, true).(float64)
}

func (tx *Transaction) GetFloat64(key string) float64 {
	return tx.hm.getCounter(reflect.Float64, key).(float64)
}

func (tx *Transaction) GetUint64(key string) uint64 {
	return tx.hm.getCounter(reflect.Uint64, key).(uint64)
}

func (tx *Transaction) GetInt64(key string) int64 {
	return tx.hm.getCounter(reflect.Int64, key).(int64)
}

// ---------------------- bagian hashmap ----------------------------------

func (hm *HashMapCounter) GetFloat64(key string) float64 {
	return hm.getCounter(reflect.Float64, key).(float64)
}

func (hm *HashMapCounter) PrintFloat64(key string) float64 {
	val := hm.getCounter(reflect.Float64, key).(float64)
	fmt.Printf("%s: %.3f\n", key, val)
	return val
}

func (hm *HashMapCounter) GetUint64(key string) uint64 {
	return hm.getCounter(reflect.Uint64, key).(uint64)
}

func (hm *HashMapCounter) GetInt64(key string) int64 {
	return hm.getCounter(reflect.Int64, key).(int64)
}

type KeyStore interface {
	GetInt64(key string) int64
	GetUint64(key string) uint64
	GetFloat64(key string) float64
	PutInt64(key string, value int64) int64
	PutUint64(key string, value uint64) uint64
	PutFloat64(key string, value float64) float64
	IncInt64(key string, delta int64) int64
	IncUint64(key string, delta uint64) uint64
	IncFloat64(key string, delta float64) float64
	Close() error
	ResetCounter() error
	// Snapshot()
}
