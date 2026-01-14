package stream_core

import "time"

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
	UpdatedKey(last time.Time, handler func(key string) error) error
}
