package stream_core

import "reflect"

func (hm *HashMapCounter) IncFloat64(key string, delta float64) float64 {
	return hm.apply(key, delta, false).(float64)
}

func (hm *HashMapCounter) GetFloat64(key string) float64 {
	return hm.getCounter(reflect.Float64, key).(float64)
}

func (hm *HashMapCounter) IncUint64(key string, delta uint64) uint64 {
	return hm.apply(key, delta, false).(uint64)
}

func (hm *HashMapCounter) GetUint64(key string) uint64 {
	return hm.getCounter(reflect.Uint64, key).(uint64)
}

func (hm *HashMapCounter) IncInt64(key string, delta int64) int64 {
	return hm.apply(key, delta, false).(int64)
}

func (hm *HashMapCounter) GetInt64(key string) int64 {
	return hm.getCounter(reflect.Int64, key).(int64)
}

func (hm *HashMapCounter) PutUint64(key string, value uint64) uint64 {
	return hm.apply(key, value, true).(uint64)
}

func (hm *HashMapCounter) PutInt64(key string, value int64) int64 {
	return hm.apply(key, value, true).(int64)
}

func (hm *HashMapCounter) PutFloat64(key string, value float64) float64 {
	return hm.apply(key, value, true).(float64)
}
