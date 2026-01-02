package stream_core

func (hm *HashMapCounter) IncFloat64(key string, delta float64) float64 {
	return hm.apply(key, delta).(float64)
}

func (hm *HashMapCounter) GetFloat64(key string) float64 {
	return hm.getCounter(key).(float64)
}

func (hm *HashMapCounter) IncUint64(key string, delta uint64) uint64 {
	return hm.apply(key, delta).(uint64)
}

func (hm *HashMapCounter) GetUint64(key string) uint64 {
	return hm.getCounter(key).(uint64)
}

func (hm *HashMapCounter) IncInt64(key string, delta int64) int64 {
	return hm.apply(key, delta).(int64)
}

func (hm *HashMapCounter) GetInt64(key string) int64 {
	return hm.getCounter(key).(int64)
}
