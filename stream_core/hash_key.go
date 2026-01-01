package stream_core

import "github.com/cespare/xxhash"

type hashKey struct {
	cfg *CoreConfig
}

func (hs *hashKey) hash(key string) int64 {
	h := xxhash.Sum64String(key)
	slot := h & (hs.cfg.HashMapCounterSlots - 1)
	return int64(slot * HASHMAP_SLOT_SIZE)
}

func (hs *hashKey) hashByte(data []byte) int64 {
	h := xxhash.Sum64(data)
	slot := h & (hs.cfg.HashMapCounterSlots - 1)
	return int64(slot * HASHMAP_SLOT_SIZE)
}
