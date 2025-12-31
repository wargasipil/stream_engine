package stream_core

import "github.com/cespare/xxhash"

type hashKey struct {
	cfg *CoreConfig
}

func (hs *hashKey) hash(key string) int64 {

	h := xxhash.Sum64String(key)
	slot := h & (hs.cfg.KVCounterSlots - 1)
	return int64(slot * SlotSize)
}
