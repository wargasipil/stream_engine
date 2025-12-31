package stream_core

import (
	"os"

	"github.com/wargasipil/stream_engine/proto_core/wal_message/v1"
)

type CoreConfig struct {
	WalDir              string
	WalSerialization    wal_message.WalSerialization
	KVCounterPath       string
	KVCounterSlots      uint64
	HashMapCounterPath  string
	HashMapCounterSlots uint64
	DynamicValuePath    string
}

func NewDefaultCoreConfig() *CoreConfig {
	return &CoreConfig{
		WalDir:              "/tmp/stream_engine/wal",
		KVCounterPath:       "/tmp/stream_engine/kv_counter",
		HashMapCounterPath:  "/tmp/stream_engine/hm_counter_test",
		HashMapCounterSlots: 100_000_000,
		DynamicValuePath:    "/tmp/stream_engine/dynamic_value",
		KVCounterSlots:      100_000_000,
	}
}

func NewDefaultCoreConfigTest() *CoreConfig {
	return &CoreConfig{
		WalDir:              "/tmp/stream_engine/wal_test",
		KVCounterPath:       "/tmp/stream_engine/kv_counter_test",
		HashMapCounterPath:  "/tmp/stream_engine/hm_counter_test",
		HashMapCounterSlots: 1000,
		DynamicValuePath:    "/tmp/stream_engine/dynamic_value_test",
		KVCounterSlots:      1000,
	}
}

func init() {
	os.MkdirAll("/tmp/stream_engine", 0755)
}
