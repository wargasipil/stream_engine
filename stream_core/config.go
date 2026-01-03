package stream_core

import (
	"os"
)

type CoreConfig struct {
	// WalDir           string
	// WalSerialization wal_message.WalSerialization

	HashMapCounterPath string
	// must n^2 for the size
	HashMapCounterSlots uint64
	DynamicValuePath    string
}

func NewDefaultCoreConfig() *CoreConfig {
	return &CoreConfig{
		// WalDir:              "/tmp/stream_engine/wal",
		HashMapCounterPath:  "/tmp/stream_engine/hm_counter",
		HashMapCounterSlots: 536_870_912,
		DynamicValuePath:    "/tmp/stream_engine/dynamic_value",
	}
}

func NewDefaultCoreConfigTest() *CoreConfig {
	return &CoreConfig{
		// WalDir:              "/tmp/stream_engine/wal_test",
		HashMapCounterPath:  "/tmp/stream_engine/hm_counter_test",
		HashMapCounterSlots: 32,
		DynamicValuePath:    "/tmp/stream_engine/dynamic_value_test",
	}
}

func init() {
	os.MkdirAll("/tmp/stream_engine", 0755)
}
