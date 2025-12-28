package stream_core

import (
	"kvcounter/proto_core/wal_message/v1"
	"os"
)

type CoreConfig struct {
	WalDir           string
	WalSerialization wal_message.WalSerialization
	KVCounterPath    string
	KVCounterSlots   uint64
}

func NewDefaultCoreConfig() *CoreConfig {
	return &CoreConfig{
		WalDir:         "/tmp/stream_engine/wal",
		KVCounterPath:  "/tmp/stream_engine/kv_counter",
		KVCounterSlots: 1_000_000,
	}
}

func NewDefaultCoreConfigTest() *CoreConfig {
	return &CoreConfig{
		WalDir:         "/tmp/stream_engine/wal_test",
		KVCounterPath:  "/tmp/stream_engine/kv_counter_test",
		KVCounterSlots: 1000,
	}
}

func init() {
	os.MkdirAll("/tmp/stream_engine", 0755)
}
