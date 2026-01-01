package main

import (
	"log"
	"time"

	"github.com/wargasipil/stream_engine/stream_core"
)

func main() {
	cfg := stream_core.NewDefaultCoreConfigTest()
	cfg.HashMapCounterSlots = 100

	before := time.Now()

	kv, err := stream_core.NewHashMapCounter(cfg)
	if err != nil {
		log.Fatalf("failed to init kv counter: %v", err)
	}
	defer kv.Close()
	kv.IncInt("user/order_count", 1)
	kv.IncInt("user/order_count_cross", 2)
	kv.IncInt("user/price", 3)

	// checking key nt supported
	acckey, _ := kv.MergeInt(
		stream_core.MergeOpAdd,
		"user/all_order",
		"user/order_count",
		"user/order_count_cross",
	)

	kv.MergeInt(
		stream_core.MergeOpAdd,
		"user/order_count_cross_total",
		"user/all_order",
		"user/order_count",
		"user/order_count_cross",
		"user/price",
	)

	accget := kv.GetInt("user/all_order")

	kv.Snapshot(before, func(key string, value int64) error {
		log.Println("snapshot", key, value)
		return nil
	})

	log.Println("user/all_order", acckey, accget)

}
