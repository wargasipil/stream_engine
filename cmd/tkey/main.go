package main

import (
	"log"
	"reflect"
	"time"

	"github.com/wargasipil/stream_engine/stream_core"
)

func main() {
	cfg := stream_core.NewDefaultCoreConfigTest()
	cfg.HashMapCounterSlots = 128

	before := time.Now()

	kv, err := stream_core.NewHashMapCounter(cfg)
	if err != nil {
		log.Fatalf("failed to init kv counter: %v", err)
	}
	defer kv.Close()
	kv.ResetCounter()

	kv.IncInt64("user/order_count", int64(1))
	kv.IncInt64("user/order_count_cross", int64(2))
	kv.IncInt64("user/price", int64(3))

	// checking key nt supported
	acckey, _ := kv.Merge(
		stream_core.MergeOpAdd,
		reflect.Uint64,
		"user/all_order",
		"user/order_count",
		"user/order_count_cross",
	)

	kv.Merge(
		stream_core.MergeOpAdd,
		reflect.Int64,
		"user/order_count_cross_total",
		"user/all_order",
		"user/order_count",
		"user/order_count_cross",
		"user/price",
	)

	accget := kv.GetUint64("user/all_order")

	kv.Snapshot(before, func(key string, kind reflect.Kind, value any) error {
		log.Println("snapshot", key, value)
		return nil
	})

	log.Println("user/all_order", accget, acckey)

}
