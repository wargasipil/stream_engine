package main

import (
	"log"

	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_schema"
)

func main() {
	cfg := stream_core.NewDefaultCoreConfigTest()
	cfg.HashMapCounterSlots = 128

	kv, err := stream_core.NewHashMapCounter(cfg)
	if err != nil {
		log.Fatalf("failed to init kv counter: %v", err)
	}
	defer kv.Close()
	// kv.ResetCounter()

	kv.Transaction(func(tx *stream_core.Transaction) error {
		metric := stream_schema.NewMetricUserTeam(tx, 1, 30)
		for i := 0; i < 100; i++ {
			metric.IncCredit(123)
			metric.PutLastBalance(12000.6)
		}

		log.Println(metric.GetKey(), metric.Name, metric.GetLastBalance(), metric.GetCredit())
		return nil
	})

}
