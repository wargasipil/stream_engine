package main

import (
	"encoding/json"
	"log"

	"github.com/wargasipil/stream_engine/example"
	"github.com/wargasipil/stream_engine/stream_core"
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
		metric := example.NewMetricExample(tx, 1, 30, "tokosaya")
		for i := 0; i < 100; i++ {
			metric.IncStockCount(123)
			metric.PutLastBalance(12000.6)
		}
		log.Println(metric.Data())
		log.Println(metric.GetKey())
		log.Println(metric.Values(), metric.Name, metric.GetLastBalance(), metric.GetStockCount())
		log.Println(example.IsMetricExample("team_user_sshopname/asd/asdasd"))

		metric, err = example.NewMetricExampleFromKey(kv, "team_user_shopname/1/30/tokosaya/log")
		if err != nil {
			panic(err)
		}

		raw, _ := json.Marshal(metric)
		log.Println(string(raw))

		log.Println(metric.GetKey(), metric.GetLastBalance())

		return nil
	})

	kv.Transaction(func(tx *stream_core.Transaction) error {
		metrict := example.NewMetricExampleTeam(tx, 4)
		metrict.PutLastBalance(700000)

		return nil
	})

	metrict, err := example.NewMetricExampleTeamFromKey(kv, "team/4/log")
	if err != nil {
		panic(err)
	}

	raw, _ := json.Marshal(metrict)
	log.Println(string(raw))

	log.Println(metrict.GetKey(), metrict.GetLastBalance())

}
