package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/wargasipil/stream_engine/stream_core"
	// _ "net/http/pprof"
)

func main() {
	// go func() {
	// 	http.ListenAndServe("localhost:6060", nil)
	// }()
	// ctx := context.Background()
	// projectID := os.Getenv("GOOGLE_CLOUD_PROJECT") // or set directly
	// collection := "experimental"

	// fs, err := storage.NewFirestoreKeyStorage(ctx, projectID, collection)
	// if err != nil {
	// 	log.Fatalf("failed to init firestore: %v", err)
	// }
	// defer fs.Close()

	// err = fs.Increment("users/1/products/42", "order_count", 20)
	// if err != nil {
	// 	log.Fatalf("Increment failed: %v", err)
	// }

	// err = fs.Increment("users/default", "order_count", 20)
	// if err != nil {
	// 	log.Fatalf("Increment failed: %v", err)
	// }

	// cfg := stream_core.NewDefaultCoreConfigTest()
	cfg := stream_core.NewDefaultCoreConfig()

	kv, err := stream_core.NewHashMapCounter(cfg)
	if err != nil {
		log.Fatalf("failed to init kv counter: %v", err)
	}
	defer kv.Close()

	// resetting counter
	kv.ResetCounter()

	start := time.Now()

	err = iterateExample("example-tiny.json", func(e *Transaction) error {

		err = kv.Transaction(func(tx *stream_core.Transaction) error {
			metric := NewMetricTeamAccount(tx, uint64(e.TeamID), e.AccountKey)
			metric.IncDebit(float64(e.Debit))
			metric.IncCredit(float64(e.Credit))

			metric.IncBalance(
				metric.GetDebit() - metric.GetCredit(),
			)

			return nil
		})

		if err != nil {
			return err
		}

		metric := NewMetricTeamAccount(kv, uint64(e.TeamID), e.AccountKey)
		raw, _ := json.Marshal(metric.Data())
		log.Println(string(raw))

		return nil

	})

	if err != nil {
		panic(err)
	}

	duration := time.Since(start)

	// kv.Snapshot(start, true, func(key string, kind reflect.Kind, value any) error {
	// 	log.Printf("%s\t%.3f\n", key, value)
	// 	return nil
	// })

	log.Printf("duration seconds %s", duration)
	kv.PrintStat()
}
