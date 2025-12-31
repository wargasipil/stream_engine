package main

import (
	"fmt"
	"log"
	"time"

	"github.com/wargasipil/stream_engine/stream_core"

	"net/http"
	_ "net/http/pprof"
)

func main() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
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

	kv, err := stream_core.NewKeyValueCounter(cfg)
	if err != nil {
		log.Fatalf("failed to init kv counter: %v", err)
	}
	defer kv.Close()

	kv.IncInt(stream_core.CounterKey("users/1/products/42/order_count"), 5)
	kv.IncInt(stream_core.CounterKey("users/1/products/42/order_count"), 10)
	kv.IncInt(stream_core.CounterKey("users/1/products/42/order_amount"), 12000)
	kv.IncInt(stream_core.CounterKey("users/1/products/42/stock_pending"), 5)

	data, ts := kv.GetInt("users/default/order_count")
	log.Printf("users/default/order_count: %d (timestamp: %s)", data, time.UnixMilli(ts).String())

	start := time.Now()

	err = iterateExample(func(e *Transaction) error {
		var teamID string
		if e.TeamID == e.AccountTeamID {
			teamID = "default"
		} else {
			teamID = fmt.Sprintf("%d", e.AccountID)
		}
		key := fmt.Sprintf(
			"teams/%d/daily/%s/%s/%s",
			e.TeamID,
			(time.Time)(e.EntryTime).Format("2006-01-02"),
			e.AccountKey,
			teamID,
		)

		keyDebit := key + "/debit"
		keyCredit := key + "/credit"

		kv.IncInt(stream_core.CounterKey(keyDebit), int64(e.Debit))
		kv.IncInt(stream_core.CounterKey(keyCredit), int64(e.Credit))

		return nil
	})

	duration := time.Since(start)

	kv.Snapshot(time.Now().AddDate(-1, 0, 0), func(key stream_core.CounterKey, value int64) error {
		// log.Printf("%s with counter: %d\n", key, value)
		field, path := key.CounterName()
		log.Println(field, path, value)
		// err = fs.Increment(path, field, value)
		return nil
	})

	log.Printf("duration seconds %s", duration)
}
