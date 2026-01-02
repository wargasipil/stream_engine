package main

import (
	"fmt"
	"log"
	"reflect"
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

	kv, err := stream_core.NewHashMapCounter(cfg)
	if err != nil {
		log.Fatalf("failed to init kv counter: %v", err)
	}
	defer kv.Close()

	// resetting counter

	kv.IncInt64("users/1/products/42/order_count", 5)
	kv.IncInt64("users/1/products/42/order_count", 10)
	kv.IncInt64("users/1/products/42/order_amount", 12000)
	kv.IncInt64("users/1/products/42/stock_pending", 5)

	data := kv.GetInt64("users/1/products/42/order_count")
	log.Printf("users/1/products/42/order_count: %d", data)

	start := time.Now()

	err = iterateExample("example.json", func(e *Transaction) error {
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

		kv.IncInt64(keyDebit, int64(e.Debit))
		kv.IncInt64(keyCredit, int64(e.Credit))

		// log.Println(key)
		return nil
	})

	if err != nil {
		panic(err)
	}

	duration := time.Since(start)

	kv.Snapshot(start, func(key string, kind reflect.Kind, value any) error {
		log.Println(key, value)
		return nil
	})

	log.Printf("duration seconds %s", duration)

	kv.IncInt64("teams/78/daily/2025-12-02/ads_expense/default/credit", 1)

	delta := kv.IncInt64("test_key", 12)
	log.Println("asdasd", kv.GetInt64("test_key"), delta)
}
