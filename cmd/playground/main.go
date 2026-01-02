package main

import (
	"fmt"
	"log"
	"reflect"
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
		// var teamID string

		accountkey := fmt.Sprintf("%s", e.AccountKey)

		kv.IncFloat64("debit", float64(e.Debit))
		kv.IncFloat64("credit", float64(e.Credit))
		switch e.BalanceType {
		case "d":
			kv.Merge(stream_core.MergeOpMin,
				reflect.Float64,
				"balance",
				"debit",
				"credit",
			)
		case "c":
			kv.Merge(stream_core.MergeOpMin,
				reflect.Float64,
				"balance",
				"credit",
				"debit",
			)

		}

		kv.IncFloat64(accountkey+"/debit", float64(e.Debit))
		kv.IncFloat64(accountkey+"/credit", float64(e.Credit))

		switch e.BalanceType {
		case "d":
			kv.Merge(stream_core.MergeOpMin,
				reflect.Float64,
				accountkey+"/balance",
				accountkey+"/debit",
				accountkey+"/credit",
			)
		case "c":
			kv.Merge(stream_core.MergeOpMin,
				reflect.Float64,
				accountkey+"/balance",
				accountkey+"/credit",
				accountkey+"/debit",
			)

		}

		// if e.TeamID == e.AccountTeamID {
		// 	teamID = "default"
		// } else {
		// 	teamID = fmt.Sprintf("%d", e.AccountID)
		// }
		// key := fmt.Sprintf(
		// 	"teams/%d/daily/%s/%s/%s",
		// 	e.TeamID,
		// 	(time.Time)(e.EntryTime).Format("2006-01-02"),
		// 	e.AccountKey,
		// 	teamID,
		// )

		// keyDebit := key + "/debit"
		// keyCredit := key + "/credit"

		// kv.IncInt64(keyDebit, int64(e.Debit))
		// kv.IncInt64(keyCredit, int64(e.Credit))

		// log.Println(key)
		return nil
	})

	if err != nil {
		panic(err)
	}

	duration := time.Since(start)

	kv.Snapshot(start, func(key string, kind reflect.Kind, value any) error {
		log.Printf("%s\t%.3f\n", key, value)
		return nil
	})

	log.Printf("duration seconds %s", duration)
	kv.PrintStat()
}
