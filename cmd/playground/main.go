package main

import (
	"fmt"
	"log"
	"time"

	"github.com/wargasipil/stream_engine/beetree"
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
	tree, err := beetree.NewBeeTree("/tmp/stream_engine/example.index")

	if err != nil {
		panic(err)
	}

	defer tree.Close()
	start := time.Now()

	err = iterateExample("example.json", func(e *Transaction) error {
		var t time.Time = time.Time(e.EntryTime)

		key := fmt.Sprintf("team/%d/daily/%s/team/%d", e.TeamID, t.Format("2006-01-02"), e.AccountTeamID)
		tree.InsertKeyString(key, uint64(e.Debit))
		// log.Println(key)
		// log.Println(tree.Get([]byte(key)))
		return nil

	})

	if err != nil {
		panic(err)
	}

	duration := time.Since(start)

	tree.InsertKeyString("hollow", 991)
	log.Println(tree.Get([]byte("hollow")))

	// kv.Snapshot(start, true, func(key string, kind reflect.Kind, value any) error {
	// 	log.Printf("%s\t%.3f\n", key, value)
	// 	return nil
	// })

	log.Printf("duration seconds %s", duration)
}
