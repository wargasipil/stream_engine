package main

import (
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/wargasipil/stream_engine/beetree"
	"github.com/wargasipil/stream_engine/stream_counter"
)

func main() {
	// var err error
	kv := stream_counter.NewKeyCounter("/tmp/stream_engine")
	defer kv.Close()

	// for i := 0; i < 200; i++ {
	// 	key := uuid.New().String()

	// 	val := rand.Int64N(5000)

	// 	kv.PutInt64(key, val)
	// 	log.Println(kv.GetInt64(key))
	// }

	// kv.PutInt64("userstock", kv.GetInt64("userstock")+30)

	// kv.PutInt64("userstock_amount",
	// 	kv.GetInt64("userstock")+300,
	// )

	// log.Println("userstock_amount", kv.GetInt64("userstock_amount"))

	// met := example.NewMetricExampleTeam(kv, 2)
	// met.GetLastBalance()
	// met.PutLastBalance(float64(met.GetProductCount()) * float64(met.GetReadyStockCount()))

	// met.IncLastBalance(123.00)
	// log.Println(met.GetLastBalance())

	// met.PutProductCount(123233)
	// met.IncReadyStockCount(12)
	// met.IncStockCount(12)
	// dd, _ := json.Marshal(met.Data())
	// log.Println(string(dd))
	kv.PutFloat64("tiga", 3)
	kv.PutFloat64("empat", 4)
	kv.PutFloat64("satu", 1)
	kv.PutFloat64("dua", 2)

	kv.UpdatedKey(time.Now(), func(key string) error {
		log.Println(key)
		return nil
	})
}

func mainb() {
	fname := "/tmp/stream_engine/beetree_test"
	// defer os.RemoveAll(fname)
	tree, err := beetree.NewBeeTree(fname)
	if err != nil {
		log.Fatal(err)
	}

	defer tree.Close()

	tree.Inspect()

	tree.InsertKeyString("sddd", 123)
	tree.InsertKeyString("kedua", 123)

	for i := 0; i < 100000; i++ {
		k := uuid.New().String()
		log.Println(k)
		tree.InsertKeyString(k, 400)
	}

}
