package main

import (
	"log"

	"github.com/google/uuid"
	"github.com/wargasipil/stream_engine/beetree"
	"github.com/wargasipil/stream_engine/counter"
	"github.com/wargasipil/stream_engine/stream_core"
)

func main() {
	// var err error
	var kv stream_core.KeyStore = counter.NewKeyCounter("/tmp/stream_engine")
	defer kv.Close()
	// for i := 0; i < 200; i++ {
	// 	key := uuid.New().String()

	// 	val := rand.Int64N(5000)

	// 	kv.PutInt64(key, val)
	// 	log.Println(kv.GetInt64(key))
	// }

	kv.PutInt64("userstock", kv.GetInt64("userstock")+30)

	kv.PutInt64("userstock_amount",
		kv.GetInt64("userstock")+300,
	)

	log.Println("userstock_amount", kv.GetInt64("userstock_amount"))

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

	tree.Insert("sddd", 123)
	tree.Insert("kedua", 123)

	for i := 0; i < 100000; i++ {
		k := uuid.New().String()
		log.Println(k)
		tree.Insert(k, 400)
	}

}
