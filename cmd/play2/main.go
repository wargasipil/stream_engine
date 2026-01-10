package main

import (
	"log"

	"github.com/google/uuid"
	"github.com/wargasipil/stream_engine/beetree"
)

func main() {
	fname := "/tmp/stream_engine/beetree_test"
	// defer os.RemoveAll(fname)
	tree, err := beetree.NewBeeTree(fname)
	if err != nil {
		log.Fatal(err)
	}

	defer tree.Close()

	// tree.Inspect()

	tree.Insert("sddd", 123)
	tree.Insert("kedua", 123)

	for i := 0; i < 100000; i++ {
		k := uuid.New().String()
		log.Println(k)
		tree.Insert(k, 400)
	}

}
