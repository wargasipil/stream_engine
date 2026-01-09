package main

import (
	"log"

	"github.com/wargasipil/stream_engine/stream_core"
)

func main() {
	tree, err := stream_core.NewBeeTree("/tmp/stream_engine/beetree_test")
	if err != nil {
		log.Fatal(err)
	}

	defer tree.Close()

	tree.Insert("slow", 200)
	tree.Insert("user/asdasd/team6", 200)
}
