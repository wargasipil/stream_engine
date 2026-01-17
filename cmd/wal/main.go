package main

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/wargasipil/stream_engine/stream_storage"
)

func main() {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	wal := stream_storage.NewWalStream(ctx, "stream_experiment", "test_source", client)
	defer wal.Close()

	err = wal.Replay(func(data []byte) error {
		println(string(data))
		return nil
	})

	if err != nil {
		panic(err)
	}

	// for c := 0; c < 100; c++ {
	// 	log.Println(c)
	// 	err = wal.Append([]byte("test"))
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

}
