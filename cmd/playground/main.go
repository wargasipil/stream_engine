package main

import (
	"kvcounter/proto_core/wal_message/v1"
	"kvcounter/stream_core"
	"log"
	"time"

	"google.golang.org/protobuf/proto"
)

func main() {
	cfg := stream_core.NewDefaultCoreConfigTest()
	counter, err := stream_core.NewKeyValueCounter(cfg)
	if err != nil {
		panic(err)
	}

	defer counter.Close()

	stream_core.Replay(cfg.WalDir, func(data []byte) {
		msg := &wal_message.CounterUint{}
		proto.Unmarshal(data, msg)
		// counter.IncUint(msg.Key, msg.Value)
		log.Println(msg.Key, msg.Value)
	})

	timeout := time.NewTimer(time.Minute)

Parent:
	for {
		select {
		case <-timeout.C:
			break Parent
		default:
			counter.IncUint("test.key", 1)
			counter.IncUint("test.key", 2)
			counter.IncUint("test.key2", 12)
			counter.IncUint("test.key2", 3)
		}
	}

	value := counter.GetUint("test.key")
	log.Println("test.key =", value)
	counter.Snapshot(func(keyid string, value uint64) error {
		log.Println(keyid, value)
		return nil
	})
}
