package main

import (
	"log"
	"time"

	"github.com/wargasipil/stream_engine/proto_core/wal_message/v1"
	"github.com/wargasipil/stream_engine/stream_core"
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
		counter.IncUint(msg.Key, msg.Value, false)
		// log.Println(msg.Key, msg.Value)
	})

	value := counter.GetUint("test.key")
	log.Println("test.key in wal =", value)

	return

	timeout := time.NewTimer(time.Minute)

Parent:
	for {
		select {
		case <-timeout.C:
			break Parent
		default:
			counter.IncUint("test.key", 1, true)
			// counter.IncUint("test.key", 2, true)
			counter.IncUint("test.key2", 12, true)
			counter.IncUint("test.key2", 3, true)
		}
	}

	value = counter.GetUint("test.key")
	log.Println("test.key =", value)
	counter.Snapshot(func(keyid string, value uint64) error {
		log.Println(keyid, value)
		return nil
	})
}
