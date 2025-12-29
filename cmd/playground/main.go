package main

import (
	"log"
	"time"

	"github.com/wargasipil/stream_engine/stream_core"
)

func main() {
	cfg := stream_core.NewDefaultCoreConfigTest()
	counter, err := stream_core.NewKeyValueCounter(cfg)
	if err != nil {
		panic(err)
	}

	defer counter.Close()

	// stream_core.Replay(cfg.WalDir, func(data []byte) {
	// 	msg := &wal_message.CounterUint{}
	// 	proto.Unmarshal(data, msg)
	// 	counter.IncUint(msg.Key, msg.Value)
	// 	// log.Println(msg.Key, msg.Value)
	// })

	// value := counter.GetUint("test.key")
	// log.Println("test.key in wal =", value)

	// return

	// reflect.Uint16

	timeout := time.NewTimer(time.Second * 5)

Parent:
	for {
		select {
		case <-timeout.C:
			break Parent
		default:
			counter.IncUint("order_count/team:1/product:1/warehouse:2", 3)
			counter.IncUint("order_count/team:1/product:1/warehouse:1", 2)
			counter.IncUint("order_count/team:1/product:2", 1)
		}
	}

	key := stream_core.CounterKey("order_count/team:1/product:1/warehouse:2")

	for _, k := range key.Iterate() {
		log.Printf("%s count %d\n", k, counter.GetUint(k))
	}

	log.Printf("team 2 count %d\n", counter.GetUint("order_count/team:2"))
	log.Printf("team 1 count %d\n", counter.GetUint("order_count/team:1"))

	counter.Snapshot(time.Now().AddDate(-1, 0, 0), func(keyid string, value uint64) error {
		log.Println(keyid, value)
		return nil
	})
}
