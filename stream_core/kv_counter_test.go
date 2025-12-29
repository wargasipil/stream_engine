package stream_core_test

import (
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wargasipil/stream_engine/proto_core/wal_message/v1"
	"github.com/wargasipil/stream_engine/stream_core"
	"google.golang.org/protobuf/proto"
)

func TestKeyvalueCounter(t *testing.T) {
	cfg := stream_core.NewDefaultCoreConfigTest()
	counter, err := stream_core.NewKeyValueCounter(cfg)
	if err != nil {
		t.Fatal(err)
	}

	defer counter.Close()

	stream_core.Replay(cfg.WalDir, func(data []byte) {
		msg := &wal_message.CounterUint{}
		proto.Unmarshal(data, msg)
		// counter.IncUint(msg.Key, msg.Value)
		log.Println(msg.Key, msg.Value)
	})

	counter.IncUint("test.key", 1)
	counter.IncUint("test.key", 2)
	counter.IncUint("test.key2", 12)
	counter.IncUint("test.key2", 3)

	value := counter.GetUint("test.key")
	assert.Equal(t, uint64(3), value)

	counter.Snapshot(time.Now().AddDate(-1, 0, 0), func(keyid string, value uint64) error {
		t.Log(keyid, value)
		return nil
	})

}
