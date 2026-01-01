package stream_core_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wargasipil/stream_engine/stream_core"
)

func TestHashmap(t *testing.T) {

	cfg := stream_core.CoreConfig{
		HashMapCounterPath:  "/tmp/stream_engine/hashmap_unittest",
		HashMapCounterSlots: 32,
		DynamicValuePath:    "/tmp/stream_engine/hashmap_value_unittest",
	}

	kv, err := stream_core.NewHashMapCounter(&cfg)

	assert.Nil(t, err)
	defer os.Remove(cfg.DynamicValuePath)
	defer os.Remove(cfg.HashMapCounterPath)
	defer kv.Close()

	t.Run("testing increment", func(t *testing.T) {
		newkey := kv.IncInt("product/stock", 1)
		assert.Equal(t, int64(1), newkey)

		newkey = kv.IncInt("product/stock", 1)
		assert.Equal(t, int64(2), newkey)

		t.Run("adding new key", func(t *testing.T) {
			newkey = kv.IncInt("product/stock_amount", 200)
			assert.Equal(t, int64(200), newkey)
			newkey = kv.IncInt("product/stock_amount", 100)
			assert.Equal(t, int64(300), newkey)
		})
	})

	t.Run("testing computed key", func(t *testing.T) {
		kv.IncInt("product/pending_stock", 1)

		_, err := kv.MergeInt(stream_core.MergeOpAdd, "product/all_stock",
			"",
		)
		assert.NotNil(t, err, "merge key kosong")

		_, err = kv.MergeInt(stream_core.MergeOpAdd, "product/all_stock",
			"",
		)
		assert.NotNil(t, err, "tidak punya merge key")

		t.Run("testing merge normal", func(t *testing.T) {
			_, err = kv.MergeInt(stream_core.MergeOpAdd, "product/all_stock",
				"product/stock",
				"product/pending_stock",
			)
			assert.Nil(t, err)

			value := kv.GetInt("product/all_stock")
			assert.Equal(t, int64(3), value)
		})

		t.Run("testing merge change", func(t *testing.T) {
			_, err = kv.MergeInt(stream_core.MergeOpAdd, "product/all_stock",
				"product/stock",
				"product/pending_stock",
				"product/stock_amount",
			)
			assert.NotNil(t, err)
		})

	})

	t.Run("testing getting snapshot", func(t *testing.T) {
		kv.Snapshot(time.Now().Add(time.Second*-10), func(key string, value int64) error {
			switch key {
			case "product/stock_amount":
				assert.Equal(t, int64(300), value)
			case "product/stock":
				assert.Equal(t, int64(2), value)
			case "product/all_stock":
				assert.Equal(t, int64(3), value)
			}
			return nil
		})
	})
}
