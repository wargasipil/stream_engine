package stream_core_test

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wargasipil/stream_engine/stream_core"
)

func TestHashmap(t *testing.T) {

	cfg := stream_core.CoreConfig{
		HashMapCounterPath:  "/tmp/stream_engine/hashmap_unittest",
		HashMapCounterSlots: 64,
		DynamicValuePath:    "/tmp/stream_engine/hashmap_value_unittest",
	}
	os.Remove(cfg.DynamicValuePath)
	os.Remove(cfg.HashMapCounterPath)

	kv, err := stream_core.NewHashMapCounter(&cfg)

	assert.Nil(t, err)

	defer kv.Close()

	t.Run("testing increment float", func(t *testing.T) {
		newkey := kv.IncFloat64("users/ads_spents", 2000.01)
		assert.Equal(t, float64(2000.01), newkey)

		newkey = kv.IncFloat64("users/ads_spents", 1.22)
		assert.Equal(t, float64(2001.23), newkey)
	})

	t.Run("testing increment int", func(t *testing.T) {
		newkey := kv.IncInt64("product/stock", int64(1))
		assert.Equal(t, int64(1), newkey)

		newkey = kv.IncInt64("product/stock", int64(1))
		assert.Equal(t, int64(2), newkey)

		t.Run("adding new key", func(t *testing.T) {
			newkey = kv.IncInt64("product/stock_amount", 200)
			assert.Equal(t, int64(200), newkey)
			newkey = kv.IncInt64("product/stock_amount", 100)
			assert.Equal(t, int64(300), newkey)
		})
	})

	t.Run("testing computed key", func(t *testing.T) {
		kv.IncInt64("product/pending_stock", int64(1))

		_, err := kv.Merge(stream_core.MergeOpAdd, reflect.Uint64, "product/all_stock",
			"",
		)
		assert.NotNil(t, err, "merge key kosong")

		_, err = kv.Merge(stream_core.MergeOpAdd, reflect.Uint64, "product/all_stock",
			"",
		)
		assert.NotNil(t, err, "tidak punya merge key")

		t.Run("testing merge normal", func(t *testing.T) {
			_, err = kv.Merge(stream_core.MergeOpAdd, reflect.Uint64, "product/all_stock",
				"product/stock",
				"product/pending_stock",
			)
			assert.Nil(t, err)

			value := kv.GetUint64("product/all_stock")
			assert.Equal(t, uint64(3), value)

			t.Run("merge twice", func(t *testing.T) {
				_, err = kv.Merge(stream_core.MergeOpAdd, reflect.Uint64, "product/all_stock",
					"product/stock",
					"product/pending_stock",
				)
				assert.Nil(t, err)

				value := kv.GetUint64("product/all_stock")
				assert.Equal(t, uint64(3), value)
			})
		})

		t.Run("testing merge change", func(t *testing.T) {
			_, err = kv.Merge(stream_core.MergeOpAdd, reflect.Uint64, "product/all_stock",
				"product/stock",
				"product/pending_stock",
				"product/stock_amount",
			)
			assert.NotNil(t, err)
		})

	})

	t.Run("testing getting snapshot", func(t *testing.T) {
		kv.Snapshot(time.Now().Add(time.Second*-10), func(key string, kind reflect.Kind, value any) error {
			switch key {
			case "product/stock_amount":
				assert.Equal(t, int64(300), value)
			case "product/stock":
				assert.Equal(t, int64(2), value)
			case "product/all_stock":
				assert.Equal(t, uint64(3), value)
			}
			return nil
		})
	})
}
