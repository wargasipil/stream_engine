package stream_core_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wargasipil/stream_engine/stream_core"
)

func TestCounterKeyIterate(t *testing.T) {
	tests := []struct {
		name     string
		input    stream_core.CounterKey
		expected []stream_core.CounterKey
	}{
		{
			name:     "empty key",
			input:    stream_core.CounterKey(""),
			expected: []stream_core.CounterKey{},
		},
		{
			name:  "users/1/products/42/order_count",
			input: stream_core.CounterKey("users/1/products/42/order_count"),
			expected: []stream_core.CounterKey{
				"users/default/order_count",
				"users/1/order_count",
				"users/1/products/default/order_count",
				"users/1/products/42/order_count",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.input.Iterate()
			assert.Equal(t, tt.expected, result)
		})
	}

	t.Run("testing path", func(t *testing.T) {
		var key stream_core.CounterKey = "users/1/products/default/order_count"
		field, path := key.CounterName()
		assert.Equal(t, "order_count", field)
		assert.Equal(t, "users/1/products/default", path)

		iterkey := key.Iterate()
		expected := []stream_core.CounterKey{
			"users/default/order_count",
			"users/1/order_count",
			"users/1/products/default/order_count",
		}
		assert.Equal(t, expected, iterkey)
	})
}
