package stream_core_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wargasipil/stream_engine/stream_core"
)

func TestNestedKey(t *testing.T) {
	var key stream_core.NestedKey = "users/1/products/42"
	iterkey := []string{}

	key.Iterate(func(key string) { iterkey = append(iterkey, key) })
	expected := []string{
		"users/1",
		"users/1/products/42",
	}

	assert.Equal(t, expected, iterkey)
}
