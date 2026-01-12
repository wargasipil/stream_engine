package counter_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wargasipil/stream_engine/counter"
)

func TestCounter(t *testing.T) {
	kv := counter.NewKeyCounter("/tmp/stream_engine/test")
	kv.PutFloat64("testkey", 123)
	assert.Equal(t, 123.00, kv.GetFloat64("testkey"))
	kv.PutFloat64("testkey2", 1232)
	assert.Equal(t, 1232.00, kv.GetFloat64("testkey2"))
	kv.PutFloat64("testkey3", 1233)
	assert.Equal(t, 1233.00, kv.GetFloat64("testkey3"))

	kv.IncFloat64("testkey", 1233)
	assert.Equal(t, 1356.00, kv.GetFloat64("testkey"))
}
