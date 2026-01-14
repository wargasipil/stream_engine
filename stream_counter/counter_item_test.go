package stream_counter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCounter2(t *testing.T) {
	key := "flowtest123412312312312312312312"
	data := make([]byte, 5+36+len(key))
	cc := newCounter(5, data)

	cc.putKey(key)

	assert.Equal(t, cc.key(), key)
	assert.Equal(t, int(cc.keylen()), len(key))
}
