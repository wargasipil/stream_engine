package stream_core_test

import (
	"testing"

	"github.com/cespare/xxhash"
)

func TestHashmapHash(t *testing.T) {
	h := xxhash.Sum64String("slowsamo")
	t.Error(h & 3)
}
