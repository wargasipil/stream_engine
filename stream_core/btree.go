package stream_core

import (
	"bytes"

	"github.com/google/btree"
)

const BTreeDegree = 32

type KVItem struct {
	Key []byte
	Off uint64
}

func (a KVItem) Less(b btree.Item) bool {
	return bytes.Compare(a.Key, b.(KVItem).Key) < 0
}
