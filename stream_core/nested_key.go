package stream_core

import "strings"

type NestedKey string

func (k NestedKey) Iterate(handler func(key string)) {
	keys := strings.Split(string(k), "/")

	for i := range keys {
		if i%2 == 0 {
			continue
		}
		partialKey := strings.Join(keys[:i+1], "/")
		handler(partialKey)
	}
}
