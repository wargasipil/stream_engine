package stream_core

import (
	"strings"
)

type CounterKey string

func (k CounterKey) CounterName() (string, string) {
	keys := strings.Split(string(k), "/")
	return keys[len(keys)-1], strings.Join(keys[:len(keys)-1], "/")
}

func (k CounterKey) Iterate() []CounterKey {

	keys := strings.Split(string(k), "/")
	counterName := keys[len(keys)-1]
	keys = keys[:len(keys)-1]

	result := []CounterKey{}

	parentKey := []string{}
	for i, key := range keys {
		parentKey = append(parentKey, key)
		if i%2 == 1 {
			if key == "default" {
				continue
			}
			partialKey := strings.Join(parentKey, "/")
			result = append(result, CounterKey(partialKey+"/"+counterName))
		} else {
			partialKey := strings.Join(parentKey, "/")
			result = append(result, CounterKey(partialKey+"/default/"+counterName))
		}
	}

	return result

}
