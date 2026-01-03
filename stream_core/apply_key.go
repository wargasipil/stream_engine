package stream_core

import (
	"encoding/binary"
	"math"
	"reflect"
	"time"
)

func (hm *HashMapCounter) getCounter(mustKind reflect.Kind, key string) any {
	offset := hm.hash.hash(key) + HASHMAP_METADATA_SIZE
	counter := binary.LittleEndian.Uint64(hm.data[offset+COUNTER_OFFSET : offset+COUNTER_OFFSET+8])
	typeCounter := reflect.Kind(hm.data[offset+HASHMAP_TYPE_COUNTER_OFFSET])
	switch typeCounter {
	case reflect.Uint64:
		return uint64(counter)
	case reflect.Int64:
		return int64(counter)
	case reflect.Float64:
		return math.Float64frombits(counter)
	case reflect.Invalid:
		switch mustKind {
		case reflect.Uint64:
			return uint64(counter)
		case reflect.Int64:
			return int64(counter)
		case reflect.Float64:
			return math.Float64frombits(counter)
		default:
			panic("get counter typedata default not supported")
		}
	default:
		panic("get counter typedata not supported")
	}
}

func (hm *HashMapCounter) apply(key string, delta any, replace bool) any {
	hm.lock.Lock()
	defer hm.lock.Unlock()

	t := time.Now().UnixMilli()
	ts := uint64(t)
	hkey := hm.hash.hash(key)
	offset := hkey + HASHMAP_METADATA_SIZE // offset + current count metadata

	lastts := binary.LittleEndian.Uint64(hm.data[offset+TIMESTAMP_OFFSET : offset+TIMESTAMP_OFFSET+8])

	if lastts == 0 {
		hm.keyCount += 1
		setCurrentCount(hm.data, hm.keyCount)

		// set counter and counter typedata
		switch val := delta.(type) {
		case uint64:
			hm.data[offset+HASHMAP_TYPE_COUNTER_OFFSET] = byte(reflect.Uint64)
			binary.LittleEndian.PutUint64(hm.data[offset+COUNTER_OFFSET:offset+COUNTER_OFFSET+8], val)
		case int64:
			hm.data[offset+HASHMAP_TYPE_COUNTER_OFFSET] = byte(reflect.Int64)
			binary.LittleEndian.PutUint64(hm.data[offset+COUNTER_OFFSET:offset+COUNTER_OFFSET+8], uint64(val))
		case float64:
			hm.data[offset+HASHMAP_TYPE_COUNTER_OFFSET] = byte(reflect.Float64)
			d := math.Float64bits(val)
			binary.LittleEndian.PutUint64(hm.data[offset+COUNTER_OFFSET:offset+COUNTER_OFFSET+8], d)
		default:
			panic("create counter typedata not supported")
		}

		// set timestamp
		binary.LittleEndian.PutUint64(hm.data[offset+TIMESTAMP_OFFSET:offset+TIMESTAMP_OFFSET+8], uint64(ts))
		// set type key
		binary.LittleEndian.PutUint64(hm.data[offset+TYPE_KEY_OFFSET:offset+TYPE_KEY_OFFSET+8], uint64(CounterKeyType))
		// set key pointer
		var counter byte = CounterKeyType
		keyOffset, err := hm.dynamicValue.Write(key, hkey, []byte{counter})
		if err != nil {
			panic(err)
		}
		binary.LittleEndian.PutUint64(hm.data[offset+KEY_POINTER_OFFSET:offset+KEY_POINTER_OFFSET+8], uint64(keyOffset))
		return delta
	}

	// jika sudah ada
	binary.LittleEndian.PutUint64(hm.data[offset+TIMESTAMP_OFFSET:offset+TIMESTAMP_OFFSET+8], uint64(ts))

	// check type counter dan lakukan operasi increment
	typeCounter := reflect.Kind(hm.data[offset+HASHMAP_TYPE_COUNTER_OFFSET])
	switch val := delta.(type) {
	case uint64:
		if typeCounter != reflect.Uint64 {
			panic("apply counter typedata false")
		}
		prevVal := binary.LittleEndian.Uint64(hm.data[offset+COUNTER_OFFSET : offset+COUNTER_OFFSET+8])
		var nextVal uint64
		if replace {
			nextVal = replaceOps(prevVal, val)
		} else {
			nextVal = addOps(prevVal, val)
		}
		binary.LittleEndian.PutUint64(hm.data[offset+COUNTER_OFFSET:offset+COUNTER_OFFSET+8], uint64(nextVal))

		return nextVal
	case int64:
		if typeCounter != reflect.Int64 {
			panic("apply counter typedata false")
		}
		prevVal := int64(binary.LittleEndian.Uint64(hm.data[offset+COUNTER_OFFSET : offset+COUNTER_OFFSET+8]))
		var nextVal int64
		if replace {
			nextVal = replaceOps(prevVal, val)
		} else {
			nextVal = addOps(prevVal, val)
		}
		binary.LittleEndian.PutUint64(hm.data[offset+COUNTER_OFFSET:offset+COUNTER_OFFSET+8], uint64(nextVal))

		return nextVal
	case float64:
		if typeCounter != reflect.Float64 {
			panic("apply counter typedata false")
		}
		d := binary.LittleEndian.Uint64(hm.data[offset+COUNTER_OFFSET : offset+COUNTER_OFFSET+8])
		prevVal := math.Float64frombits(d)
		var nextVal float64
		if replace {
			nextVal = replaceOps(prevVal, val)
		} else {
			nextVal = addOps(prevVal, val)
		}
		binary.LittleEndian.PutUint64(hm.data[offset+COUNTER_OFFSET:offset+COUNTER_OFFSET+8], math.Float64bits(nextVal))

		return nextVal
	default:
		panic("apply counter typedata not supported")
	}
}

func replaceOps[T uint64 | int64 | float64](prev T, next T) T {
	return next
}

func addOps[T uint64 | int64 | float64](prev T, next T) T {
	return prev + next
}
