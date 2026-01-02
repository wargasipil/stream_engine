package stream_core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"reflect"
	"sort"
	"time"
)

/*
computed field
| 8 byte operator type | 8 byte keylen | 8 byte source_key_hash | [8 byte data pointer] multiple
*/

const (
	MERGE_OPS_METADATA_SIZE       = 24
	MERGE_OPS_TYPE_OFFSET         = 0
	MERGE_DERRIVED_KEY_LEN_OFFSET = 8
	MERGE_SOURCE_HASH_OFFSET      = 16
	MERGE_DATA_OFFSET             = 24
)

type MergeOps int

const (
	MergeOpAdd MergeOps = iota
	MergeOpMin
	MergeOpMultiply
	MergeOpDivide
)

type Int64Slice []int64

func (s Int64Slice) Len() int           { return len(s) }
func (s Int64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type MergeData []byte

func NewMergeData(keylen int64) MergeData {
	// log.Println(keylen, "asdasd")
	data := make([]byte, MERGE_OPS_METADATA_SIZE+(keylen*8))
	binary.LittleEndian.PutUint64(data[MERGE_DERRIVED_KEY_LEN_OFFSET:MERGE_DERRIVED_KEY_LEN_OFFSET+8], uint64(keylen))
	return data
}

func (m MergeData) setOp(op MergeOps) {
	binary.LittleEndian.PutUint64(m[MERGE_OPS_TYPE_OFFSET:MERGE_OPS_TYPE_OFFSET+8], uint64(op))
}

func (m MergeData) setHashKeys(hasher *hashKey, keys Int64Slice) {
	sort.Sort(keys)
	keylen := len(keys)

	// set pointer data key

	for i, key := range keys {
		start := i * 8
		end := start + 8
		binary.LittleEndian.PutUint64(m[MERGE_DATA_OFFSET+start:MERGE_DATA_OFFSET+end], uint64(key))
	}
	// set key length
	binary.LittleEndian.PutUint64(m[MERGE_DERRIVED_KEY_LEN_OFFSET:MERGE_DERRIVED_KEY_LEN_OFFSET+8], uint64(keylen))
	// set hash derivedkey
	keyhash := hasher.hashByte(m[MERGE_DATA_OFFSET : MERGE_DATA_OFFSET+(keylen*8)])
	binary.LittleEndian.PutUint64(m[MERGE_SOURCE_HASH_OFFSET:MERGE_SOURCE_HASH_OFFSET+8], uint64(keyhash))
}

func (m MergeData) keys() []uint64 {

	klen := int64(binary.LittleEndian.Uint64(m[MERGE_DERRIVED_KEY_LEN_OFFSET : MERGE_DERRIVED_KEY_LEN_OFFSET+8]))
	offset := make([]uint64, klen)
	for i := 0; i < int(klen); i++ {
		start := i * 8
		end := start + 8
		offset[i] = binary.LittleEndian.Uint64(m[MERGE_DATA_OFFSET+start : MERGE_DATA_OFFSET+end])
	}

	return offset
}

func (m MergeData) getSourceHash() int64 {
	val := binary.LittleEndian.Uint64(m[MERGE_SOURCE_HASH_OFFSET : MERGE_SOURCE_HASH_OFFSET+8])
	return int64(val)
}

// ---------------------------- merge int implementation ---------------------------------

func (hm *HashMapCounter) Merge(op MergeOps, kind reflect.Kind, computedKey string, keys ...string) (any, error) {
	hkey := hm.hash.hash(computedKey)
	offset := hkey + HASHMAP_METADATA_SIZE

	var derivedKeyLen int64 = int64(len(keys))
	if derivedKeyLen == 0 {
		return 0, fmt.Errorf("derrived key %s empty", computedKey)
	}

	mergeData := NewMergeData(derivedKeyLen)
	mergeData.setOp(op)

	// // generating key hash offset
	derrivedKeys := make([]int64, len(keys))
	for i, key := range keys {
		if key == "" {
			return 0, errors.New("key have empty string")
		}
		dhkey := hm.hash.hash(key)
		derrivedKeys[i] = dhkey
	}

	mergeData.setHashKeys(hm.hash, derrivedKeys)

	hm.lock.Lock()
	defer hm.lock.Unlock()

	lastts := binary.LittleEndian.Uint64(hm.data[offset+TIMESTAMP_OFFSET : offset+TIMESTAMP_OFFSET+8])

	t := time.Now().UnixMilli()
	ts := uint64(t)

	if lastts == 0 {
		hm.keyCount += 1
		setCurrentCount(hm.data, hm.keyCount)

		// set counter typedata
		switch kind {
		case reflect.Uint64:
			hm.data[offset+HASHMAP_TYPE_COUNTER_OFFSET] = byte(reflect.Uint64)
		case reflect.Int64:
			hm.data[offset+HASHMAP_TYPE_COUNTER_OFFSET] = byte(reflect.Int64)
		case reflect.Float64:
			hm.data[offset+HASHMAP_TYPE_COUNTER_OFFSET] = byte(reflect.Float64)
		default:
			panic("merge counter typedata not supported")
		}

		// set timestamp
		binary.LittleEndian.PutUint64(hm.data[offset+TIMESTAMP_OFFSET:offset+TIMESTAMP_OFFSET+8], uint64(ts))
		// set type key
		binary.LittleEndian.PutUint64(hm.data[offset+TYPE_KEY_OFFSET:offset+TYPE_KEY_OFFSET+8], uint64(MergeKeyType))

		// writing to dynamic key
		keyOffset, err := hm.dynamicValue.Write(computedKey, hkey, mergeData)
		if err != nil {
			return 0, err
		}

		log.Println("new pointer offset", keyOffset)

		// set key pointer
		binary.LittleEndian.PutUint64(hm.data[offset+KEY_POINTER_OFFSET:offset+KEY_POINTER_OFFSET+8], uint64(keyOffset))

	} else {
		// checking keyhash before
		mdataOffset := int64(binary.LittleEndian.Uint64(hm.data[offset+KEY_POINTER_OFFSET : offset+KEY_POINTER_OFFSET+8]))
		log.Println("exist pointer offset", mdataOffset)

		var mdata MergeData = hm.dynamicValue.GetData(mdataOffset)
		log.Println("source hash", mdata.getSourceHash(), mergeData.getSourceHash())

		if mdata.getSourceHash() != mergeData.getSourceHash() {
			return 0, fmt.Errorf("%s derrived key hash changed", computedKey)
		}

		// checking counter data
		existKind := reflect.Kind(hm.data[offset+HASHMAP_TYPE_COUNTER_OFFSET])
		if existKind != kind {
			return 0, fmt.Errorf("%s derrived counter type inconsistent", computedKey)
		}
	}

	// recalculate key
	var accvalue accumulator
	existKind := reflect.Kind(hm.data[offset+HASHMAP_TYPE_COUNTER_OFFSET])

	switch existKind {
	case reflect.Uint64:
		accvalue = &accumulatorImpl[uint64]{}
	case reflect.Int64:
		accvalue = &accumulatorImpl[int64]{}
	case reflect.Float64:
		accvalue = &accumulatorImpl[float64]{}
	default:
		panic("merge counter typedata not supported")
	}

	for _, offsetKey := range mergeData.keys() {
		bytesValue := hm.data[offsetKey+HASHMAP_METADATA_SIZE+COUNTER_OFFSET : offsetKey+HASHMAP_METADATA_SIZE+COUNTER_OFFSET+8]
		typeValue := reflect.Kind(hm.data[offsetKey+HASHMAP_METADATA_SIZE+HASHMAP_TYPE_COUNTER_OFFSET])

		accvalue.ops(op, typeValue, bytesValue)

	}
	binary.LittleEndian.PutUint64(hm.data[offset+COUNTER_OFFSET:offset+COUNTER_OFFSET+8], accvalue.getUint64())

	return accvalue.getValue(), nil
}

type accumulator interface {
	ops(op MergeOps, src reflect.Kind, value []byte)
	getUint64() uint64
	getValue() any
}

type accumulatorImpl[T int64 | uint64 | float64] struct {
	value T
}

func (a *accumulatorImpl[T]) getValue() any {
	return a.value
}

func (a *accumulatorImpl[T]) getUint64() uint64 {
	switch val := any(a.value).(type) {
	case uint64:
		return val
	case int64:
		return uint64(val)
	case float64:
		return math.Float64bits(val)
	default:
		panic("convert value typedata not supported")
	}

}

func (a *accumulatorImpl[T]) ops(op MergeOps, src reflect.Kind, value []byte) {
	switch op {
	case MergeOpAdd:
		a.value += a.convert(src, value)
	case MergeOpDivide:
		a.value = a.value / a.convert(src, value)
	case MergeOpMultiply:
		a.value = a.value * a.convert(src, value)
	case MergeOpMin:
		a.value -= a.convert(src, value)
	}
}

func (a *accumulatorImpl[T]) convert(src reflect.Kind, value []byte) T {
	switch src {
	case reflect.Uint64:
		return T(binary.LittleEndian.Uint64(value))
	case reflect.Int64:
		return T(binary.LittleEndian.Uint64(value))
	case reflect.Float64:
		return T(math.Float64frombits(binary.LittleEndian.Uint64(value)))
	default:
		panic("convert value typedata not supported")
	}
}
