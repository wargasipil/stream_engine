package stream_core

import (
	"encoding/binary"
	"errors"
	"fmt"
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

func (hm *HashMapCounter) MergeInt(op MergeOps, computedKey string, keys ...string) (int64, error) {
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

		// set timestamp
		binary.LittleEndian.PutUint64(hm.data[offset+TIMESTAMP_OFFSET:offset+TIMESTAMP_OFFSET+8], uint64(ts))
		// set type key
		binary.LittleEndian.PutUint64(hm.data[offset+TYPE_KEY_OFFSET:offset+TYPE_KEY_OFFSET+8], uint64(MergeKeyType))

		// writing to dynamic key
		keyOffset, err := hm.dynamicValue.Write(computedKey, hkey, mergeData)
		if err != nil {
			return 0, err
		}
		// set key pointer
		binary.LittleEndian.PutUint64(hm.data[offset+KEY_POINTER_OFFSET:offset+KEY_POINTER_OFFSET+8], uint64(keyOffset))

	} else {
		// checking keyhash before
		var mdata MergeData = hm.dynamicValue.GetData(hkey)
		if mdata.getSourceHash() != mergeData.getSourceHash() {
			return 0, fmt.Errorf("%s derrived key hash changed", computedKey)
		}
	}

	// recalculate key
	var accvalue uint64
	for _, offsetKey := range mergeData.keys() {
		value := binary.LittleEndian.Uint64(hm.data[offsetKey+HASHMAP_METADATA_SIZE+COUNTER_OFFSET : offsetKey+HASHMAP_METADATA_SIZE+COUNTER_OFFSET+8])
		switch op {
		case MergeOpAdd:
			accvalue += value
		case MergeOpDivide:
			accvalue = accvalue / value
		case MergeOpMultiply:
			accvalue = accvalue * value
		case MergeOpMin:
			accvalue -= value
		}

		// log.Println("offset", value, offsetKey+HASHMAP_METADATA_SIZE+COUNTER_OFFSET)
	}
	binary.LittleEndian.PutUint64(hm.data[offset+COUNTER_OFFSET:offset+COUNTER_OFFSET+8], accvalue)

	return int64(accvalue), nil

}
