package stream_utils

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/wargasipil/stream_engine/stream_core"
)

func NewBackup(kv *stream_core.HashMapCounter, dirPath string) (string, error) {
	kv.Lock()
	defer kv.Unlock()

	slog.Info("starting backup..")

	os.MkdirAll(dirPath, os.ModeDir)
	ts := time.Now()
	fname := filepath.Join(dirPath, fmt.Sprintf("%d.backup", ts.UnixMicro()))
	file, err := os.OpenFile(
		fname,
		os.O_WRONLY|os.O_CREATE,
		0644,
	)

	if err != nil {
		return "", err
	}

	defer file.Close()

	dynamic := kv.GetDynamicValue()
	err = dynamic.Iterate(func(key string, coffset int64, data []byte) error {
		counterData := kv.GetBytes(coffset)
		datalen := len(key) + len(data) + len(counterData)
		backupBytes := make([]byte, datalen+8)

		binary.LittleEndian.PutUint64(backupBytes[:8], uint64(datalen))
		off := 8
		var i int

		for i, c := range key {
			backupBytes[off+i] = byte(c)
		}
		off += i
		for i, c := range data {
			backupBytes[off+i] = c
		}

		off += i
		for i, c := range counterData {
			backupBytes[off+i] = c
		}

		_, err = file.Write(backupBytes)
		return err
	})

	if err != nil {
		return fname, err
	}

	return fname, nil
}
