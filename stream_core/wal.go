package stream_core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"
)

const (
	magic       uint32 = 0x57414C31 // "WAL1"
	headerSize         = 12         // magic(4) + len(4) + crc(4)
	segmentSize        = 64 << 20   // 64MB
)

type WAL struct {
	mu        sync.Mutex
	dir       string
	file      *os.File
	segmentID uint64
	size      int64
}

// ---------- WAL OPEN ----------

func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	id, err := lastSegmentID(dir)
	if err != nil {
		return nil, err
	}

	f, size, err := openSegment(dir, id)
	if err != nil {
		return nil, err
	}

	return &WAL{
		dir:       dir,
		file:      f,
		segmentID: id,
		size:      size,
	}, nil
}

// ---------- APPEND ----------

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

func (w *WAL) Append(msg proto.Message) error {

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	recSize := int64(headerSize + len(data))
	if w.size+recSize > segmentSize {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	buf := make([]byte, headerSize+len(data))
	binary.LittleEndian.PutUint32(buf[0:4], magic)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(data)))
	binary.LittleEndian.PutUint32(buf[8:12], crc32.ChecksumIEEE(data))
	copy(buf[12:], data)

	if _, err := w.file.Write(buf); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}

	w.size += recSize
	return nil
}

// ---------- ROTATE ----------

func (w *WAL) rotate() error {
	if err := w.file.Close(); err != nil {
		return err
	}

	w.segmentID++
	f, _, err := openSegment(w.dir, w.segmentID)
	if err != nil {
		return err
	}

	w.file = f
	w.size = 0
	return nil
}

// ---------- REPLAY ----------

func Replay(dir string, apply func([]byte)) error {
	ids, err := listSegments(dir)
	if err != nil {
		return err
	}

	for _, id := range ids {
		path := filepath.Join(dir, segmentName(id))
		f, err := os.Open(path)
		if err != nil {
			return err
		}

		for {
			header := make([]byte, headerSize)
			_, err := io.ReadFull(f, header)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // clean stop or torn write
			}
			if err != nil {
				f.Close()
				return err
			}

			if binary.LittleEndian.Uint32(header[0:4]) != magic {
				f.Close()
				return errors.New("bad wal magic")
			}

			l := binary.LittleEndian.Uint32(header[4:8])
			crc := binary.LittleEndian.Uint32(header[8:12])

			data := make([]byte, l)
			if _, err := io.ReadFull(f, data); err != nil {
				break // torn write
			}

			if crc32.ChecksumIEEE(data) != crc {
				break // corruption
			}

			apply(data)
		}

		f.Close()
	}
	return nil
}

// ---------- HELPERS ----------

func openSegment(dir string, id uint64) (*os.File, int64, error) {
	path := filepath.Join(dir, segmentName(id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, err
	}
	info, _ := f.Stat()
	return f, info.Size(), nil
}

func segmentName(id uint64) string {
	return fmt.Sprintf("%016d.wal", id)
}

func lastSegmentID(dir string) (uint64, error) {
	ids, err := listSegments(dir)
	if err != nil || len(ids) == 0 {
		return 1, nil
	}
	return ids[len(ids)-1], nil
}

func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var ids []uint64
	for _, e := range entries {
		var id uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.wal", &id); err == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
