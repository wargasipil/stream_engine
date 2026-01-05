package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type WAL struct {
	mu        sync.Mutex
	dir       string
	maxSize   int64
	segmentID int64

	file *os.File
	bw   *bufio.Writer
	size int64
}

/* ---------- WAL OPEN ---------- */

func OpenWAL(dir string, maxSize int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	last, err := lastSegment(dir)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		dir:       dir,
		maxSize:   maxSize,
		segmentID: last,
	}

	if err := w.openSegment(last); err != nil {
		return nil, err
	}

	return w, nil
}

/* ---------- APPEND ---------- */

func (w *WAL) Append(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	recordSize := int64(4 + len(data)) // uint32 length + payload
	if w.size+recordSize > w.maxSize {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	if err := binary.Write(w.bw, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}

	if _, err := w.bw.Write(data); err != nil {
		return err
	}

	if err := w.bw.Flush(); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	w.size += recordSize
	return nil
}

/* ---------- ROTATION ---------- */

func (w *WAL) rotate() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	w.segmentID++
	return w.openSegment(w.segmentID)
}

func (w *WAL) openSegment(id int64) error {
	path := segmentPath(w.dir, id)

	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return err
	}

	info, _ := f.Stat()

	w.file = f
	w.bw = bufio.NewWriter(f)
	w.size = info.Size()
	return nil
}

/* ---------- REPLAY ---------- */

func ReplayWAL(dir string, apply func([]byte) error) error {
	segs, err := listSegments(dir)
	if err != nil {
		return err
	}

	for _, id := range segs {
		if err := replayFile(segmentPath(dir, id), apply); err != nil {
			return err
		}
	}
	return nil
}

func replayFile(path string, apply func([]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	for {
		var size uint32
		if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		buf := make([]byte, size)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}

		if err := apply(buf); err != nil {
			return err
		}
	}
}

/* ---------- CLEANUP ---------- */

func RemoveUpTo(dir string, upto int64) error {
	segs, err := listSegments(dir)
	if err != nil {
		return err
	}

	for _, id := range segs {
		if id <= upto {
			_ = os.Remove(segmentPath(dir, id))
		}
	}
	return nil
}

/* ---------- HELPERS ---------- */

func segmentPath(dir string, id int64) string {
	return filepath.Join(dir, fmt.Sprintf("wal-%06d.log", id))
}

func listSegments(dir string) ([]int64, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var ids []int64
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log") {
			id, err := strconv.ParseInt(
				strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log"),
				10, 64,
			)
			if err == nil {
				ids = append(ids, id)
			}
		}
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids, nil
}

func lastSegment(dir string) (int64, error) {
	segs, err := listSegments(dir)
	if err != nil || len(segs) == 0 {
		return 1, nil
	}
	return segs[len(segs)-1], nil
}

/* ---------- CLOSE ---------- */

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_ = w.bw.Flush()
	return w.file.Close()
}

/* ---------- DEMO MAIN ---------- */

func main() {
	walDir := "./wal"
	maxSegSize := int64(1024 * 1024) // 1MB per segment

	// ---- recovery ----
	fmt.Println("Replaying WAL...")
	ReplayWAL(walDir, func(b []byte) error {
		fmt.Println("REPLAY:", string(b))
		return nil
	})

	// ---- open wal ----
	wal, err := OpenWAL(walDir, maxSegSize)
	if err != nil {
		panic(err)
	}
	defer wal.Close()

	// ---- append records ----
	for i := 1; i <= 5; i++ {
		entry := fmt.Sprintf("tx=%d amount=%d", i, i*100)
		if err := wal.Append([]byte(entry)); err != nil {
			panic(err)
		}
		fmt.Println("APPEND:", entry)
	}

	fmt.Println("Done")
}
