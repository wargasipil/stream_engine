package stream_storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"path"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type WalStream struct {
	bucketName string
	sourceName string
	writer     *storage.Writer
	ctx        context.Context
	client     *storage.Client
	size       int
}

func NewWalStream(
	ctx context.Context,
	bucketName string,
	sourceName string,
	client *storage.Client,
) *WalStream {
	wal := &WalStream{bucketName, sourceName, nil, ctx, client, 0}

	return wal
}

func (wal *WalStream) Append(data []byte) error {
	writer, err := wal.currentSegmentWriter()
	if err != nil {
		return err
	}

	writeByte := make([]byte, len(data)+8)
	datalen := len(data)
	binary.LittleEndian.PutUint64(writeByte[:8], uint64(datalen))
	copy(writeByte[8:], data)

	nbyte, err := writer.Write(writeByte)
	if err != nil {
		return err
	}

	wal.size += nbyte
	if wal.size > 104_857_600 {
		err = writer.Close()
		if err != nil {
			return err
		}
		wal.writer = nil
		wal.size = 0
	}

	return err
}

func (wal *WalStream) Replay(handler func(data []byte) error) error {
	bucket := wal.client.Bucket(wal.bucketName)

	it := bucket.Objects(wal.ctx, &storage.Query{
		Prefix: path.Join(wal.sourceName, "segment-"),
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		log.Println("replay segment", attrs.Name)
		err = wal.replayFile(attrs.Name, handler)
		if err != nil {
			return err
		}
	}
	return nil
}

func (wal *WalStream) replayFile(name string, apply func(data []byte) error) error {
	bucket := wal.client.Bucket(wal.bucketName)
	reader, err := bucket.Object(name).NewReader(wal.ctx)
	if err != nil {
		return err
	}

	for {
		var size uint64
		if err := binary.Read(reader, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		buf := make([]byte, size)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return err
		}

		if err := apply(buf); err != nil {
			return err
		}
	}

}

func (wal *WalStream) Close() error {
	if wal.writer != nil {
		return wal.writer.Close()
	}
	wal.client.Close()
	return nil
}

func (wal *WalStream) currentSegmentWriter() (*storage.Writer, error) {
	if wal.writer != nil {
		return wal.writer, nil
	}

	segmentId, err := wal.getLastSegmentNumber()
	if err != nil {
		if !errors.Is(err, storage.ErrObjectNotExist) {
			return nil, err
		}
	}

	segmentId++

	name := fmt.Sprintf("segment-%020d", segmentId)
	writer := wal.getObject(name).NewWriter(wal.ctx)
	wal.writer = writer

	wal.putLastSegmentNumber(segmentId)
	return writer, nil
}

func (wal *WalStream) getLastSegmentNumber() (uint64, error) {
	reader, err := wal.getObject("commit").NewReader(wal.ctx)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	buf := make([]byte, 8)
	if _, err := reader.Read(buf); err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(buf), nil
}

func (wal *WalStream) putLastSegmentNumber(number uint64) error {
	writer := wal.getObject("commit").NewWriter(wal.ctx)
	defer writer.Close()

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, number)
	if _, err := writer.Write(buf); err != nil {
		return err
	}

	return nil
}

func (wal *WalStream) getObject(name string) *storage.ObjectHandle {
	return wal.client.Bucket(wal.bucketName).Object(path.Join(wal.sourceName, name))
}
