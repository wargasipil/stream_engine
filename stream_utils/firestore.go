package stream_utils

import (
	"context"
	"log"
	"log/slog"
	"os"
	"reflect"
	"strings"

	"cloud.google.com/go/firestore"
)

type FirestoreKeyStorage struct {
	ctx    context.Context
	client *firestore.Client
}

func NewFirestoreKeyStorage(ctx context.Context, database string) (*FirestoreKeyStorage, error) {
	client, err := firestore.NewClientWithDatabase(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"), database)
	if err != nil {
		return nil, err
	}
	return &FirestoreKeyStorage{ctx, client}, nil
}

func (f *FirestoreKeyStorage) SnapshotHandler() NextHandler {
	return func(next NextFunc) NextFunc {
		return func(key string, kind reflect.Kind, value any) error {
			var field, docref string

			keys := strings.Split(key, "/")
			if len(keys) <= 2 {
				return next(key, kind, value)
			} else {
				docrefs := []string{}
				var i int = 0

				for i < len(keys) {
					if i%2 == 0 {
						i++
						continue
					}

					docrefs = append(docrefs, strings.Join(keys[i-1:i+1], "/"))
					i++
				}

				docref = strings.Join(docrefs, "/")
				field = strings.ReplaceAll(key, docref, "")
				field = strings.ReplaceAll(field, "/", "_")
				field = strings.Trim(field, "_")
				if field == "" {
					field = "value"
				}
			}
			log.Println("updating", docref, field, value)
			doc := f.client.Doc(docref)

			_, err := doc.Set(f.ctx, map[string]interface{}{
				field: value,
			}, firestore.MergeAll)
			if err != nil {
				panic(err)
				slog.Error(key, slog.String("docref", docref), slog.String("field", field))
				return err
			}

			return next(key, kind, value)
		}
	}
}

// Increment implements KeyStorage.
func (f *FirestoreKeyStorage) Increment(path string, field string, delta int64) error {
	doc := f.client.Doc(path)

	_, err := doc.Set(f.ctx, map[string]interface{}{
		field: firestore.Increment(delta),
	}, firestore.MergeAll)

	return err
}

func (f *FirestoreKeyStorage) Close() error {
	return f.client.Close()
}
