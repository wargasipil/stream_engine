package storage

import (
	"context"

	"cloud.google.com/go/firestore"
)

type FirestoreKeyStorage struct {
	ctx    context.Context
	client *firestore.Client
}

func NewFirestoreKeyStorage(ctx context.Context, projectID, database string) (KeyStorage, error) {
	client, err := firestore.NewClientWithDatabase(ctx, projectID, database)
	if err != nil {
		return nil, err
	}
	return &FirestoreKeyStorage{ctx, client}, nil
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
