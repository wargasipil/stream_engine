package storage

type KeyStorage interface {
	Increment(path string, field string, delta int64) error
	Close() error
}
