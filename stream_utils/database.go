package stream_utils

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/cespare/xxhash"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type ref struct {
	name  string
	value string
}
type CounterKey struct {
	refs  []*ref
	field string
}

func (c *CounterKey) Field() string {
	return c.field
}

func (c *CounterKey) Header() []string {
	res := []string{}
	for _, ref := range c.refs {
		res = append(res, ref.name)
	}
	return res
}

func (c *CounterKey) Row() []string {
	// res := map[string]string{}
	vals := []string{}
	for _, ref := range c.refs {
		// res[ref.name] = ref.value
		vals = append(vals, ref.value)
	}
	return vals
}

func (c *CounterKey) ID() uint64 {
	keys := []string{}
	for _, ref := range c.refs {
		keys = append(keys, fmt.Sprintf("%s/%s", ref.name, ref.value))
	}
	return xxhash.Sum64String(strings.Join(keys, "/"))
}

func (c *CounterKey) TableName() string {
	tnames := []string{}
	for _, ref := range c.refs {
		tnames = append(tnames, ref.name)
	}

	return strings.Join(tnames, "_")
}

func NewCounterKey(key string) *CounterKey {
	keys := strings.Split(key, "/")

	refs := []*ref{}
	field := keys[len(keys)-1]
	for i, k := range keys {
		if i%2 == 0 {
			continue
		}

		refs = append(refs, &ref{
			name:  keys[i-1],
			value: k,
		})
	}
	return &CounterKey{refs, field}
}

var tableChecked = map[string]bool{}

func WriteToDatabase(db *gorm.DB, key string, value any) error {
	// var err error

	ckey := NewCounterKey(key)
	id := ckey.ID()
	tablename := ckey.TableName()

	if !tableChecked[tablename] {
		if !db.Migrator().HasTable(tablename) {
			log.Println("create table")
		}
	}

	res := db.
		Table(ckey.TableName()).
		Where("id = ?", id).
		Update(ckey.field, value)

	err := res.Error
	if err != nil {
		if errors.Is(err, gorm.ErrUnsupportedRelation) {
			log.Println("asdasdasdasd")
		}
		return err
	}

	if res.RowsAffected == 0 {
		// insert key
	}

	return nil
}

type LocalDatabase *gorm.DB

func NewDatabaseLocal() LocalDatabase {
	// Use credentials from your Docker Compose file
	dsn := "host=localhost user=myuser password=mypassword dbname=mydatabase port=5432 sslmode=disable"

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	return db
}

func hashKey(key string) uint64 {
	h := xxhash.Sum64String(key)
	return h
}
