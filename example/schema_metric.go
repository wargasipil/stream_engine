package example

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/wargasipil/stream_engine/stream_core"
)

// jangan DIEDIT, file ini generate an dari package github.com/wargasipil/stream_engine

type MetricExample struct {
	key   string
	Name  string
	store stream_core.KeyStore

	// Jangan Diubah letterlek
	TeamID uint64
	// Jangan Diubah letterlek
	UserID uint64
	// Jangan Diubah letterlek
	ShopName string
}

func NewMetricExample(store stream_core.KeyStore, TeamID uint64, UserID uint64, ShopName string) *MetricExample {
	keys := []string{}
	names := []string{}
	keys = append(keys, fmt.Sprintf("%d", TeamID))
	names = append(names, "team")
	keys = append(keys, fmt.Sprintf("%d", UserID))
	names = append(names, "user")
	keys = append(keys, fmt.Sprintf("%s", ShopName))
	names = append(names, "shopname")
	key := fmt.Sprintf("%s/%s", strings.Join(names, "_"), strings.Join(keys, "/"))
	Name := strings.Join(names, "_")

	return &MetricExample{
		store:    store,
		Name:     Name,
		key:      key,
		TeamID:   TeamID,
		UserID:   UserID,
		ShopName: ShopName,
	}
}

func NewMetricExampleFromKey(store stream_core.KeyStore, mkey string) (*MetricExample, error) {

	var err error

	keys := strings.Split(mkey, "/")
	if len(keys) <= 2 {
		return nil, errors.New("key invalid")
	}
	Name := keys[0]
	names := strings.Split(Name, "_")
	indexkeys := keys[1:]
	key := Name + "/" + strings.Join(indexkeys[:len(names)], "/")
	if len(indexkeys) <= 1 {
		return nil, errors.New("index on key invalid")
	}
	var TeamID uint64
	TeamID, err = strconv.ParseUint(indexkeys[0], 10, 64)

	if err != nil {
		return nil, err
	}
	var UserID uint64
	UserID, err = strconv.ParseUint(indexkeys[1], 10, 64)

	if err != nil {
		return nil, err
	}
	var ShopName string = indexkeys[2]

	return &MetricExample{
		store:    store,
		Name:     Name,
		key:      key,
		TeamID:   TeamID,
		UserID:   UserID,
		ShopName: ShopName,
	}, nil
}

func IsMetricExample(key string) bool {
	return strings.HasPrefix(key, "team_user_shopname/")
}

func (m *MetricExample) PutLastBalance(value float64) float64 {
	return m.store.PutFloat64(m.key+"/last_balance", value)
}

func (m *MetricExample) IncLastBalance(value float64) float64 {
	return m.store.IncFloat64(m.key+"/last_balance", value)
}

func (m *MetricExample) GetLastBalance() float64 {
	return m.store.GetFloat64(m.key + "/last_balance")
}

func (m *MetricExample) PutProductCount(value int64) int64 {
	return m.store.PutInt64(m.key+"/product_count", value)
}

func (m *MetricExample) IncProductCount(value int64) int64 {
	return m.store.IncInt64(m.key+"/product_count", value)
}

func (m *MetricExample) GetProductCount() int64 {
	return m.store.GetInt64(m.key + "/product_count")
}

func (m *MetricExample) PutStockCount(value uint64) uint64 {
	return m.store.PutUint64(m.key+"/stock_count", value)
}

func (m *MetricExample) IncStockCount(value uint64) uint64 {
	return m.store.IncUint64(m.key+"/stock_count", value)
}

func (m *MetricExample) GetStockCount() uint64 {
	return m.store.GetUint64(m.key + "/stock_count")
}

func (m *MetricExample) PutReadyStockCount(value uint64) uint64 {
	return m.store.PutUint64(m.key+"/ready_stock_count", value)
}

func (m *MetricExample) IncReadyStockCount(value uint64) uint64 {
	return m.store.IncUint64(m.key+"/ready_stock_count", value)
}

func (m *MetricExample) GetReadyStockCount() uint64 {
	return m.store.GetUint64(m.key + "/ready_stock_count")
}

func (m *MetricExample) GetKey() string {
	return m.key
}

func (m *MetricExample) Values() map[string]any {
	return map[string]any{
		"ID":              stream_core.HashKeyString(m.key),
		"TeamID":          m.TeamID,
		"UserID":          m.UserID,
		"ShopName":        m.ShopName,
		"LastBalance":     m.GetLastBalance(),
		"ProductCount":    m.GetProductCount(),
		"StockCount":      m.GetStockCount(),
		"ReadyStockCount": m.GetReadyStockCount(),
	}
}

func (m *MetricExample) Data() *Example {
	return &Example{
		ID:              stream_core.HashKeyString(m.key),
		TeamID:          m.TeamID,
		UserID:          m.UserID,
		ShopName:        m.ShopName,
		LastBalance:     m.GetLastBalance(),
		ProductCount:    m.GetProductCount(),
		StockCount:      m.GetStockCount(),
		ReadyStockCount: m.GetReadyStockCount(),
	}
}

type MetricExampleTeam struct {
	key   string
	Name  string
	store stream_core.KeyStore

	// Jangan Diubah letterlek
	TeamID uint64
}

func NewMetricExampleTeam(store stream_core.KeyStore, TeamID uint64) *MetricExampleTeam {
	keys := []string{}
	names := []string{}
	keys = append(keys, fmt.Sprintf("%d", TeamID))
	names = append(names, "team")
	key := fmt.Sprintf("%s/%s", strings.Join(names, "_"), strings.Join(keys, "/"))
	Name := strings.Join(names, "_")

	return &MetricExampleTeam{
		store:  store,
		Name:   Name,
		key:    key,
		TeamID: TeamID,
	}
}

func NewMetricExampleTeamFromKey(store stream_core.KeyStore, mkey string) (*MetricExampleTeam, error) {

	var err error

	keys := strings.Split(mkey, "/")
	if len(keys) <= 2 {
		return nil, errors.New("key invalid")
	}
	Name := keys[0]
	names := strings.Split(Name, "_")
	indexkeys := keys[1:]
	key := Name + "/" + strings.Join(indexkeys[:len(names)], "/")
	if len(indexkeys) <= 1 {
		return nil, errors.New("index on key invalid")
	}
	var TeamID uint64
	TeamID, err = strconv.ParseUint(indexkeys[0], 10, 64)

	if err != nil {
		return nil, err
	}

	return &MetricExampleTeam{
		store:  store,
		Name:   Name,
		key:    key,
		TeamID: TeamID,
	}, nil
}

func IsMetricExampleTeam(key string) bool {
	return strings.HasPrefix(key, "team/")
}

func (m *MetricExampleTeam) PutLastBalance(value float64) float64 {
	return m.store.PutFloat64(m.key+"/last_balance", value)
}

func (m *MetricExampleTeam) IncLastBalance(value float64) float64 {
	return m.store.IncFloat64(m.key+"/last_balance", value)
}

func (m *MetricExampleTeam) GetLastBalance() float64 {
	return m.store.GetFloat64(m.key + "/last_balance")
}

func (m *MetricExampleTeam) PutProductCount(value int64) int64 {
	return m.store.PutInt64(m.key+"/product_count", value)
}

func (m *MetricExampleTeam) IncProductCount(value int64) int64 {
	return m.store.IncInt64(m.key+"/product_count", value)
}

func (m *MetricExampleTeam) GetProductCount() int64 {
	return m.store.GetInt64(m.key + "/product_count")
}

func (m *MetricExampleTeam) PutStockCount(value uint64) uint64 {
	return m.store.PutUint64(m.key+"/stock_count", value)
}

func (m *MetricExampleTeam) IncStockCount(value uint64) uint64 {
	return m.store.IncUint64(m.key+"/stock_count", value)
}

func (m *MetricExampleTeam) GetStockCount() uint64 {
	return m.store.GetUint64(m.key + "/stock_count")
}

func (m *MetricExampleTeam) PutReadyStockCount(value uint64) uint64 {
	return m.store.PutUint64(m.key+"/ready_stock_count", value)
}

func (m *MetricExampleTeam) IncReadyStockCount(value uint64) uint64 {
	return m.store.IncUint64(m.key+"/ready_stock_count", value)
}

func (m *MetricExampleTeam) GetReadyStockCount() uint64 {
	return m.store.GetUint64(m.key + "/ready_stock_count")
}

func (m *MetricExampleTeam) GetKey() string {
	return m.key
}

func (m *MetricExampleTeam) Values() map[string]any {
	return map[string]any{
		"ID":              stream_core.HashKeyString(m.key),
		"TeamID":          m.TeamID,
		"LastBalance":     m.GetLastBalance(),
		"ProductCount":    m.GetProductCount(),
		"StockCount":      m.GetStockCount(),
		"ReadyStockCount": m.GetReadyStockCount(),
	}
}

func (m *MetricExampleTeam) Data() *ExampleTeam {
	return &ExampleTeam{
		ID:              stream_core.HashKeyString(m.key),
		TeamID:          m.TeamID,
		LastBalance:     m.GetLastBalance(),
		ProductCount:    m.GetProductCount(),
		StockCount:      m.GetStockCount(),
		ReadyStockCount: m.GetReadyStockCount(),
	}
}
