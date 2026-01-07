package stream_schema

import (
	"fmt"
	"strings"

	"github.com/wargasipil/stream_engine/stream_core"
)

// jangan DIEDIT, file ini generate an dari package github.com/wargasipil/stream_engine

type MetricExample struct {
	key string
	Name string
	store stream_core.KeyStore

	// Jangan Diubah letterlek
	TeamID uint64
	// Jangan Diubah letterlek
	UserID uint64
	// Jangan Diubah letterlek
	Shopname string
}

func NewMetricExample(store stream_core.KeyStore, TeamID uint64, UserID uint64, Shopname string) *MetricExample {
	keys := []string{}
	names := []string{}
	keys = append(keys, fmt.Sprintf("team/%d", TeamID))
	names = append(names, "team")
	keys = append(keys, fmt.Sprintf("user/%d", UserID))
	names = append(names, "user")
	keys = append(keys, fmt.Sprintf("shopname/%s", Shopname))
	names = append(names, "shopname")

	return &MetricExample{
		store: store,
		Name: strings.Join(names, "_"),
		key: strings.Join(keys, "/"),
		TeamID: TeamID,
		UserID: UserID,
		Shopname: Shopname,
	}
}

func (m *MetricExample) PutLastBalance(value float64) float64 {
	return m.store.PutFloat64(m.key + "/last_balance", value)
}

func (m *MetricExample) IncLastBalance(value float64) float64 {
	return m.store.IncFloat64(m.key + "/last_balance", value)
}

func (m *MetricExample) GetLastBalance() float64 {
	return m.store.GetFloat64(m.key + "/last_balance")
}

func (m *MetricExample) PutProductCount(value int64) int64 {
	return m.store.PutInt64(m.key + "/product_count", value)
}

func (m *MetricExample) IncProductCount(value int64) int64 {
	return m.store.IncInt64(m.key + "/product_count", value)
}

func (m *MetricExample) GetProductCount() int64 {
	return m.store.GetInt64(m.key + "/product_count")
}

func (m *MetricExample) PutStockCount(value uint64) uint64 {
	return m.store.PutUint64(m.key + "/stock_count", value)
}

func (m *MetricExample) IncStockCount(value uint64) uint64 {
	return m.store.IncUint64(m.key + "/stock_count", value)
}

func (m *MetricExample) GetStockCount() uint64 {
	return m.store.GetUint64(m.key + "/stock_count")
}

func (m *MetricExample) PutReadyStockCount(value uint64) uint64 {
	return m.store.PutUint64(m.key + "/ready_stock_count", value)
}

func (m *MetricExample) IncReadyStockCount(value uint64) uint64 {
	return m.store.IncUint64(m.key + "/ready_stock_count", value)
}

func (m *MetricExample) GetReadyStockCount() uint64 {
	return m.store.GetUint64(m.key + "/ready_stock_count")
}

func (m *MetricExample) GetKey() string {
	return m.key
}

func (m *MetricExample) Values() map[string]any {
	return map[string]any{
		"ID": stream_core.HashKeyString(m.key),
		"TeamID": m.TeamID,
		"UserID": m.UserID,
		"Shopname": m.Shopname,
		"LastBalance": m.GetLastBalance(),
		"ProductCount": m.GetProductCount(),
		"StockCount": m.GetStockCount(),
		"ReadyStockCount": m.GetReadyStockCount(),
	}
}

func (m *MetricExample) Data() *Example {
	return &Example{
		ID: stream_core.HashKeyString(m.key),
		TeamID: m.TeamID,
		UserID: m.UserID,
		Shopname: m.Shopname,
		LastBalance: m.GetLastBalance(),
		ProductCount: m.GetProductCount(),
		StockCount: m.GetStockCount(),
		ReadyStockCount: m.GetReadyStockCount(),
	}
}

