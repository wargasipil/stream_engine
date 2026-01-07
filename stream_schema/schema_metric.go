package stream_schema

import (
	"fmt"
	"strings"

	"github.com/wargasipil/stream_engine/stream_core"
)

// jangan DIEDIT, file ini generate an dari package github.com/wargasipil/stream_engine

type MetricUserTeam struct {
	key string
	Name string
	store stream_core.KeyStore

	// Jangan Diubah letterlek
	UserID uint64
	// Jangan Diubah letterlek
	TeamID uint64
}

func NewMetricUserTeam(store stream_core.KeyStore, UserID uint64, TeamID uint64) *MetricUserTeam {
	keys := []string{}
	names := []string{}
	keys = append(keys, fmt.Sprintf("user/%d", UserID))
	names = append(names, "user")
	keys = append(keys, fmt.Sprintf("team/%d", TeamID))
	names = append(names, "team")

	return &MetricUserTeam{
		store: store,
		Name: strings.Join(names, "_"),
		key: strings.Join(keys, "/"),
		UserID: UserID,
		TeamID: TeamID,
	}
}

func (m *MetricUserTeam) PutDebit(value float64) float64 {
	return m.store.PutFloat64(m.key + "/debit", value)
}

func (m *MetricUserTeam) IncDebit(value float64) float64 {
	return m.store.IncFloat64(m.key + "/debit", value)
}

func (m *MetricUserTeam) GetDebit() float64 {
	return m.store.GetFloat64(m.key + "/debit")
}

func (m *MetricUserTeam) PutCredit(value float64) float64 {
	return m.store.PutFloat64(m.key + "/credit", value)
}

func (m *MetricUserTeam) IncCredit(value float64) float64 {
	return m.store.IncFloat64(m.key + "/credit", value)
}

func (m *MetricUserTeam) GetCredit() float64 {
	return m.store.GetFloat64(m.key + "/credit")
}

func (m *MetricUserTeam) PutBalance(value int64) int64 {
	return m.store.PutInt64(m.key + "/balance", value)
}

func (m *MetricUserTeam) IncBalance(value int64) int64 {
	return m.store.IncInt64(m.key + "/balance", value)
}

func (m *MetricUserTeam) GetBalance() int64 {
	return m.store.GetInt64(m.key + "/balance")
}

func (m *MetricUserTeam) PutLastBalance(value float64) float64 {
	return m.store.PutFloat64(m.key + "/last_balance", value)
}

func (m *MetricUserTeam) IncLastBalance(value float64) float64 {
	return m.store.IncFloat64(m.key + "/last_balance", value)
}

func (m *MetricUserTeam) GetLastBalance() float64 {
	return m.store.GetFloat64(m.key + "/last_balance")
}

func (m *MetricUserTeam) GetKey() string {
	return m.key
}

type MetricTeamAccount struct {
	key string
	Name string
	store stream_core.KeyStore

	// Jangan Diubah letterlek
	TeamID uint64
	// Jangan Diubah letterlek
	Account string
}

func NewMetricTeamAccount(store stream_core.KeyStore, TeamID uint64, Account string) *MetricTeamAccount {
	keys := []string{}
	names := []string{}
	keys = append(keys, fmt.Sprintf("team/%d", TeamID))
	names = append(names, "team")
	keys = append(keys, fmt.Sprintf("account/%s", Account))
	names = append(names, "account")

	return &MetricTeamAccount{
		store: store,
		Name: strings.Join(names, "_"),
		key: strings.Join(keys, "/"),
		TeamID: TeamID,
		Account: Account,
	}
}

func (m *MetricTeamAccount) PutBalance(value int64) int64 {
	return m.store.PutInt64(m.key + "/balance", value)
}

func (m *MetricTeamAccount) IncBalance(value int64) int64 {
	return m.store.IncInt64(m.key + "/balance", value)
}

func (m *MetricTeamAccount) GetBalance() int64 {
	return m.store.GetInt64(m.key + "/balance")
}

func (m *MetricTeamAccount) PutDebit(value int64) int64 {
	return m.store.PutInt64(m.key + "/debit", value)
}

func (m *MetricTeamAccount) IncDebit(value int64) int64 {
	return m.store.IncInt64(m.key + "/debit", value)
}

func (m *MetricTeamAccount) GetDebit() int64 {
	return m.store.GetInt64(m.key + "/debit")
}

func (m *MetricTeamAccount) PutCredit(value int64) int64 {
	return m.store.PutInt64(m.key + "/credit", value)
}

func (m *MetricTeamAccount) IncCredit(value int64) int64 {
	return m.store.IncInt64(m.key + "/credit", value)
}

func (m *MetricTeamAccount) GetCredit() int64 {
	return m.store.GetInt64(m.key + "/credit")
}

func (m *MetricTeamAccount) PutLastBalance(value int64) int64 {
	return m.store.PutInt64(m.key + "/last_balance", value)
}

func (m *MetricTeamAccount) IncLastBalance(value int64) int64 {
	return m.store.IncInt64(m.key + "/last_balance", value)
}

func (m *MetricTeamAccount) GetLastBalance() int64 {
	return m.store.GetInt64(m.key + "/last_balance")
}

func (m *MetricTeamAccount) GetKey() string {
	return m.key
}

