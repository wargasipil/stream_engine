package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/wargasipil/stream_engine/stream_core"
)

// jangan DIEDIT, file ini generate an dari package github.com/wargasipil/stream_engine

type MetricTeamAccount struct {
	key string
	Name string
	store stream_core.KeyStore

	// Jangan Diubah letterlek
	TeamID uint64
	// Jangan Diubah letterlek
	AccountKey string
}

func NewMetricTeamAccount(store stream_core.KeyStore, TeamID uint64, AccountKey string) *MetricTeamAccount {
	keys := []string{}
	names := []string{}
	keys = append(keys, fmt.Sprintf("%d", TeamID))
	names = append(names, "team")
	keys = append(keys, fmt.Sprintf("%s", AccountKey))
	names = append(names, "accountkey")
	key := fmt.Sprintf("%s/%s", strings.Join(names, "_"), strings.Join(keys, "/"))
	Name := strings.Join(names, "_")

	return &MetricTeamAccount{
		store: store,
		Name: Name,
		key: key,
		TeamID: TeamID,
		AccountKey: AccountKey,
	}
}

func NewMetricTeamAccountFromKey(store stream_core.KeyStore, mkey string) (*MetricTeamAccount, error) {

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
	var AccountKey string = indexkeys[1]

	return &MetricTeamAccount{
		store: store,
		Name: Name,
		key: key,
		TeamID: TeamID,
		AccountKey: AccountKey,
	}, nil
}

func IsMetricTeamAccount(key string) bool {
	return strings.HasPrefix(key, "team_accountkey/")
}

func (m *MetricTeamAccount) PutDebit(value float64) float64 {
	return m.store.PutFloat64(m.key + "/debit", value)
}

func (m *MetricTeamAccount) IncDebit(value float64) float64 {
	return m.store.IncFloat64(m.key + "/debit", value)
}

func (m *MetricTeamAccount) GetDebit() float64 {
	return m.store.GetFloat64(m.key + "/debit")
}

func (m *MetricTeamAccount) PutCredit(value float64) float64 {
	return m.store.PutFloat64(m.key + "/credit", value)
}

func (m *MetricTeamAccount) IncCredit(value float64) float64 {
	return m.store.IncFloat64(m.key + "/credit", value)
}

func (m *MetricTeamAccount) GetCredit() float64 {
	return m.store.GetFloat64(m.key + "/credit")
}

func (m *MetricTeamAccount) PutBalance(value float64) float64 {
	return m.store.PutFloat64(m.key + "/balance", value)
}

func (m *MetricTeamAccount) IncBalance(value float64) float64 {
	return m.store.IncFloat64(m.key + "/balance", value)
}

func (m *MetricTeamAccount) GetBalance() float64 {
	return m.store.GetFloat64(m.key + "/balance")
}

func (m *MetricTeamAccount) GetKey() string {
	return m.key
}

func (m *MetricTeamAccount) Values() map[string]any {
	return map[string]any{
		"ID": stream_core.HashKeyString(m.key),
		"TeamID": m.TeamID,
		"AccountKey": m.AccountKey,
		"Debit": m.GetDebit(),
		"Credit": m.GetCredit(),
		"Balance": m.GetBalance(),
	}
}

func (m *MetricTeamAccount) Data() *TeamAccount {
	return &TeamAccount{
		ID: stream_core.HashKeyString(m.key),
		TeamID: m.TeamID,
		AccountKey: m.AccountKey,
		Debit: m.GetDebit(),
		Credit: m.GetCredit(),
		Balance: m.GetBalance(),
	}
}

