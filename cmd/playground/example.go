package main

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"
)

type EntryRaw struct {
	ID            string `json:"id"`
	AccountID     string `json:"account_id"`
	TeamID        string `json:"team_id"`
	AccountTeamID string `json:"account_team_id"`
	TransactionID string `json:"transaction_id"`
	CreatedByID   string `json:"created_by_id"`
	EntryTime     string `json:"entry_time"`
	Debit         string `json:"debit"`
	Credit        string `json:"credit"`
	Rollback      string `json:"rollback"`
	AccountKey    string `json:"account_key"`
	BalanceType   string `json:"balance_type"`
	CanAdjust     string `json:"can_adjust"`
	ShopID        string `json:"shop_id"`
}

type Entry struct {
	ID            int64
	AccountID     int64
	TeamID        int64
	AccountTeamID int64
	TransactionID int64
	CreatedByID   int64
	EntryTime     time.Time
	Debit         int64
	Credit        int64
	Rollback      bool
	AccountKey    string
	BalanceType   string
	CanAdjust     bool
	ShopID        int64
}

func parseInt(s string) int64 {
	if strings.Contains(s, ".") {
		v, _ := strconv.ParseFloat(s, 64)
		return int64(v * 1000)
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func parseBool(s string) bool {
	v, err := strconv.ParseBool(s)
	if err != nil {
		panic(err)
	}
	return v
}

func parseTime(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05.999999 MST", s)
	if err != nil {
		panic(err)
	}
	return t
}

func getExample(handler func(e Entry) error) error {
	var err error
	// 1. Read file
	data, err := os.ReadFile("example.json")
	if err != nil {
		panic(err)
	}

	// 2. Unmarshal raw JSON
	var raw []EntryRaw
	if err := json.Unmarshal(data, &raw); err != nil {
		panic(err)
	}

	// 3. Convert & iterate
	for _, r := range raw {
		e := Entry{
			ID:            parseInt(r.ID),
			AccountID:     parseInt(r.AccountID),
			TeamID:        parseInt(r.TeamID),
			AccountTeamID: parseInt(r.AccountTeamID),
			TransactionID: parseInt(r.TransactionID),
			CreatedByID:   parseInt(r.CreatedByID),
			EntryTime:     parseTime(r.EntryTime),
			Debit:         parseInt(r.Debit),
			Credit:        parseInt(r.Credit),
			Rollback:      parseBool(r.Rollback),
			AccountKey:    r.AccountKey,
			BalanceType:   r.BalanceType,
			CanAdjust:     parseBool(r.CanAdjust),
			ShopID:        parseInt(r.ShopID),
		}

		err = handler(e)
		if err != nil {
			return err
		}
	}

	return nil
}
