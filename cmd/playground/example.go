package main

import (
	"bufio"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"
)

type Int64String int64

func (i *Int64String) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if strings.Contains(s, ".") {
		s = strings.Split(s, ".")[0]
	}
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	*i = Int64String(val)
	return nil
}

type TimeRFC3339 time.Time

func (t *TimeRFC3339) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	parsed, err := time.Parse("2006-01-02 15:04:05.999999 MST", s)
	if err != nil {
		return err
	}

	*t = TimeRFC3339(parsed)
	return nil
}

type Transaction struct {
	ID            Int64String `json:"id"`
	AccountID     Int64String `json:"account_id"`
	TeamID        Int64String `json:"team_id"`
	TransactionID Int64String `json:"transaction_id"`
	CreatedByID   Int64String `json:"created_by_id"`
	EntryTime     TimeRFC3339 `json:"entry_time"`
	Debit         Int64String `json:"debit"`
	Credit        Int64String `json:"credit"`
	Rollback      bool        `json:"rollback"`
	AccountTeamID Int64String `json:"account_team_id"`
	AccountKey    string      `json:"account_key"`
	Coa           string      `json:"coa"`
	BalanceType   string      `json:"balance_type"`
	CanAdjust     bool        `json:"can_adjust"`
	ShopID        Int64String `json:"shop_id"`
}

func iterateExample(fname string, handler func(data *Transaction) error) error {
	file, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	decoder := json.NewDecoder(reader)

	count := 0

	for {
		var record Transaction
		err := decoder.Decode(&record)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		// Process record (example)
		err = handler(&record)
		if err != nil {
			return err
		}

		count++
	}

	return nil

}
