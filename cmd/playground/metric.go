package main

//go:generate metric_generate

type TeamAccount struct {
	ID         uint64 `metric:"id" json:"id"`
	TeamID     uint64 `metric:"index" json:"team_id"`
	AccountKey string `metric:"index" json:"account_key"`

	Debit   float64 `json:"debit"`
	Credit  float64 `json:"credit"`
	Balance float64 `json:"balance"`
}
