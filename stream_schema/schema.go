package stream_schema

//go:generate metric_generate

type Example struct {
	ID       uint64 `metric:"id"`
	TeamID   uint64 `metric:"index"`
	UserID   uint64 `metric:"index"`
	Shopname string `metric:"index"`

	LastBalance     float64
	ProductCount    int64
	StockCount      uint64
	ReadyStockCount uint64
}
