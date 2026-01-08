package example

//go:generate metric_generate

type Example struct {
	ID       int64  `metric:"id"`
	TeamID   uint64 `metric:"index"`
	UserID   uint64 `metric:"index"`
	ShopName string `metric:"index"`

	LastBalance     float64
	ProductCount    int64
	StockCount      uint64
	ReadyStockCount uint64
}

type ExampleTeam struct {
	ID     int64  `metric:"id"`
	TeamID uint64 `metric:"index"`

	LastBalance     float64
	ProductCount    int64
	StockCount      uint64
	ReadyStockCount uint64
}

type AllAccount struct {
	ID        int64  `metric:"id"`
	AccountID string `metric:"index"`

	LastBalance     float64
	ProductCount    int64
	StockCount      uint64
	ReadyStockCount uint64
}
