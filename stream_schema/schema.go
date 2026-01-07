package stream_schema

//asdasd
type UserTeam struct {
	UserID      uint64 `metric:"index"`
	TeamID      uint64 `metric:"index"`
	Debit       float64
	Credit      float64
	Balance     int64
	LastBalance float64
}

type TeamAccount struct {
	TeamID      uint64 `metric:"index"`
	Account     string `metric:"index"`
	Balance     int64
	Debit       int64
	Credit      int64
	LastBalance int64
}

// //asdasdsa
// type UserTeamMetric struct {
// 	key string

// 	// index
// 	userID uint64
// 	teamID uint64
// }

// func (m *UserTeamMetric) PutDebit(debit float64) {
// 	panic("implement me")
// }

// func (m *UserTeamMetric) PutCredit(credit float64) {
// 	panic("implement me")
// }

// func (m *UserTeamMetric) GetDebit() float64 {
// 	panic("implement me")
// }

// func (m *UserTeamMetric) GetCredit() float64 {
// 	panic("implement me")
// }

// func (m *UserTeamMetric) IncDebit(debit float64) {
// 	panic("implement me")
// }

// func (m *UserTeamMetric) IncCredit(credit float64) {
// 	panic("implement me")
// }

// func (m *UserTeamMetric) Data() *UserTeam {
// 	panic("implement me")
// }

// func NewUserTeamMetric(kv *stream_core.HashMapCounter, userID uint64, teamID uint64) *UserTeamMetric {
// 	return &UserTeamMetric{
// 		key:    "user/1/team/2",
// 		userID: userID,
// 		teamID: teamID,
// 	}
// }
