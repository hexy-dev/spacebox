package model

type DistributionCommission struct {
	Height    int64  `json:"height"`
	Validator string `json:"validator"`
	Amount    string `json:"amount"`
}
