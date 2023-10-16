package model

import (
	"time"
)

type (
	SParams struct {
		UnbondingTime     time.Duration `json:"unbonding_time"`      //
		MaxValidators     uint64        `json:"max_validators"`      //
		MaxEntries        uint64        `json:"max_entries"`         //
		HistoricalEntries uint64        `json:"historical_entries"`  //
		BondDenom         string        `json:"bond_denom"`          //
		MinCommissionRate float64       `json:"min_commission_rate"` //
	}

	StakingParams struct {
		Params SParams `json:"params"` //
		Height int64   `json:"height"` //
	}
)
