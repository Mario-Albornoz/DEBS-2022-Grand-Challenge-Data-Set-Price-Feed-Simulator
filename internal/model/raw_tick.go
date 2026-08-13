// Package model contains all the main structs used across the project
package model

import "time"

type RawTick struct {
	ID       string `json:"ID"`
	Exchange string `json:"Exchange"`
	SecType  string `json:"SecType"`
	ISIN     string `json:"ISIN"`

	Bid         float64 `json:"Bid"`
	Ask         float64 `json:"Ask"`
	TotalVolume float64 `json:"TotalVolume"`

	TradingTime time.Time `json:"TradingTime"`
	Date        time.Time `json:"Date"`
	Time        time.Time `json:"Time"`
}
