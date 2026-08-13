// Package model contains all the main structs used across the project
package model

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

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

// ExtractExchange parses the exchange code from an instrument ID.
// Expected format: "SYMBOL.EXCHANGE" (e.g., "RDSA.NL" → "NL", "A2ASZ7.ETR" → "ETR")
func ExtractExchange(id string) string {
	parts := strings.Split(id, ".")
	if len(parts) == 2 {
		return parts[1]
	}
	return "UNKNOWN"
}

// Validate checks if the RawTick has valid field values.
func (r *RawTick) Validate() error {
	if r.ID == "" {
		return errors.New("ID is required")
	}
	if r.SecType == "" {
		return errors.New("SecType is required")
	}
	if r.SecType != "E" && r.SecType != "I" {
		return fmt.Errorf("SecType must be 'E' or 'I', got %q", r.SecType)
	}
	if r.Bid < 0 {
		return fmt.Errorf("Bid must be non-negative, got %f", r.Bid)
	}
	if r.Ask < 0 {
		return fmt.Errorf("Ask must be non-negative, got %f", r.Ask)
	}
	if r.Ask > 0 && r.Bid > 0 && r.Bid > r.Ask {
		return fmt.Errorf("Bid (%f) cannot be greater than Ask (%f)", r.Bid, r.Ask)
	}
	if r.TotalVolume < 0 {
		return fmt.Errorf("TotalVolume must be non-negative, got %f", r.TotalVolume)
	}
	if r.TradingTime.IsZero() {
		return errors.New("TradingTime is required")
	}
	return nil
}

// String returns a human-readable representation of the RawTick.
func (r *RawTick) String() string {
	return fmt.Sprintf("RawTick{ID: %s, Exchange: %s, SecType: %s, Bid: %.2f, Ask: %.2f, Volume: %.0f, Time: %s}",
		r.ID, r.Exchange, r.SecType, r.Bid, r.Ask, r.TotalVolume, r.TradingTime.Format("15:04:05"))
}
