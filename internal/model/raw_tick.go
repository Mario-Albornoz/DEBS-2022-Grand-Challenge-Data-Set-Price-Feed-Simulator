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

	Bid             float64 `json:"Bid"`
	Ask             float64 `json:"Ask"`
	TotalVolume     float64 `json:"TotalVolume"`
	LastTradedPrice float64 `json:"Last"`

	TradingTime time.Time `json:"TradingTime"`
	Date        time.Time `json:"Date"`
	Time        time.Time `json:"Time"`
	
	// Anomaly injection metadata (omitted when false/empty)
	// Seq numbers the ticks of a run in the order they were read (1, 2, 3, ...). It rides
	// through the feed-handler and the detector to the scores, so a score can be matched to
	// the exact injected message: an instrument's messages often share one millisecond, so
	// (instrument, time) cannot tell them apart.
	Seq uint64 `json:"Seq"`

	AnomalyInjected bool   `json:"anomaly_injected,omitempty"`
	AnomalyType     string `json:"anomaly_type,omitempty"`
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
	if r.LastTradedPrice < 0 {
		return fmt.Errorf("LastTradedPrice must be non-negative, got %f", r.LastTradedPrice)
	}
	if r.TradingTime.IsZero() {
		return errors.New("TradingTime is required")
	}
	return nil
}

// String returns a human-readable representation of the RawTick.
func (r *RawTick) String() string {
	return fmt.Sprintf("RawTick{ID: %s, Exchange: %s, SecType: %s, Bid: %.2f, Ask: %.2f, Last: %.2f, Volume: %.0f, Time: %s}",
		r.ID, r.Exchange, r.SecType, r.Bid, r.Ask, r.LastTradedPrice, r.TotalVolume, r.TradingTime.Format("15:04:05"))
}
