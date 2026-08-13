package model

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestExtractExchange(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		expected string
	}{
		{
			name:     "valid RDSA.NL",
			id:       "RDSA.NL",
			expected: "NL",
		},
		{
			name:     "valid A2ASZ7.ETR",
			id:       "A2ASZ7.ETR",
			expected: "ETR",
		},
		{
			name:     "valid with longer code",
			id:       "AAPL.NASDAQ",
			expected: "NASDAQ",
		},
		{
			name:     "no dot separator",
			id:       "INVALID",
			expected: "UNKNOWN",
		},
		{
			name:     "empty string",
			id:       "",
			expected: "UNKNOWN",
		},
		{
			name:     "multiple dots",
			id:       "A.B.C",
			expected: "UNKNOWN",
		},
		{
			name:     "trailing dot",
			id:       "SYMBOL.",
			expected: "",
		},
		{
			name:     "leading dot",
			id:       ".EXCHANGE",
			expected: "EXCHANGE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractExchange(tt.id)
			if result != tt.expected {
				t.Errorf("ExtractExchange(%q) = %q, want %q", tt.id, result, tt.expected)
			}
		})
	}
}

func TestRawTickValidate(t *testing.T) {
	validTime := time.Now()

	tests := []struct {
		name      string
		tick      RawTick
		wantError bool
	}{
		{
			name: "valid tick",
			tick: RawTick{
				ID:          "RDSA.NL",
				Exchange:    "NL",
				SecType:     "E",
				ISIN:        "NL0011267392",
				Bid:         100.5,
				Ask:         100.7,
				TotalVolume: 1000,
				TradingTime: validTime,
				Date:        validTime,
				Time:        validTime,
			},
			wantError: false,
		},
		{
			name: "empty ID",
			tick: RawTick{
				ID:          "",
				SecType:     "E",
				Bid:         100.5,
				Ask:         100.7,
				TradingTime: validTime,
			},
			wantError: true,
		},
		{
			name: "empty SecType",
			tick: RawTick{
				ID:          "RDSA.NL",
				SecType:     "",
				Bid:         100.5,
				Ask:         100.7,
				TradingTime: validTime,
			},
			wantError: true,
		},
		{
			name: "invalid SecType",
			tick: RawTick{
				ID:          "RDSA.NL",
				SecType:     "X",
				Bid:         100.5,
				Ask:         100.7,
				TradingTime: validTime,
			},
			wantError: true,
		},
		{
			name: "negative Bid",
			tick: RawTick{
				ID:          "RDSA.NL",
				SecType:     "E",
				Bid:         -10.0,
				Ask:         100.7,
				TradingTime: validTime,
			},
			wantError: true,
		},
		{
			name: "negative Ask",
			tick: RawTick{
				ID:          "RDSA.NL",
				SecType:     "E",
				Bid:         100.5,
				Ask:         -100.7,
				TradingTime: validTime,
			},
			wantError: true,
		},
		{
			name: "Bid greater than Ask",
			tick: RawTick{
				ID:          "RDSA.NL",
				SecType:     "E",
				Bid:         100.7,
				Ask:         100.5,
				TradingTime: validTime,
			},
			wantError: true,
		},
		{
			name: "negative TotalVolume",
			tick: RawTick{
				ID:          "RDSA.NL",
				SecType:     "E",
				Bid:         100.5,
				Ask:         100.7,
				TotalVolume: -500,
				TradingTime: validTime,
			},
			wantError: true,
		},
		{
			name: "zero TradingTime",
			tick: RawTick{
				ID:          "RDSA.NL",
				SecType:     "E",
				Bid:         100.5,
				Ask:         100.7,
				TradingTime: time.Time{},
			},
			wantError: true,
		},
		{
			name: "zero Bid and Ask is valid",
			tick: RawTick{
				ID:          "RDSA.NL",
				SecType:     "E",
				Bid:         0,
				Ask:         0,
				TradingTime: validTime,
			},
			wantError: false,
		},
		{
			name: "SecType I (index)",
			tick: RawTick{
				ID:          "RDSA.NL",
				SecType:     "I",
				Bid:         100.5,
				Ask:         100.7,
				TradingTime: validTime,
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tick.Validate()
			if tt.wantError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
		})
	}
}

func TestRawTickString(t *testing.T) {
	tradingTime, _ := time.Parse("15:04:05", "09:30:45")
	tick := RawTick{
		ID:          "RDSA.NL",
		Exchange:    "NL",
		SecType:     "E",
		ISIN:        "NL0011267392",
		Bid:         100.50,
		Ask:         100.75,
		TotalVolume: 12345,
		TradingTime: tradingTime,
	}

	result := tick.String()

	expectedParts := []string{
		"RawTick",
		"ID: RDSA.NL",
		"Exchange: NL",
		"SecType: E",
		"Bid: 100.50",
		"Ask: 100.75",
		"Volume: 12345",
		"Time: 09:30:45",
	}

	for _, part := range expectedParts {
		if !strings.Contains(result, part) {
			t.Errorf("String() missing expected part %q in result: %s", part, result)
		}
	}
}

func TestRawTickJSONSerialization(t *testing.T) {
	tradingTime := time.Date(2021, 11, 8, 9, 30, 45, 0, time.UTC)
	date := time.Date(2021, 11, 8, 0, 0, 0, 0, time.UTC)
	timeVal := time.Date(2021, 11, 8, 9, 30, 45, 0, time.UTC)

	original := RawTick{
		ID:          "RDSA.NL",
		Exchange:    "NL",
		SecType:     "E",
		ISIN:        "NL0011267392",
		Bid:         100.50,
		Ask:         100.75,
		TotalVolume: 12345.67,
		TradingTime: tradingTime,
		Date:        date,
		Time:        timeVal,
	}

	jsonData, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal RawTick: %v", err)
	}

	var decoded RawTick
	err = json.Unmarshal(jsonData, &decoded)
	if err != nil {
		t.Fatalf("Failed to unmarshal RawTick: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID mismatch: got %q, want %q", decoded.ID, original.ID)
	}
	if decoded.Exchange != original.Exchange {
		t.Errorf("Exchange mismatch: got %q, want %q", decoded.Exchange, original.Exchange)
	}
	if decoded.SecType != original.SecType {
		t.Errorf("SecType mismatch: got %q, want %q", decoded.SecType, original.SecType)
	}
	if decoded.ISIN != original.ISIN {
		t.Errorf("ISIN mismatch: got %q, want %q", decoded.ISIN, original.ISIN)
	}
	if decoded.Bid != original.Bid {
		t.Errorf("Bid mismatch: got %f, want %f", decoded.Bid, original.Bid)
	}
	if decoded.Ask != original.Ask {
		t.Errorf("Ask mismatch: got %f, want %f", decoded.Ask, original.Ask)
	}
	if decoded.TotalVolume != original.TotalVolume {
		t.Errorf("TotalVolume mismatch: got %f, want %f", decoded.TotalVolume, original.TotalVolume)
	}
}

func TestRawTickJSONDeserialization(t *testing.T) {
	jsonStr := `{
		"ID": "RDSA.NL",
		"Exchange": "NL",
		"SecType": "E",
		"ISIN": "NL0011267392",
		"Bid": 100.50,
		"Ask": 100.75,
		"TotalVolume": 12345.67,
		"TradingTime": "2021-11-08T09:30:45Z",
		"Date": "2021-11-08T00:00:00Z",
		"Time": "2021-11-08T09:30:45Z"
	}`

	var tick RawTick
	err := json.Unmarshal([]byte(jsonStr), &tick)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if tick.ID != "RDSA.NL" {
		t.Errorf("ID = %q, want %q", tick.ID, "RDSA.NL")
	}
	if tick.Exchange != "NL" {
		t.Errorf("Exchange = %q, want %q", tick.Exchange, "NL")
	}
	if tick.Bid != 100.50 {
		t.Errorf("Bid = %f, want %f", tick.Bid, 100.50)
	}
	if tick.Ask != 100.75 {
		t.Errorf("Ask = %f, want %f", tick.Ask, 100.75)
	}
}
