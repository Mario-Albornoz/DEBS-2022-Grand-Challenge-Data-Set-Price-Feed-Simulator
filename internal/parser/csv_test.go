package parser

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/config"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

func TestParseRow(t *testing.T) {
	cfg := config.Default()
	parser := NewCSVParser(&cfg)

	validRow := "RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67"

	tick, err := parser.parseRow(validRow, 1)
	if err != nil {
		t.Fatalf("Failed to parse valid row: %v", err)
	}
	defer ReleaseTick(tick)

	if tick.ID != "RDSA.NL" {
		t.Errorf("ID = %q, want %q", tick.ID, "RDSA.NL")
	}
	if tick.Exchange != "NL" {
		t.Errorf("Exchange = %q, want %q", tick.Exchange, "NL")
	}
	if tick.SecType != "E" {
		t.Errorf("SecType = %q, want %q", tick.SecType, "E")
	}
	if tick.ISIN != "NL0011267392" {
		t.Errorf("ISIN = %q, want %q", tick.ISIN, "NL0011267392")
	}
	if tick.Ask != 100.75 {
		t.Errorf("Ask = %f, want %f", tick.Ask, 100.75)
	}
	if tick.Bid != 100.50 {
		t.Errorf("Bid = %f, want %f", tick.Bid, 100.50)
	}
	if tick.LastTradedPrice != 100.60 {
		t.Errorf("LastTradedPrice = %f, want %f", tick.LastTradedPrice, 100.60)
	}
	if tick.TotalVolume != 12345.67 {
		t.Errorf("TotalVolume = %f, want %f", tick.TotalVolume, 12345.67)
	}
	if tick.TradingTime.Format("15:04:05") != "09:30:45" {
		t.Errorf("TradingTime = %s, want %s", tick.TradingTime.Format("15:04:05"), "09:30:45")
	}
}

func TestParseRowInvalidCases(t *testing.T) {
	cfg := config.Default()
	parser := NewCSVParser(&cfg)

	tests := []struct {
		name string
		row  string
	}{
		{"insufficient fields", "A,B,C"},
		{"invalid Ask", "ID,E,08-11-2021,09:30:45.123,INVALID,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,ISIN,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67"},
		{"invalid Bid", "ID,E,08-11-2021,09:30:45.123,100.75,1000,INVALID,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,ISIN,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67"},
		{"invalid LastTradedPrice", "ID,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,ISIN,0,99.50,98.00,09:30:00,100.00,1.00,INVALID,250,09:30:45,12345.67"},
		{"invalid TotalVolume", "ID,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,ISIN,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,INVALID"},
		{"invalid Date/Time", "ID,E,INVALID,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,ISIN,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tick, err := parser.parseRow(tt.row, 1)
			if err == nil {
				if tick != nil {
					ReleaseTick(tick)
				}
				t.Error("Expected error, got nil")
			}
		})
	}
}

func TestParseFile(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")

	csvContent := `ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,Nominal value,Last,Last volume,Trading time,Total volume,Mid price,Trading date,Profit,Current price,Related indices,Day high bid time,Day low bid time,Open Time,Last trade time,Close Time,Day high Time,Day low Time,Bid time,Auction Time
RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67,100.625,08-11-2021,0,100.60,IDX1,09:30:45,09:30:00,09:00:00,09:30:45,16:00:00,09:30:45,09:30:00,09:30:45,09:00:00
AAPL.US,E,08-11-2021,09:31:00.000,150.25,2000,150.00,1500,09:31:00,151.00,149.00,USD,09:31:00,151.50,US0378331005,0,148.50,148.00,09:30:00,149.50,1.00,150.10,300,09:31:00,23456.78,150.125,08-11-2021,0,150.10,IDX2,09:31:00,09:30:00,09:00:00,09:31:00,16:00:00,09:31:00,09:30:00,09:31:00,09:00:00
`

	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	cfg := config.Default()
	parser := NewCSVParser(&cfg)

	output := make(chan *model.RawTick, 10)
	ctx := context.Background()

	go func() {
		if err := parser.ParseFile(ctx, csvPath, output); err != nil {
			t.Errorf("ParseFile failed: %v", err)
		}
		close(output)
	}()

	ticks := []*model.RawTick{}
	for tick := range output {
		ticks = append(ticks, tick)
	}

	if len(ticks) != 2 {
		t.Fatalf("Expected 2 ticks, got %d", len(ticks))
	}

	if ticks[0].ID != "RDSA.NL" {
		t.Errorf("First tick ID = %q, want %q", ticks[0].ID, "RDSA.NL")
	}
	if ticks[1].ID != "AAPL.US" {
		t.Errorf("Second tick ID = %q, want %q", ticks[1].ID, "AAPL.US")
	}

	for _, tick := range ticks {
		ReleaseTick(tick)
	}
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		wantErr  bool
	}{
		{"09:30:45", "09:30:45", false},
		{"14:25:30", "14:25:30", false},
		{"00:00:00", "00:00:00", false},
		{"23:59:59", "23:59:59", false},
		{"", "", true},
		{"invalid", "", true},
		{"25:00:00", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseTime(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if result.Format("15:04:05") != tt.expected {
					t.Errorf("parseTime(%q) = %s, want %s", tt.input, result.Format("15:04:05"), tt.expected)
				}
			}
		})
	}
}

func TestParseDateTime(t *testing.T) {
	date, timeVal, err := parseDateTime("08-11-2021", "09:30:45.123")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if date.Year() != 2021 || date.Month() != 11 || date.Day() != 8 {
		t.Errorf("Date = %v, want 2021-11-08", date)
	}

	if timeVal.Hour() != 9 || timeVal.Minute() != 30 || timeVal.Second() != 45 {
		t.Errorf("Time = %v, want 09:30:45", timeVal)
	}
}

func BenchmarkParseRow(b *testing.B) {
	cfg := config.Default()
	parser := NewCSVParser(&cfg)
	row := "RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick, err := parser.parseRow(row, i)
		if err != nil {
			b.Fatal(err)
		}
		ReleaseTick(tick)
	}
}

func BenchmarkParseTime(b *testing.B) {
	timeStr := "09:30:45"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := parseTime(timeStr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseDateTime(b *testing.B) {
	dateStr := "08-11-2021"
	timeStr := "09:30:45.123"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := parseDateTime(dateStr, timeStr)
		if err != nil {
			b.Fatal(err)
		}
	}
}
