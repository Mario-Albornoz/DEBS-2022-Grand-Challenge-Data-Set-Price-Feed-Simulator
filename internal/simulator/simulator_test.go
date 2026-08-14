package simulator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/config"
)

func TestNewSimulator(t *testing.T) {
	cfg := config.Default()
	sim, err := NewSimulator(&cfg)
	if err != nil {
		t.Fatalf("Failed to create simulator: %v", err)
	}
	defer sim.Stop()

	if sim.config == nil {
		t.Error("Config is nil")
	}
	if sim.parser == nil {
		t.Error("Parser is nil")
	}
	if sim.publisher == nil {
		t.Error("Publisher is nil")
	}
	if sim.timing == nil {
		t.Error("Timing is nil")
	}
}

func TestFindCSVFiles(t *testing.T) {
	tmpDir := t.TempDir()

	testFiles := []string{
		"debs2022-gc-trading-day-01.csv",
		"debs2022-gc-trading-day-02.csv",
		"other-file.txt",
	}

	for _, name := range testFiles {
		path := filepath.Join(tmpDir, name)
		if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	cfg := config.Default()
	cfg.Simulator.DataDir = tmpDir
	cfg.Simulator.FilePattern = "debs2022-gc-trading-day-*.csv"

	sim, err := NewSimulator(&cfg)
	if err != nil {
		t.Fatalf("Failed to create simulator: %v", err)
	}
	defer sim.Stop()

	files, err := sim.findCSVFiles()
	if err != nil {
		t.Fatalf("Failed to find CSV files: %v", err)
	}

	if len(files) != 2 {
		t.Errorf("Found %d files, want 2", len(files))
	}
}

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1234567, "1,234,567"},
		{1000000, "1,000,000"},
	}

	for _, tt := range tests {
		result := formatNumber(tt.input)
		if result != tt.expected {
			t.Errorf("formatNumber(%d) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    uint64
		contains string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
	}

	for _, tt := range tests {
		result := formatBytes(tt.input)
		if result != tt.contains {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.input, result, tt.contains)
		}
	}
}

func TestStopIdempotency(t *testing.T) {
	cfg := config.Default()
	sim, err := NewSimulator(&cfg)
	if err != nil {
		t.Fatalf("Failed to create simulator: %v", err)
	}

	err1 := sim.Stop()
	err2 := sim.Stop()

	if err1 != nil {
		t.Errorf("First Stop() returned error: %v", err1)
	}
	if err2 != nil {
		t.Errorf("Second Stop() returned error: %v", err2)
	}
}

func TestSimulatorWithMockData(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")

	csvContent := `ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,Nominal value,Last,Last volume,Trading time,Total volume
RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67
`

	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	cfg := config.Default()
	cfg.Simulator.DataDir = tmpDir
	cfg.Simulator.FilePattern = "test.csv"
	cfg.Simulator.Mode = "fullspeed"
	cfg.Logging.StatsIntervalSec = 60

	sim, err := NewSimulator(&cfg)
	if err != nil {
		t.Fatalf("Failed to create simulator: %v", err)
	}
	defer sim.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- sim.Start(ctx)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Simulator did not complete in time")
	}
}
