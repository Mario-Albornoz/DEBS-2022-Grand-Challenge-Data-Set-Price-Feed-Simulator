package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/config"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/simulator"
)

func TestEndToEndSimulation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test-data.csv")

	csvContent := `ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,Nominal value,Last,Last volume,Trading time,Total volume
RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67
AAPL.US,E,08-11-2021,09:30:46.123,150.25,2000,150.00,1500,09:30:46,151.00,149.00,USD,09:30:46,151.50,US0378331005,0,148.50,148.00,09:30:00,149.50,1.00,150.10,300,09:30:46,23456.78
MSFT.US,E,08-11-2021,09:30:47.123,250.50,1500,250.25,1000,09:30:47,251.00,249.00,USD,09:30:47,251.50,US5949181045,0,248.50,248.00,09:30:00,249.50,1.00,250.30,200,09:30:47,34567.89
`

	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	cfg := config.Default()
	cfg.Simulator.DataDir = tmpDir
	cfg.Simulator.FilePattern = "test-data.csv"
	cfg.Simulator.Mode = "fullspeed"
	cfg.Performance.ParseWorkers = 2
	cfg.Publisher.Workers = 2
	cfg.Logging.StatsIntervalSec = 60

	sim, err := simulator.NewSimulator(&cfg)
	if err != nil {
		t.Fatalf("Failed to create simulator: %v", err)
	}
	defer sim.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- sim.Start(ctx)
	}()

	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("Simulation failed: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Simulation did not complete in time")
	}
}

func TestMultipleFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	tmpDir := t.TempDir()

	csvContent := `ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,Nominal value,Last,Last volume,Trading time,Total volume
RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67
`

	files := []string{"file1.csv", "file2.csv", "file3.csv"}
	for _, name := range files {
		path := filepath.Join(tmpDir, name)
		if err := os.WriteFile(path, []byte(csvContent), 0644); err != nil {
			t.Fatalf("Failed to write test CSV: %v", err)
		}
	}

	cfg := config.Default()
	cfg.Simulator.DataDir = tmpDir
	cfg.Simulator.FilePattern = "file*.csv"
	cfg.Simulator.Mode = "fullspeed"
	cfg.Logging.StatsIntervalSec = 60

	sim, err := simulator.NewSimulator(&cfg)
	if err != nil {
		t.Fatalf("Failed to create simulator: %v", err)
	}
	defer sim.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- sim.Start(ctx)
	}()

	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("Simulation failed: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Simulation did not complete in time")
	}
}

func TestGracefulShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "large-data.csv")

	header := `ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,Nominal value,Last,Last volume,Trading time,Total volume
`
	content := header
	for i := 0; i < 1000; i++ {
		content += "RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67\n"
	}

	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	cfg := config.Default()
	cfg.Simulator.DataDir = tmpDir
	cfg.Simulator.FilePattern = "large-data.csv"
	cfg.Simulator.Mode = "fullspeed"
	cfg.Logging.StatsIntervalSec = 60

	sim, err := simulator.NewSimulator(&cfg)
	if err != nil {
		t.Fatalf("Failed to create simulator: %v", err)
	}
	defer sim.Stop()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- sim.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("Simulation failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Graceful shutdown did not complete in time")
	}
}

func TestRealtimeMode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "realtime-test.csv")

	csvContent := `ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,Nominal value,Last,Last volume,Trading time,Total volume
RDSA.NL,E,08-11-2021,09:30:00.000,100.75,1000,100.50,500,09:30:00,101.00,99.00,EUR,09:30:00,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:00,12345.67
AAPL.US,E,08-11-2021,09:30:01.000,150.25,2000,150.00,1500,09:30:01,151.00,149.00,USD,09:30:01,151.50,US0378331005,0,148.50,148.00,09:30:00,149.50,1.00,150.10,300,09:30:01,23456.78
MSFT.US,E,08-11-2021,09:30:02.000,250.50,1500,250.25,1000,09:30:02,251.00,249.00,USD,09:30:02,251.50,US5949181045,0,248.50,248.00,09:30:00,249.50,1.00,250.30,200,09:30:02,34567.89
`

	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	cfg := config.Default()
	cfg.Simulator.DataDir = tmpDir
	cfg.Simulator.FilePattern = "realtime-test.csv"
	cfg.Simulator.Mode = "realtime"
	cfg.Logging.StatsIntervalSec = 60

	sim, err := simulator.NewSimulator(&cfg)
	if err != nil {
		t.Fatalf("Failed to create simulator: %v", err)
	}
	defer sim.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- sim.Start(ctx)
	}()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		if err != nil && err != context.Canceled {
			t.Fatalf("Simulation failed: %v", err)
		}
		if elapsed < 2*time.Second {
			t.Errorf("Realtime mode completed too quickly: %v", elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Simulation did not complete in time")
	}
}
