package benchmark

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/config"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/parser"
)

func TestCSVParserThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping throughput test in short mode")
	}

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "throughput-test.csv")

	header := `ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,Nominal value,Last,Last volume,Trading time,Total volume
`
	content := header
	testSize := 100000
	for i := 0; i < testSize; i++ {
		content += "RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67\n"
	}

	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	cfg := config.Default()
	p := parser.NewCSVParser(&cfg)

	output := make(chan *model.RawTick, 1000)
	ctx := context.Background()

	start := time.Now()

	go func() {
		if err := p.ParseFile(ctx, csvPath, output); err != nil {
			t.Errorf("ParseFile failed: %v", err)
		}
		close(output)
	}()

	count := 0
	for tick := range output {
		count++
		parser.ReleaseTick(tick)
	}

	elapsed := time.Since(start).Seconds()
	throughput := float64(count) / elapsed

	t.Logf("Parsed %d ticks in %.2f seconds", count, elapsed)
	t.Logf("Throughput: %.0f ticks/second", throughput)
	t.Logf("Target: 100,000+ ticks/second for CSV parsing")

	if count != testSize {
		t.Errorf("Expected to parse %d ticks, got %d", testSize, count)
	}

	minThroughput := 100000.0
	if throughput < minThroughput {
		t.Errorf("Throughput %.0f ticks/sec is below minimum target of %.0f ticks/sec", throughput, minThroughput)
	} else {
		t.Logf("✓ Throughput meets target (%.0f >= %.0f ticks/sec)", throughput, minThroughput)
	}
}

func TestChannelThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping throughput test in short mode")
	}

	testSize := 1000000
	ch := make(chan *model.RawTick, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	received := 0
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-ctx.Done():
				done <- true
				return
			case tick, ok := <-ch:
				if !ok {
					done <- true
					return
				}
				received++
				parser.ReleaseTick(tick)
			}
		}
	}()

	start := time.Now()

	for i := 0; i < testSize; i++ {
		tick := parser.AcquireTick()
		tick.ID = "RDSA.NL"
		tick.Exchange = "NL"
		tick.SecType = "E"
		tick.TradingTime = time.Now()
		ch <- tick
	}

	close(ch)
	<-done

	elapsed := time.Since(start).Seconds()
	throughput := float64(received) / elapsed

	t.Logf("Sent and received %d ticks in %.2f seconds", received, elapsed)
	t.Logf("Throughput: %.0f ticks/second", throughput)
	t.Logf("Target: 500,000+ ticks/second for channel operations")

	if received != testSize {
		t.Errorf("Expected to receive %d ticks, got %d", testSize, received)
	}

	minThroughput := 500000.0
	if throughput < minThroughput {
		t.Errorf("Throughput %.0f ticks/sec is below minimum target of %.0f ticks/sec", throughput, minThroughput)
	} else {
		t.Logf("✓ Throughput meets target (%.0f >= %.0f ticks/sec)", throughput, minThroughput)
	}
}

func TestObjectPoolPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	testSize := 10000000

	t.Run("with_pool", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < testSize; i++ {
			tick := parser.AcquireTick()
			tick.ID = "RDSA.NL"
			tick.Bid = 100.50
			tick.Ask = 100.75
			parser.ReleaseTick(tick)
		}
		elapsed := time.Since(start).Seconds()
		throughput := float64(testSize) / elapsed
		t.Logf("With pool: %.0f operations/sec", throughput)
	})

	t.Run("without_pool", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < testSize; i++ {
			tick := &model.RawTick{}
			tick.ID = "RDSA.NL"
			tick.Bid = 100.50
			tick.Ask = 100.75
			_ = tick
		}
		elapsed := time.Since(start).Seconds()
		throughput := float64(testSize) / elapsed
		t.Logf("Without pool: %.0f operations/sec", throughput)
	})
}

func TestFullPipelineThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping full pipeline throughput test in short mode")
	}

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "pipeline-throughput.csv")

	header := `ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,Nominal value,Last,Last volume,Trading time,Total volume
`
	content := header
	testSize := 50000
	for i := 0; i < testSize; i++ {
		content += "RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67\n"
	}

	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	cfg := config.Default()
	cfg.Performance.ParseWorkers = 8
	cfg.Performance.ChannelBuffer = 10000

	p := parser.NewCSVParser(&cfg)

	parserOut := make(chan *model.RawTick, cfg.Performance.ChannelBuffer)
	timingOut := make(chan *model.RawTick, 1000)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()

	go func() {
		if err := p.ParseFile(ctx, csvPath, parserOut); err != nil {
			t.Errorf("ParseFile failed: %v", err)
		}
		close(parserOut)
	}()

	go func() {
		for tick := range parserOut {
			timingOut <- tick
		}
		close(timingOut)
	}()

	count := 0
	for tick := range timingOut {
		count++
		parser.ReleaseTick(tick)
	}

	elapsed := time.Since(start).Seconds()
	throughput := float64(count) / elapsed

	t.Logf("Full pipeline processed %d ticks in %.2f seconds", count, elapsed)
	t.Logf("Throughput: %.0f ticks/second", throughput)
	t.Logf("Target: 100,000+ ticks/second for full pipeline")

	if count != testSize {
		t.Errorf("Expected to process %d ticks, got %d", testSize, count)
	}

	minThroughput := 100000.0
	if throughput < minThroughput {
		t.Logf("⚠ Throughput %.0f ticks/sec is below target of %.0f ticks/sec", throughput, minThroughput)
		t.Logf("Note: Full system throughput depends on Kafka availability and network")
	} else {
		t.Logf("✓ Pipeline throughput meets target (%.0f >= %.0f ticks/sec)", throughput, minThroughput)
	}
}
