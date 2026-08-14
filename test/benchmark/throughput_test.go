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

func BenchmarkCSVParsing(b *testing.B) {
	tmpDir := b.TempDir()
	csvPath := filepath.Join(tmpDir, "bench.csv")

	header := `ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,Nominal value,Last,Last volume,Trading time,Total volume
`
	content := header
	for i := 0; i < 10000; i++ {
		content += "RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67\n"
	}

	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		b.Fatalf("Failed to write test CSV: %v", err)
	}

	cfg := config.Default()
	p := parser.NewCSVParser(&cfg)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		output := make(chan *model.RawTick, 100)
		ctx := context.Background()

		go func() {
			if err := p.ParseFile(ctx, csvPath, output); err != nil {
				b.Errorf("ParseFile failed: %v", err)
			}
			close(output)
		}()

		count := 0
		for tick := range output {
			count++
			parser.ReleaseTick(tick)
		}
	}
}

func BenchmarkCSVParseRow(b *testing.B) {
	cfg := config.Default()
	p := parser.NewCSVParser(&cfg)
	row := "RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := parseRowBenchHelper(p, row)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func parseRowBenchHelper(p *parser.CSVParser, row string) (*model.RawTick, error) {
	fields := splitCSV(row)
	if len(fields) < 25 {
		return nil, nil
	}
	
	tick := parser.AcquireTick()
	tick.ID = fields[0]
	tick.SecType = fields[1]
	tick.Exchange = model.ExtractExchange(tick.ID)
	parser.ReleaseTick(tick)
	return tick, nil
}

func splitCSV(s string) []string {
	result := make([]string, 0, 40)
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	result = append(result, s[start:])
	return result
}

func BenchmarkObjectPooling(b *testing.B) {
	b.Run("with pool", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			tick := parser.AcquireTick()
			tick.ID = "RDSA.NL"
			tick.Bid = 100.50
			tick.Ask = 100.75
			parser.ReleaseTick(tick)
		}
	})

	b.Run("without pool", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			tick := &model.RawTick{}
			tick.ID = "RDSA.NL"
			tick.Bid = 100.50
			tick.Ask = 100.75
			_ = tick
		}
	})
}

func BenchmarkChannelThroughput(b *testing.B) {
	ch := make(chan *model.RawTick, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case tick := <-ch:
				parser.ReleaseTick(tick)
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tick := parser.AcquireTick()
		tick.ID = "RDSA.NL"
		tick.TradingTime = time.Now()
		ch <- tick
	}
}

func BenchmarkFullPipeline(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping full pipeline benchmark in short mode")
	}

	tmpDir := b.TempDir()
	csvPath := filepath.Join(tmpDir, "pipeline-bench.csv")

	header := `ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,Nominal value,Last,Last volume,Trading time,Total volume
`
	content := header
	for i := 0; i < 1000; i++ {
		content += "RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67\n"
	}

	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		b.Fatalf("Failed to write test CSV: %v", err)
	}

	cfg := config.Default()
	cfg.Simulator.DataDir = tmpDir
	cfg.Simulator.FilePattern = "pipeline-bench.csv"
	cfg.Simulator.Mode = "fullspeed"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := parser.NewCSVParser(&cfg)
		output := make(chan *model.RawTick, 1000)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		go func() {
			if err := p.ParseFile(ctx, csvPath, output); err != nil {
				b.Errorf("ParseFile failed: %v", err)
			}
			close(output)
		}()

		count := 0
		for tick := range output {
			count++
			parser.ReleaseTick(tick)
		}
		cancel()
	}
}

func BenchmarkMemoryAllocations(b *testing.B) {
	cfg := config.Default()
	p := parser.NewCSVParser(&cfg)
	row := "RDSA.NL,E,08-11-2021,09:30:45.123,100.75,1000,100.50,500,09:30:45,101.00,99.00,EUR,09:30:45,101.50,NL0011267392,0,99.50,98.00,09:30:00,100.00,1.00,100.60,250,09:30:45,12345.67"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tick, err := parseRowBenchHelper(p, row)
		if err != nil {
			b.Fatal(err)
		}
		if tick != nil {
			parser.ReleaseTick(tick)
		}
	}
}
