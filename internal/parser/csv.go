// Package parser provides high-performance CSV parsing for DEBS 2022 market data files.
package parser

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/config"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

type ParserStats struct {
	RowsParsed   uint64
	RowsFailed   uint64
	BytesRead    uint64
}

type CSVParser struct {
	config *config.Config
	stats  ParserStats
}

// NewCSVParser creates a new CSV parser with the given configuration.
func NewCSVParser(cfg *config.Config) *CSVParser {
	return &CSVParser{
		config: cfg,
	}
}

// ParseFile reads and parses a CSV file, sending RawTick messages to the output channel.
// Returns when file is fully processed or context is cancelled.
func (p *CSVParser) ParseFile(ctx context.Context, filepath string, output chan<- *model.RawTick) error {
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	bufferSize := p.config.Performance.CSVBufferKB * 1024
	reader := bufio.NewReaderSize(file, bufferSize)

	headerLine, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	atomic.AddUint64(&p.stats.BytesRead, uint64(len(headerLine)))

	lineNum := 1
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("read line %d: %w", lineNum, err)
		}

		atomic.AddUint64(&p.stats.BytesRead, uint64(len(line)))
		lineNum++

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		tick, err := p.parseRow(line)
		if err != nil {
			atomic.AddUint64(&p.stats.RowsFailed, 1)
			continue
		}

		atomic.AddUint64(&p.stats.RowsParsed, 1)

		select {
		case output <- tick:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *CSVParser) parseRow(line string) (*model.RawTick, error) {
	fields := strings.Split(line, ",")
	if len(fields) < 25 {
		return nil, fmt.Errorf("insufficient fields: %d", len(fields))
	}

	tick := AcquireTick()

	tick.ID = fields[0]
	tick.SecType = fields[1]
	tick.Exchange = model.ExtractExchange(tick.ID)
	tick.ISIN = fields[14]

	ask, err := parseFloat(fields[4])
	if err != nil {
		ReleaseTick(tick)
		return nil, fmt.Errorf("parse Ask: %w", err)
	}
	tick.Ask = ask

	bid, err := parseFloat(fields[6])
	if err != nil {
		ReleaseTick(tick)
		return nil, fmt.Errorf("parse Bid: %w", err)
	}
	tick.Bid = bid

	volume, err := parseFloat(fields[24])
	if err != nil {
		ReleaseTick(tick)
		return nil, fmt.Errorf("parse TotalVolume: %w", err)
	}
	tick.TotalVolume = volume

	tradingTime, err := parseTime(fields[23])
	if err != nil {
		ReleaseTick(tick)
		return nil, fmt.Errorf("parse TradingTime: %w", err)
	}
	tick.TradingTime = tradingTime

	date, timeVal, err := parseDateTime(fields[2], fields[3])
	if err != nil {
		ReleaseTick(tick)
		return nil, fmt.Errorf("parse Date/Time: %w", err)
	}
	tick.Date = date
	tick.Time = timeVal

	return tick, nil
}

func parseFloat(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	return strconv.ParseFloat(s, 64)
}

func parseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	return time.Parse("15:04:05", s)
}

func parseDateTime(dateStr, timeStr string) (time.Time, time.Time, error) {
	dateStr = strings.TrimSpace(dateStr)
	timeStr = strings.TrimSpace(timeStr)

	datetime := dateStr + " " + timeStr
	dt, err := time.Parse("02-01-2006 15:04:05.000", datetime)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	dateOnly := time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC)
	return dateOnly, dt, nil
}

// GetStats returns the current parsing statistics.
func (p *CSVParser) GetStats() ParserStats {
	return ParserStats{
		RowsParsed: atomic.LoadUint64(&p.stats.RowsParsed),
		RowsFailed: atomic.LoadUint64(&p.stats.RowsFailed),
		BytesRead:  atomic.LoadUint64(&p.stats.BytesRead),
	}
}
