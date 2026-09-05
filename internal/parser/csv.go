// Package parser provides high-performance CSV parsing for DEBS 2022 market data files.
package parser

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
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
	
	// Field emptiness tracking
	EmptyAsk     uint64
	EmptyBid     uint64
	EmptyVolume  uint64
	
	// Zero value tracking (explicit zeros, not empty)
	ZeroAsk      uint64
	ZeroBid      uint64
	ZeroVolume   uint64
	
	// Error type breakdown
	ErrInsufficientFields uint64
	ErrInvalidTime        uint64
	ErrInvalidDateTime    uint64
	ErrInvalidNumber      uint64
}

type ParseError struct {
	LineNum int
	Type    string
	Message string
}

type CSVParser struct {
	config       *config.Config
	stats        ParserStats
	lastError    ParseError
	errorSample  uint64 // Log every Nth error
}

// NewCSVParser creates a new CSV parser with the given configuration.
func NewCSVParser(cfg *config.Config) *CSVParser {
	return &CSVParser{
		config:      cfg,
		errorSample: 1000, // Log every 1000th error
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

		tick, err := p.parseRow(line, lineNum)
		if err != nil {
			atomic.AddUint64(&p.stats.RowsFailed, 1)
			p.logError(lineNum, err)
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

func (p *CSVParser) parseRow(line string, lineNum int) (*model.RawTick, error) {
	fields := strings.Split(line, ",")
	if len(fields) < 25 {
		atomic.AddUint64(&p.stats.ErrInsufficientFields, 1)
		return nil, fmt.Errorf("insufficient fields: %d", len(fields))
	}

	tick := AcquireTick()

	tick.ID = fields[0]
	tick.SecType = fields[1]
	tick.Exchange = model.ExtractExchange(tick.ID)
	tick.ISIN = fields[14]

	// Track empty/zero fields for Ask
	ask, isEmpty, err := parseFloatWithTracking(fields[4])
	if err != nil {
		ReleaseTick(tick)
		atomic.AddUint64(&p.stats.ErrInvalidNumber, 1)
		return nil, fmt.Errorf("parse Ask: %w", err)
	}
	tick.Ask = ask

	// Track empty/zero fields for Bid
	bid, isBidEmpty, err := parseFloatWithTracking(fields[6])
	if err != nil {
		ReleaseTick(tick)
		atomic.AddUint64(&p.stats.ErrInvalidNumber, 1)
		return nil, fmt.Errorf("parse Bid: %w", err)
	}
	tick.Bid = bid

	// Track empty/zero fields for Volume
	volume, isVolEmpty, err := parseFloatWithTracking(fields[24])
	if err != nil {
		ReleaseTick(tick)
		atomic.AddUint64(&p.stats.ErrInvalidNumber, 1)
		return nil, fmt.Errorf("parse TotalVolume: %w", err)
	}
	tick.TotalVolume = volume

	tradingTime, err := parseTime(fields[23])
	if err != nil {
		// TradingTime is often empty - use Date/Time fields as fallback
		date, timeVal, err := parseDateTime(fields[2], fields[3])
		if err != nil {
			ReleaseTick(tick)
			atomic.AddUint64(&p.stats.ErrInvalidDateTime, 1)
			return nil, fmt.Errorf("parse Date/Time: %w", err)
		}
		tick.Date = date
		tick.Time = timeVal
		tick.TradingTime = timeVal // Use Time field as TradingTime
	} else {
		tick.TradingTime = tradingTime
		
		// Still parse Date/Time for completeness
		date, timeVal, err := parseDateTime(fields[2], fields[3])
		if err != nil {
			ReleaseTick(tick)
			atomic.AddUint64(&p.stats.ErrInvalidDateTime, 1)
			return nil, fmt.Errorf("parse Date/Time: %w", err)
		}
		tick.Date = date
		tick.Time = timeVal
	}

	// Only count empty/zero fields for successfully parsed rows
	if isEmpty {
		atomic.AddUint64(&p.stats.EmptyAsk, 1)
	} else if ask == 0 {
		atomic.AddUint64(&p.stats.ZeroAsk, 1)
	}
	
	if isBidEmpty {
		atomic.AddUint64(&p.stats.EmptyBid, 1)
	} else if bid == 0 {
		atomic.AddUint64(&p.stats.ZeroBid, 1)
	}
	
	if isVolEmpty {
		atomic.AddUint64(&p.stats.EmptyVolume, 1)
	} else if volume == 0 {
		atomic.AddUint64(&p.stats.ZeroVolume, 1)
	}

	return tick, nil
}

func parseFloat(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	return strconv.ParseFloat(s, 64)
}

// parseFloatWithTracking parses a float and tracks whether it was empty
func parseFloatWithTracking(s string) (value float64, isEmpty bool, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, true, nil
	}
	val, err := strconv.ParseFloat(s, 64)
	return val, false, err
}

// logError logs parse errors with sampling to avoid log spam
func (p *CSVParser) logError(lineNum int, err error) {
	failed := atomic.LoadUint64(&p.stats.RowsFailed)
	
	// Log every Nth error or first few errors
	if failed <= 10 || failed%p.errorSample == 0 {
		log.Printf("[WARN] Parse error at line %d (total failed: %d): %v", lineNum, failed, err)
	}
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
		RowsParsed:            atomic.LoadUint64(&p.stats.RowsParsed),
		RowsFailed:            atomic.LoadUint64(&p.stats.RowsFailed),
		BytesRead:             atomic.LoadUint64(&p.stats.BytesRead),
		EmptyAsk:              atomic.LoadUint64(&p.stats.EmptyAsk),
		EmptyBid:              atomic.LoadUint64(&p.stats.EmptyBid),
		EmptyVolume:           atomic.LoadUint64(&p.stats.EmptyVolume),
		ZeroAsk:               atomic.LoadUint64(&p.stats.ZeroAsk),
		ZeroBid:               atomic.LoadUint64(&p.stats.ZeroBid),
		ZeroVolume:            atomic.LoadUint64(&p.stats.ZeroVolume),
		ErrInsufficientFields: atomic.LoadUint64(&p.stats.ErrInsufficientFields),
		ErrInvalidTime:        atomic.LoadUint64(&p.stats.ErrInvalidTime),
		ErrInvalidDateTime:    atomic.LoadUint64(&p.stats.ErrInvalidDateTime),
		ErrInvalidNumber:      atomic.LoadUint64(&p.stats.ErrInvalidNumber),
	}
}
