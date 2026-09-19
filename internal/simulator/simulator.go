package simulator

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/anomaly"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/config"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/parser"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/publisher"
)

type SimulatorStats struct {
	TicksRead      uint64
	TicksPublished uint64
	TicksFailed    uint64
	TicksDropped   uint64 // Anomalies dropped
	BytesRead      uint64
	StartTime      time.Time
}

type Simulator struct {
	config      *config.Config
	parser      *parser.CSVParser
	publisher   *publisher.KafkaPublisher
	timing      *TimingSimulator
	anomaly     *anomaly.Injector
	stats       SimulatorStats
	stopOnce    sync.Once
	currentFile atomic.Value // stores string of current file being processed
}

// NewSimulator creates a new simulator instance with all required components.
func NewSimulator(cfg *config.Config) (*Simulator, error) {
	pub, err := publisher.NewKafkaPublisher(cfg)
	if err != nil {
		return nil, fmt.Errorf("create publisher: %w", err)
	}

	mode := SimulationMode(cfg.Simulator.Mode)
	timing := NewTimingSimulator(mode, cfg.Simulator.AccelerationFactor)

	// Initialize anomaly injector (can be nil if disabled)
	anomalyInj, err := anomaly.NewInjector(cfg.Anomaly)
	if err != nil {
		return nil, fmt.Errorf("create anomaly injector: %w", err)
	}

	if anomalyInj != nil {
		log.Printf("[INFO] Anomaly injection ENABLED (seed: %d, log: %s)", 
			cfg.Anomaly.Seed, cfg.Anomaly.LogFile)
	}

	return &Simulator{
		config:    cfg,
		parser:    parser.NewCSVParser(cfg),
		publisher: pub,
		timing:    timing,
		anomaly:   anomalyInj,
		stats: SimulatorStats{
			StartTime: time.Now(),
		},
	}, nil
}

// Start begins the simulation, processing all CSV files and publishing to Kafka.
func (s *Simulator) Start(ctx context.Context) error {
	log.Printf("[INFO] Starting price feed simulator in %s mode", s.config.Simulator.Mode)
	log.Printf("[INFO] Connecting to Kafka broker: %v", s.config.Kafka.Brokers)

	files, err := s.findCSVFiles()
	if err != nil {
		return fmt.Errorf("find CSV files: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no CSV files found matching pattern %q in %q",
			s.config.Simulator.FilePattern, s.config.Simulator.DataDir)
	}

	log.Printf("[INFO] Found %d CSV files to process", len(files))
	log.Printf("[INFO] Starting %d parser workers and %d publisher workers",
		s.config.Performance.ParseWorkers, s.config.Publisher.Workers)

	parserChan := make(chan *model.RawTick, s.config.Performance.ChannelBuffer)
	timingChan := make(chan *model.RawTick, 1000)

	// Create cancelable context for workers so we can stop them when done
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.timingWorker(workerCtx, parserChan, timingChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.publisher.Start(workerCtx, timingChan); err != nil {
			log.Printf("[ERROR] Publisher error: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.statsLogger(workerCtx)
	}()

	for _, file := range files {
		if err := s.processFile(ctx, file, parserChan); err != nil {
			log.Printf("[ERROR] Failed to process %s: %v", filepath.Base(file), err)
		}
	}

	close(parserChan)

	// Cancel worker context to stop statsLogger and other workers
	cancelWorkers()

	// Wait for all workers to finish processing
	wg.Wait()

	log.Printf("[INFO] All files processed, simulator shutting down gracefully")

	return nil
}

func (s *Simulator) processFile(ctx context.Context, filepath string, output chan<- *model.RawTick) error {
	filename := filepath[len(filepath)-1:]
	if idx := strings.LastIndex(filepath, "/"); idx >= 0 {
		filename = filepath[idx+1:]
	}

	s.currentFile.Store(filename)
	log.Printf("[INFO] Processing file: %s", filepath)
	return s.parser.ParseFile(ctx, filepath, output)
}

func (s *Simulator) timingWorker(ctx context.Context, input <-chan *model.RawTick, output chan<- *model.RawTick) {
	defer close(output)

	for {
		select {
		case <-ctx.Done():
			return
		case tick, ok := <-input:
			if !ok {
				return
			}

			atomic.AddUint64(&s.stats.TicksRead, 1)

			// Apply anomaly injection if enabled
			if s.anomaly != nil {
				modifiedTick, shouldDrop, err := s.anomaly.ProcessTick(tick)
				if err != nil {
					log.Printf("[WARN] Anomaly injection error for %s: %v", tick.ID, err)
				} else if shouldDrop {
					// Tick dropped by anomaly injector
					atomic.AddUint64(&s.stats.TicksDropped, 1)
					parser.ReleaseTick(tick)
					continue
				}
				tick = modifiedTick
			}

			s.timing.WaitForNextTick(tick)

			select {
			case output <- tick:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *Simulator) statsLogger(ctx context.Context) {
	ticker := time.NewTicker(s.config.StatsInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.PrintStats()
		}
	}
}

func (s *Simulator) findCSVFiles() ([]string, error) {
	pattern := filepath.Join(s.config.Simulator.DataDir, s.config.Simulator.FilePattern)
	return filepath.Glob(pattern)
}

// Stop gracefully shuts down the simulator, flushing any pending messages.
func (s *Simulator) Stop() error {
	var err error
	s.stopOnce.Do(func() {
		if s.anomaly != nil {
			// Write manifest before closing
			manifestPath := "data/injection_manifest.json"
			if manifestErr := s.anomaly.WriteManifest(manifestPath); manifestErr != nil {
				log.Printf("[WARN] Failed to write anomaly manifest: %v", manifestErr)
			} else {
				log.Printf("[INFO] Wrote anomaly manifest to %s", manifestPath)
			}
			
			if closeErr := s.anomaly.Close(); closeErr != nil {
				log.Printf("[WARN] Failed to close anomaly injector: %v", closeErr)
			}
		}
		err = s.publisher.Close()
	})
	return err
}

// PrintStats logs the current simulation statistics.
func (s *Simulator) PrintStats() {
	parserStats := s.parser.GetStats()
	pubStats := s.publisher.GetStats()

	ticksRead := atomic.LoadUint64(&s.stats.TicksRead)
	elapsed := time.Since(s.stats.StartTime).Seconds()
	throughput := float64(pubStats.Published) / elapsed

	currentFile := "N/A"
	if cf := s.currentFile.Load(); cf != nil {
		currentFile = cf.(string)
	}

	log.Println("[INFO] =====================================")
	log.Printf("[INFO] Statistics (%ds interval):", s.config.Logging.StatsIntervalSec)
	log.Printf("[INFO]   Current file:    %s", currentFile)
	log.Printf("[INFO]   Ticks parsed:    %s", formatNumber(parserStats.RowsParsed))
	log.Printf("[INFO]   Ticks sent:      %s", formatNumber(ticksRead))
	log.Printf("[INFO]   Ticks published: %s", formatNumber(pubStats.Published))
	log.Printf("[INFO]   Throughput:      %s ticks/sec", formatNumber(uint64(throughput)))

	// Error breakdown
	totalErrors := pubStats.Failed + parserStats.RowsFailed
	log.Printf("[INFO]   Errors:          %s", formatNumber(totalErrors))
	if totalErrors > 0 {
		log.Printf("[INFO]     Parse errors:  %s", formatNumber(parserStats.RowsFailed))
		if parserStats.ErrInsufficientFields > 0 {
			log.Printf("[INFO]       - Insufficient fields: %s", formatNumber(parserStats.ErrInsufficientFields))
		}
		if parserStats.ErrInvalidTime > 0 {
			log.Printf("[INFO]       - Invalid time:        %s", formatNumber(parserStats.ErrInvalidTime))
		}
		if parserStats.ErrInvalidDateTime > 0 {
			log.Printf("[INFO]       - Invalid date/time:   %s", formatNumber(parserStats.ErrInvalidDateTime))
		}
		if parserStats.ErrInvalidNumber > 0 {
			log.Printf("[INFO]       - Invalid number:      %s", formatNumber(parserStats.ErrInvalidNumber))
		}
		log.Printf("[INFO]     Publish errors: %s", formatNumber(pubStats.Failed))
	}

	// Empty field tracking - use RowsParsed for percentage
	if parserStats.RowsParsed > 0 {
		log.Printf("[INFO]   Empty fields:")
		log.Printf("[INFO]     Ask:    %s (%.1f%%)", formatNumber(parserStats.EmptyAsk), percentage(parserStats.EmptyAsk, parserStats.RowsParsed))
		log.Printf("[INFO]     Bid:    %s (%.1f%%)", formatNumber(parserStats.EmptyBid), percentage(parserStats.EmptyBid, parserStats.RowsParsed))
		log.Printf("[INFO]     Volume: %s (%.1f%%)", formatNumber(parserStats.EmptyVolume), percentage(parserStats.EmptyVolume, parserStats.RowsParsed))

		// Zero value tracking
		log.Printf("[INFO]   Zero values:")
		log.Printf("[INFO]     Ask:    %s (%.1f%%)", formatNumber(parserStats.ZeroAsk), percentage(parserStats.ZeroAsk, parserStats.RowsParsed))
		log.Printf("[INFO]     Bid:    %s (%.1f%%)", formatNumber(parserStats.ZeroBid), percentage(parserStats.ZeroBid, parserStats.RowsParsed))
		log.Printf("[INFO]     Volume: %s (%.1f%%)", formatNumber(parserStats.ZeroVolume), percentage(parserStats.ZeroVolume, parserStats.RowsParsed))
	}

	log.Printf("[INFO]   Bytes read:      %s", formatBytes(parserStats.BytesRead))
	
	// Anomaly statistics
	if s.anomaly != nil {
		anomalyStats := s.anomaly.GetStats()
		ticksDropped := atomic.LoadUint64(&s.stats.TicksDropped)
		
		log.Printf("[INFO]   Anomaly Injection:")
		log.Printf("[INFO]     Ticks dropped:  %s (Phase1: %s, Phase3: %s)", 
			formatNumber(ticksDropped),
			formatNumber(anomalyStats.Phase1Dropped),
			formatNumber(anomalyStats.Phase3Dropped))
		log.Printf("[INFO]     Ticks modified: %s (Phase2: %s, Phase4: %s)", 
			formatNumber(anomalyStats.Phase2Injected + anomalyStats.Phase4Injected),
			formatNumber(anomalyStats.Phase2Injected),
			formatNumber(anomalyStats.Phase4Injected))
		
		if anomalyStats.Phase2Injected > 0 {
			log.Printf("[INFO]       Contextual: Spikes=%s, Stale=%s, Inversions=%s",
				formatNumber(anomalyStats.PriceSpikes),
				formatNumber(anomalyStats.StalePrices),
				formatNumber(anomalyStats.BidAskInversions))
		}
		
		if anomalyStats.Phase4Injected > 0 {
			log.Printf("[INFO]       Point failures: Null=%s, Malformed=%s, TimeInv=%s",
				formatNumber(anomalyStats.NullPrices),
				formatNumber(anomalyStats.MalformedISINs),
				formatNumber(anomalyStats.TimestampInversions))
		}
	}
	
	log.Println("[INFO] =====================================")
}

func percentage(part, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

func formatNumber(n uint64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	return fmt.Sprintf("%s", addCommas(n))
}

func addCommas(n uint64) string {
	s := fmt.Sprintf("%d", n)
	result := ""
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result += ","
		}
		result += string(c)
	}
	return result
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
