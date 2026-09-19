package anomaly

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

// Injector applies anomalies to the feed based on configuration
type Injector struct {
	config Config
	rng    *rand.Rand
	
	// Per-instrument state tracking
	instrumentState map[string]*InstrumentState
	stateMutex      sync.RWMutex
	
	// Instrument selection (for random sampling)
	selectedInstruments map[string]bool
	phase3Blacklisted   map[string]time.Time // instrument -> blackout end time
	
	// Phase-specific instrument tracking for manifest
	phase1Instruments []string
	phase3Instruments []string
	
	// Timing for manifest
	firstTickTime time.Time
	lastTickTime  time.Time
	
	// Logging
	logFile   *os.File
	logWriter *csv.Writer
	logMutex  sync.Mutex
	
	// Statistics
	stats Stats
}

// InstrumentState tracks historical data for contextual anomalies
type InstrumentState struct {
	ID              string
	PriceHistory    []PricePoint
	LastSeenTime    time.Time
	LastInjectedVal float64 // For stale price anomaly
}

// PricePoint represents a price observation
type PricePoint struct {
	Timestamp       time.Time
	LastTradedPrice float64
	Bid             float64
	Ask             float64
}

// Stats tracks anomaly injection statistics
type Stats struct {
	TotalProcessed     uint64
	Phase1Dropped      uint64
	Phase2Injected     uint64
	Phase3Dropped      uint64
	Phase4Injected     uint64
	
	// Breakdown by type
	PriceSpikes        uint64
	StalePrices        uint64
	BidAskInversions   uint64
	NullPrices         uint64
	MalformedISINs     uint64
	TimestampInversions uint64
}

// NewInjector creates a new anomaly injector
func NewInjector(config Config) (*Injector, error) {
	if !config.Enabled {
		return nil, nil // Disabled, return nil
	}
	
	inj := &Injector{
		config:              config,
		rng:                 rand.New(rand.NewSource(config.Seed)),
		instrumentState:     make(map[string]*InstrumentState),
		selectedInstruments: make(map[string]bool),
		phase3Blacklisted:   make(map[string]time.Time),
	}
	
	// Open log file
	if config.LogFile != "" {
		file, err := os.Create(config.LogFile)
		if err != nil {
			return nil, fmt.Errorf("failed to create anomaly log file: %w", err)
		}
		inj.logFile = file
		inj.logWriter = csv.NewWriter(file)
		
		// Write header
		inj.logWriter.Write([]string{
			"Timestamp", "InstrumentID", "Exchange", "AnomalyType", "Phase",
			"OriginalBid", "OriginalAsk", "OriginalLast",
			"ModifiedBid", "ModifiedAsk", "ModifiedLast",
			"Dropped",
		})
		inj.logWriter.Flush()
	}
	
	return inj, nil
}

// Close closes the injector and flushes logs
func (inj *Injector) Close() error {
	if inj == nil {
		return nil
	}
	
	if inj.logWriter != nil {
		inj.logWriter.Flush()
	}
	
	if inj.logFile != nil {
		return inj.logFile.Close()
	}
	
	return nil
}

// ProcessTick processes a tick and potentially injects anomalies
// Returns: modified tick, should_drop boolean, error
func (inj *Injector) ProcessTick(tick *model.RawTick) (*model.RawTick, bool, error) {
	if inj == nil {
		return tick, false, nil // Injector disabled
	}
	
	inj.stats.TotalProcessed++
	
	// Track timing for manifest
	if inj.firstTickTime.IsZero() {
		inj.firstTickTime = tick.TradingTime
	}
	inj.lastTickTime = tick.TradingTime
	
	// Update instrument state for context tracking
	inj.updateInstrumentState(tick)
	
	marketTime := tick.TradingTime
	originalTick := *tick // Copy for logging
	dropped := false
	
	// Check Phase 3 first (Feed Silence) - highest priority
	if inj.config.Phase3.Enabled {
		if drop, err := inj.phase3FeedSilence(tick, marketTime); err != nil {
			return nil, false, err
		} else if drop {
			dropped = true
			inj.stats.Phase3Dropped++
			inj.logAnomaly(&originalTick, tick, "feed_silence", "phase3", dropped)
			return tick, true, nil // Drop this tick
		}
	}
	
	// Phase 1: Gradual Tick Rate Decline
	if inj.config.Phase1.Enabled {
		if drop, err := inj.phase1TickRateDecline(tick, marketTime); err != nil {
			return nil, false, err
		} else if drop {
			dropped = true
			inj.stats.Phase1Dropped++
			inj.logAnomaly(&originalTick, tick, "tick_rate_decline", "phase1", dropped)
			return tick, true, nil // Drop this tick
		}
	}
	
	// Phase 2: Contextual Price Anomalies
	if inj.config.Phase2.Enabled {
		if injected, err := inj.phase2ContextualAnomalies(tick, marketTime); err != nil {
			return nil, false, err
		} else if injected {
			inj.stats.Phase2Injected++
			inj.logAnomaly(&originalTick, tick, tick.AnomalyType, "phase2", false)
		}
	}
	
	// Phase 4: Sudden Point Failures
	if inj.config.Phase4.Enabled {
		if injected, err := inj.phase4PointFailures(tick, marketTime); err != nil {
			return nil, false, err
		} else if injected {
			inj.stats.Phase4Injected++
			inj.logAnomaly(&originalTick, tick, tick.AnomalyType, "phase4", false)
		}
	}
	
	return tick, dropped, nil
}

// updateInstrumentState tracks price history for contextual anomalies
func (inj *Injector) updateInstrumentState(tick *model.RawTick) {
	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()
	
	state, exists := inj.instrumentState[tick.ID]
	if !exists {
		state = &InstrumentState{
			ID:           tick.ID,
			PriceHistory: make([]PricePoint, 0, 1000),
		}
		inj.instrumentState[tick.ID] = state
	}
	
	// Add price point
	state.PriceHistory = append(state.PriceHistory, PricePoint{
		Timestamp:       tick.TradingTime,
		LastTradedPrice: tick.LastTradedPrice,
		Bid:             tick.Bid,
		Ask:             tick.Ask,
	})
	state.LastSeenTime = tick.TradingTime
	
	// Trim history to context window
	cutoff := tick.TradingTime.Add(-time.Duration(inj.config.Phase2.ContextWindowHours * float64(time.Hour)))
	trimIndex := 0
	for i, point := range state.PriceHistory {
		if point.Timestamp.After(cutoff) {
			trimIndex = i
			break
		}
	}
	if trimIndex > 0 {
		state.PriceHistory = state.PriceHistory[trimIndex:]
	}
}

// getInstrumentState retrieves state for an instrument
func (inj *Injector) getInstrumentState(instrumentID string) *InstrumentState {
	inj.stateMutex.RLock()
	defer inj.stateMutex.RUnlock()
	return inj.instrumentState[instrumentID]
}

// isInstrumentSelected checks if an instrument is selected for anomalies (with caching)
func (inj *Injector) isInstrumentSelected(instrumentID string, ratio float64) bool {
	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()
	
	if selected, exists := inj.selectedInstruments[instrumentID]; exists {
		return selected
	}
	
	// Randomly select based on ratio
	selected := inj.rng.Float64() < ratio
	inj.selectedInstruments[instrumentID] = selected
	return selected
}

// trackPhase1Instrument records an instrument selected for Phase 1
func (inj *Injector) trackPhase1Instrument(instrumentID string) {
	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()
	
	// Check if already tracked
	for _, id := range inj.phase1Instruments {
		if id == instrumentID {
			return
		}
	}
	inj.phase1Instruments = append(inj.phase1Instruments, instrumentID)
}

// trackPhase3Instrument records an instrument selected for Phase 3
func (inj *Injector) trackPhase3Instrument(instrumentID string) {
	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()
	
	// Check if already tracked
	for _, id := range inj.phase3Instruments {
		if id == instrumentID {
			return
		}
	}
	inj.phase3Instruments = append(inj.phase3Instruments, instrumentID)
}

// logAnomaly writes an anomaly event to the log file
func (inj *Injector) logAnomaly(original, modified *model.RawTick, anomalyType, phase string, dropped bool) {
	if inj.logWriter == nil {
		return
	}
	
	inj.logMutex.Lock()
	defer inj.logMutex.Unlock()
	
	droppedStr := "false"
	if dropped {
		droppedStr = "true"
	}
	
	inj.logWriter.Write([]string{
		original.TradingTime.Format("2006-01-02 15:04:05"),
		original.ID,
		original.Exchange,
		anomalyType,
		phase,
		fmt.Sprintf("%.6f", original.Bid),
		fmt.Sprintf("%.6f", original.Ask),
		fmt.Sprintf("%.6f", original.LastTradedPrice),
		fmt.Sprintf("%.6f", modified.Bid),
		fmt.Sprintf("%.6f", modified.Ask),
		fmt.Sprintf("%.6f", modified.LastTradedPrice),
		droppedStr,
	})
	inj.logWriter.Flush()
}

// GetStats returns current injection statistics
func (inj *Injector) GetStats() Stats {
	if inj == nil {
		return Stats{}
	}
	return inj.stats
}

// Manifest represents the ground truth for thesis evaluation
type Manifest struct {
	ExperimentID        string                 `json:"experiment_id"`
	Seed                int64                  `json:"seed"`
	ConfigFile          string                 `json:"config_file,omitempty"`
	StartTime           string                 `json:"start_time"`
	EndTime             string                 `json:"end_time"`
	Phases              map[string]PhaseInfo   `json:"phases"`
	SelectedInstruments map[string][]string    `json:"selected_instruments"`
	Stats               Stats                  `json:"stats"`
}

// PhaseInfo contains metadata about a single phase
type PhaseInfo struct {
	Name                string            `json:"name"`
	Enabled             bool              `json:"enabled"`
	Dates               []string          `json:"dates"`
	Window              map[string]string `json:"window"`
	AffectedInstruments int               `json:"affected_instruments"`
	TotalInjected       uint64            `json:"total_injected,omitempty"`
	TotalDropped        uint64            `json:"total_dropped,omitempty"`
	Breakdown           map[string]uint64 `json:"breakdown,omitempty"`
}

// WriteManifest writes the injection manifest for thesis evaluation
func (inj *Injector) WriteManifest(filepath string) error {
	if inj == nil {
		return nil
	}
	
	manifest := Manifest{
		ExperimentID: fmt.Sprintf("exp_%d", time.Now().Unix()),
		Seed:         inj.config.Seed,
		StartTime:    inj.firstTickTime.Format(time.RFC3339),
		EndTime:      inj.lastTickTime.Format(time.RFC3339),
		Stats:        inj.stats,
		Phases: map[string]PhaseInfo{
			"phase1": {
				Name:                "gradual_decline",
				Enabled:             inj.config.Phase1.Enabled,
				Dates:               inj.config.Phase1.DateFilter,
				Window:              map[string]string{"start": inj.config.Phase1.Window.Start, "end": inj.config.Phase1.Window.End},
				TotalDropped:        inj.stats.Phase1Dropped,
				AffectedInstruments: len(inj.phase1Instruments),
			},
			"phase2": {
				Name:          "contextual_price",
				Enabled:       inj.config.Phase2.Enabled,
				Dates:         inj.config.Phase2.DateFilter,
				Window:        map[string]string{"start": inj.config.Phase2.Window.Start, "end": inj.config.Phase2.Window.End},
				TotalInjected: inj.stats.Phase2Injected,
				Breakdown: map[string]uint64{
					"price_spike":       inj.stats.PriceSpikes,
					"stale_price":       inj.stats.StalePrices,
					"bid_ask_inversion": inj.stats.BidAskInversions,
				},
			},
			"phase3": {
				Name:                "feed_silence",
				Enabled:             inj.config.Phase3.Enabled,
				Dates:               inj.config.Phase3.DateFilter,
				Window:              map[string]string{"start": inj.config.Phase3.Window.Start, "end": inj.config.Phase3.Window.End},
				TotalDropped:        inj.stats.Phase3Dropped,
				AffectedInstruments: len(inj.phase3Instruments),
			},
			"phase4": {
				Name:          "point_failures",
				Enabled:       inj.config.Phase4.Enabled,
				Dates:         inj.config.Phase4.DateFilter,
				Window:        map[string]string{"start": inj.config.Phase4.Window.Start, "end": inj.config.Phase4.Window.End},
				TotalInjected: inj.stats.Phase4Injected,
				Breakdown: map[string]uint64{
					"null_price":           inj.stats.NullPrices,
					"malformed_isin":       inj.stats.MalformedISINs,
					"timestamp_inversion":  inj.stats.TimestampInversions,
				},
			},
		},
		SelectedInstruments: map[string][]string{
			"phase1": inj.phase1Instruments,
			"phase2": []string{"ALL"}, // Phase 2 has no pre-selection
			"phase3": inj.phase3Instruments,
			"phase4": []string{"ALL"}, // Phase 4 has no pre-selection
		},
	}
	
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	
	if err := os.WriteFile(filepath, data, 0644); err != nil {
		return fmt.Errorf("write manifest file: %w", err)
	}
	
	return nil
}

// ===== PHASE IMPLEMENTATIONS =====

// phase1TickRateDecline implements gradual tick rate decline
func (inj *Injector) phase1TickRateDecline(tick *model.RawTick, marketTime time.Time) (bool, error) {
	cfg := inj.config.Phase1
	
	if !isDateInFilter(marketTime, cfg.DateFilter) {
		return false, nil
	}
	
	inWindow, err := cfg.Window.IsInWindow(marketTime)
	if err != nil || !inWindow {
		return false, err
	}
	
	// Check if instrument is selected
	if !inj.isInstrumentSelected(tick.ID, cfg.InstrumentRatio) {
		return false, nil
	}
	
	// Track selected instrument for manifest
	inj.trackPhase1Instrument(tick.ID)
	
	// Calculate drop rate based on position in window
	start, _ := ParseTime(cfg.Window.Start)
	end, _ := ParseTime(cfg.Window.End)
	duration := end.Sub(start).Seconds()
	elapsed := float64(marketTime.Hour()*3600 + marketTime.Minute()*60 + marketTime.Second() - 
		start.Hour()*3600 - start.Minute()*60 - start.Second())
	
	progress := elapsed / duration
	if progress < 0 {
		progress = 0
	}
	if progress > 1 {
		progress = 1
	}
	
	// Calculate current rate
	var currentRate float64
	if cfg.DeclinePattern == "exponential" {
		currentRate = cfg.InitialRate * math.Pow(cfg.FinalRate/cfg.InitialRate, progress)
	} else { // linear
		currentRate = cfg.InitialRate + (cfg.FinalRate-cfg.InitialRate)*progress
	}
	
	// Drop tick if random value exceeds current rate
	shouldKeep := inj.rng.Float64() < currentRate
	return !shouldKeep, nil
}

// phase2ContextualAnomalies implements contextual price anomalies
func (inj *Injector) phase2ContextualAnomalies(tick *model.RawTick, marketTime time.Time) (bool, error) {
	cfg := inj.config.Phase2
	
	if !isDateInFilter(marketTime, cfg.DateFilter) {
		return false, nil
	}
	
	inWindow, err := cfg.Window.IsInWindow(marketTime)
	if err != nil || !inWindow {
		return false, err
	}
	
	state := inj.getInstrumentState(tick.ID)
	if state == nil || len(state.PriceHistory) < 2 {
		return false, nil // Not enough history
	}
	
	// Try each strategy
	for _, strategy := range cfg.Strategies {
		if inj.rng.Float64() < strategy.Probability {
			switch strategy.Type {
			case "price_spike":
				inj.applyPriceSpike(tick, state, strategy)
				inj.stats.PriceSpikes++
				return true, nil
				
			case "stale_price":
				inj.applyStalePrice(tick, state, strategy)
				inj.stats.StalePrices++
				return true, nil
				
			case "bid_ask_inversion":
				inj.applyBidAskInversion(tick)
				inj.stats.BidAskInversions++
				return true, nil
			}
		}
	}
	
	return false, nil
}

// phase3FeedSilence implements complete feed blackout
func (inj *Injector) phase3FeedSilence(tick *model.RawTick, marketTime time.Time) (bool, error) {
	cfg := inj.config.Phase3
	
	if !isDateInFilter(marketTime, cfg.DateFilter) {
		return false, nil
	}
	
	inWindow, err := cfg.Window.IsInWindow(marketTime)
	if err != nil || !inWindow {
		return false, err
	}
	
	// Check exchange filter
	if len(cfg.ExchangeFilter) > 0 {
		matchesFilter := false
		for _, exchange := range cfg.ExchangeFilter {
			if tick.Exchange == exchange {
				matchesFilter = true
				break
			}
		}
		if !matchesFilter {
			return false, nil
		}
	}
	
	// Check if instrument is selected
	if !inj.isInstrumentSelected(tick.ID, cfg.InstrumentRatio) {
		return false, nil
	}
	
	// Track selected instrument for manifest
	inj.trackPhase3Instrument(tick.ID)
	
	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()
	
	// Check if instrument is currently in blackout
	if blackoutEnd, exists := inj.phase3Blacklisted[tick.ID]; exists {
		if marketTime.Before(blackoutEnd) {
			return true, nil // Still in blackout
		}
		// Blackout ended, no more blackouts for this instrument
		return false, nil
	}
	
	// Start blackout for this instrument (only once)
	blackoutEnd := marketTime.Add(time.Duration(cfg.BlackoutSeconds) * time.Second)
	inj.phase3Blacklisted[tick.ID] = blackoutEnd
	
	return true, nil
}

// phase4PointFailures implements sudden point failures
func (inj *Injector) phase4PointFailures(tick *model.RawTick, marketTime time.Time) (bool, error) {
	cfg := inj.config.Phase4
	
	if !isDateInFilter(marketTime, cfg.DateFilter) {
		return false, nil
	}
	
	inWindow, err := cfg.Window.IsInWindow(marketTime)
	if err != nil || !inWindow {
		return false, err
	}
	
	// Try each strategy
	for _, strategy := range cfg.Strategies {
		if inj.rng.Float64() < strategy.Probability {
			switch strategy.Type {
			case "null_price":
				inj.applyNullPrice(tick, strategy)
				inj.stats.NullPrices++
				return true, nil
				
			case "malformed_isin":
				inj.applyMalformedISIN(tick, strategy)
				inj.stats.MalformedISINs++
				return true, nil
				
			case "timestamp_inversion":
				inj.applyTimestampInversion(tick, strategy)
				inj.stats.TimestampInversions++
				return true, nil
			}
		}
	}
	
	return false, nil
}

// ===== ANOMALY APPLICATION HELPERS =====

func (inj *Injector) applyPriceSpike(tick *model.RawTick, state *InstrumentState, strategy ContextualStrategy) {
	// Calculate average from history
	var sum float64
	count := 0
	for _, point := range state.PriceHistory {
		if point.LastTradedPrice > 0 {
			sum += point.LastTradedPrice
			count++
		}
	}
	
	if count == 0 {
		return // No valid history
	}
	
	avg := sum / float64(count)
	minDev := strategy.DeviationRange[0]
	maxDev := strategy.DeviationRange[1]
	deviation := minDev + inj.rng.Float64()*(maxDev-minDev)
	
	tick.LastTradedPrice = avg * deviation
	tick.AnomalyInjected = true
	tick.AnomalyType = "price_spike"
}

func (inj *Injector) applyStalePrice(tick *model.RawTick, state *InstrumentState, strategy ContextualStrategy) {
	// Repeat last injected value or pick from recent history
	if state.LastInjectedVal > 0 {
		tick.LastTradedPrice = state.LastInjectedVal
	} else if len(state.PriceHistory) > 1 {
		// Pick a recent price
		idx := len(state.PriceHistory) - 2
		tick.LastTradedPrice = state.PriceHistory[idx].LastTradedPrice
		state.LastInjectedVal = tick.LastTradedPrice
	}
	
	tick.AnomalyInjected = true
	tick.AnomalyType = "stale_price"
}

func (inj *Injector) applyBidAskInversion(tick *model.RawTick) {
	// Swap Bid and Ask
	tick.Bid, tick.Ask = tick.Ask, tick.Bid
	tick.AnomalyInjected = true
	tick.AnomalyType = "bid_ask_inversion"
}

func (inj *Injector) applyNullPrice(tick *model.RawTick, strategy PointFailureStrategy) {
	field := strategy.Field[inj.rng.Intn(len(strategy.Field))]
	
	switch field {
	case "Bid":
		tick.Bid = 0
	case "Ask":
		tick.Ask = 0
	case "both":
		tick.Bid = 0
		tick.Ask = 0
	}
	
	tick.AnomalyInjected = true
	tick.AnomalyType = "null_price"
}

func (inj *Injector) applyMalformedISIN(tick *model.RawTick, strategy PointFailureStrategy) {
	corruption := strategy.Corruption[inj.rng.Intn(len(strategy.Corruption))]
	
	switch corruption {
	case "truncate":
		if len(tick.ISIN) > 4 {
			tick.ISIN = tick.ISIN[:len(tick.ISIN)/2]
		}
	case "random_chars":
		tick.ISIN += "XXX"
	}
	
	tick.AnomalyInjected = true
	tick.AnomalyType = "malformed_isin"
}

func (inj *Injector) applyTimestampInversion(tick *model.RawTick, strategy PointFailureStrategy) {
	minRewind := strategy.RewindSeconds[0]
	maxRewind := strategy.RewindSeconds[1]
	rewind := minRewind + inj.rng.Intn(maxRewind-minRewind+1)
	
	tick.TradingTime = tick.TradingTime.Add(-time.Duration(rewind) * time.Second)
	tick.AnomalyInjected = true
	tick.AnomalyType = "timestamp_inversion"
}

func isDateInFilter(marketTime time.Time, dateFilter []string) bool {
	if len(dateFilter) == 0 {
		return true
	}
	
	tickDate := marketTime.Format("02-01-2006")
	for _, allowedDate := range dateFilter {
		if tickDate == allowedDate {
			return true
		}
	}
	
	return false
}

