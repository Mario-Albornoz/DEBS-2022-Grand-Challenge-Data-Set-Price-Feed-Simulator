package anomaly

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	selectedInstruments map[string]bool // keyed "phase|instrument" so phases select independently
	phase3Blacklisted   map[string]time.Time // instrument -> blackout end time
	
	// Episode ground truth (phase 1 and 3 accumulate here and are written on Close)
	phase1Episodes map[string]*phase1Episode // "date|instrument" -> episode
	phase3Episodes map[string]*Episode       // instrument -> blackout episode
	
	// Timing for manifest
	firstTickTime time.Time
	lastTickTime  time.Time
	
	// Logging
	logFile   *os.File
	logWriter *csv.Writer
	logMutex  sync.Mutex

	// Per-instrument-day activity summary
	instrumentFile   *os.File
	instrumentWriter *csv.Writer
	instrumentDays   []InstrumentDay

	// Sum of the phase's price-anomaly probabilities, the base the quota scales from
	phase2BaseP float64
	phase4BaseP float64

	// Episode-level ground truth (one row per anomaly episode)
	episodeFile   *os.File
	episodeWriter *csv.Writer
	episodePath   string
	episodeCount  uint64
	
	// Statistics
	stats Stats
}

// InstrumentState tracks historical data for contextual anomalies
type InstrumentState struct {
	ID              string
	PriceHistory    []PricePoint
	LastSeenTime    time.Time
	LastDelivered   time.Time // last tick passed downstream (phase 3 ground truth)
	Delivered       uint64    // ticks passed downstream so far (phase 3 warm-up ground truth)
	// LastDeliveredClock is the update time (whole seconds) of the last delivered tick.
	// The feed-handler's timing and silence run on that clock, so a silence alert's
	// LastSeen equals this value, not LastDelivered (TradingTime).
	LastDeliveredClock time.Time

	// Activity counters for the per-instrument quota and the instrument-day summary
	// (see quota.go).
	Exchange  string
	SecType   string
	Day       string
	DayRows   int
	DayTrades int
	Windows   map[string]*windowTrades
	Stale           *staleRun // stale-price run in progress, if any
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
	PriceDeviations    uint64
	NullPrices         uint64
	ImplausiblePrices  uint64
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
		phase1Episodes:      make(map[string]*phase1Episode),
		phase3Episodes:      make(map[string]*Episode),
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
			"Dropped", "ObservedTimestamp",
		})
		inj.logWriter.Flush()
	}
	
	for _, st := range config.Phase2.Strategies {
		inj.phase2BaseP += st.Probability
	}
	for _, st := range config.Phase4.Strategies {
		if st.Type == "implausible_price" {
			inj.phase4BaseP += st.Probability
		}
	}

	instrumentPath := config.InstrumentFile
	if instrumentPath == "" && config.LogFile != "" {
		instrumentPath = strings.TrimSuffix(config.LogFile, filepath.Ext(config.LogFile)) + "_instruments.csv"
	}
	if instrumentPath != "" {
		file, err := os.Create(instrumentPath)
		if err != nil {
			if inj.logFile != nil {
				inj.logFile.Close()
			}
			return nil, fmt.Errorf("failed to create instrument summary file: %w", err)
		}
		inj.instrumentFile = file
		inj.instrumentWriter = csv.NewWriter(file)
		inj.instrumentWriter.Write(instrumentDayHeader)
		inj.instrumentWriter.Flush()
	}

	episodePath := config.EpisodeFile
	if episodePath == "" && config.LogFile != "" {
		episodePath = strings.TrimSuffix(config.LogFile, filepath.Ext(config.LogFile)) + "_episodes.csv"
	}
	if episodePath != "" {
		file, err := os.Create(episodePath)
		if err != nil {
			if inj.logFile != nil {
				inj.logFile.Close()
			}
			return nil, fmt.Errorf("failed to create anomaly episode file: %w", err)
		}
		inj.episodeFile = file
		inj.episodeWriter = csv.NewWriter(file)
		inj.episodePath = episodePath
		inj.episodeWriter.Write(episodeHeader)
		inj.episodeWriter.Flush()
	}

	return inj, nil
}

// Close closes the injector and flushes logs
func (inj *Injector) Close() error {
	if inj == nil {
		return nil
	}
	
	inj.finalizeEpisodes()
	inj.finalizeInstrumentDays()
	inj.warnIfIdle()

	if inj.instrumentFile != nil {
		inj.instrumentFile.Close()
	}

	if inj.logWriter != nil {
		inj.logWriter.Flush()
	}

	if inj.episodeWriter != nil {
		inj.episodeWriter.Flush()
	}

	if inj.episodeFile != nil {
		inj.episodeFile.Close()
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

	// Only equities reach the detector; the feed-handler discards index rows. Index rows
	// are nearly all "trades" (they always carry a value) and outnumber equity trades six
	// to one, so injecting into them would waste most of the budget and skew the
	// per-instrument quota.
	if tick.SecType == "I" {
		return tick, false, nil
	}
	
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
		if injected, ep, err := inj.phase2ContextualAnomalies(tick, marketTime); err != nil {
			return nil, false, err
		} else if injected {
			inj.stats.Phase2Injected++
			inj.logAnomaly(&originalTick, tick, tick.AnomalyType, "phase2", false)
			inj.emitEpisode(ep)
		}
	}
	
	// Phase 4: Sudden Point Failures
	if inj.config.Phase4.Enabled {
		if injected, ep, err := inj.phase4PointFailures(tick, marketTime); err != nil {
			return nil, false, err
		} else if injected {
			inj.stats.Phase4Injected++
			inj.logAnomaly(&originalTick, tick, tick.AnomalyType, "phase4", false)
			inj.emitEpisode(ep)
		}
	}
	
	if inj.config.Phase3.Enabled {
		inj.markDelivered(tick.ID, tick.TradingTime, tick.Time)
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
	inj.countRow(state, tick)
	
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
func (inj *Injector) isInstrumentSelected(phase, instrumentID string, ratio float64) bool {
	key := phase + "|" + instrumentID
	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()
	
	if selected, exists := inj.selectedInstruments[key]; exists {
		return selected
	}
	
	// Randomly select based on ratio
	selected := inj.rng.Float64() < ratio
	inj.selectedInstruments[key] = selected
	return selected
}

// markDelivered records the last tick passed downstream for an instrument.
func (inj *Injector) markDelivered(instrumentID string, tradingTime, clockTime time.Time) {
	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()

	if state := inj.instrumentState[instrumentID]; state != nil {
		state.LastDelivered = tradingTime
		state.LastDeliveredClock = clockTime
		state.Delivered++
	}
}

// logAnomaly writes a tick-level anomaly event to the log file.
// Timestamp is the original event time; ObservedTimestamp is the (possibly
// modified) time the downstream sees. Both carry millisecond precision.
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

	const tsFormat = "2006-01-02 15:04:05.000"
	inj.logWriter.Write([]string{
		original.TradingTime.Format(tsFormat),
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
		modified.TradingTime.Format(tsFormat),
	})
	inj.logWriter.Flush()
}

// ===== EPISODE GROUND TRUTH =====

// Episode is one row of the episode-level ground truth file. All times are event
// time (TradingTime) and are written as epoch milliseconds.
//
//   - phase1: one row per affected instrument and day; Start/End are the window bounds.
//   - phase2 stale_price: one row per run; Start/End are its first and last tick.
//   - phase2 spike/deviation, phase4: one row per injected tick (Start == End).
//   - phase3: one row per blackout; Start is the first dropped tick, End the scheduled
//     end, LastDelivered the last tick the feed delivered before it and Resume the first
//     tick delivered after it (empty if the instrument never ticked again).
type Episode struct {
	Phase         string
	AnomalyType   string
	Exchange      string
	InstrumentID  string
	SecType       string // "E" (equity) or "I" (index); the detector only sees equities
	Start         time.Time
	End           time.Time
	Observed      time.Time // timestamp the downstream sees (single-tick anomalies)
	Resume        time.Time // phase3 only
	LastDelivered time.Time // phase3 only
	Detail        string    // semicolon-separated key=value pairs
	Seq           uint64    // sequence number of the injected message (of the first message for a run)
}

var episodeHeader = []string{
	"EpisodeID", "Phase", "AnomalyType", "Exchange", "InstrumentID",
	"StartMs", "EndMs", "ObservedMs", "ResumeMs", "LastDeliveredMs", "Detail", "SecType", "Seq",
}

// phase1Episode accumulates counters for one instrument on one day.
type phase1Episode struct {
	ep           Episode
	ticksSeen    uint64
	ticksDropped uint64
}

// staleRun is a stale-price anomaly in progress for one instrument.
type staleRun struct {
	value     float64
	total     int // planned repeats
	remaining int
	changed   int // ticks whose true price differed from the frozen value
	ep        Episode
}

func epochMs(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return strconv.FormatInt(t.UnixMilli(), 10)
}

// emitEpisode writes one episode row. A nil episode is ignored.
func (inj *Injector) emitEpisode(ep *Episode) {
	if ep == nil || inj.episodeWriter == nil {
		return
	}

	inj.logMutex.Lock()
	defer inj.logMutex.Unlock()

	inj.episodeCount++
	inj.episodeWriter.Write([]string{
		strconv.FormatUint(inj.episodeCount, 10),
		ep.Phase,
		ep.AnomalyType,
		ep.Exchange,
		ep.InstrumentID,
		epochMs(ep.Start),
		epochMs(ep.End),
		epochMs(ep.Observed),
		epochMs(ep.Resume),
		epochMs(ep.LastDelivered),
		ep.Detail,
		ep.SecType,
		seqOrEmpty(ep.Seq),
	})
	if inj.episodeCount%1000 == 0 {
		inj.episodeWriter.Flush()
	}
}

// pointEpisode builds the episode for a single-tick anomaly. origTime is the tick's
// event time before injection; tick.TradingTime is what the downstream will see.
func pointEpisode(phase, anomalyType string, tick *model.RawTick, origTime time.Time, detail string) *Episode {
	return &Episode{
		Phase:        phase,
		AnomalyType:  anomalyType,
		Exchange:     tick.Exchange,
		InstrumentID: tick.ID,
		SecType:      tick.SecType,
		Start:        origTime,
		End:          origTime,
		Observed:     tick.TradingTime,
		Detail:       detail,
		Seq:          tick.Seq,
	}
}

func seqOrEmpty(seq uint64) string {
	if seq == 0 {
		return ""
	}
	return strconv.FormatUint(seq, 10)
}

// finalizeEpisodes writes the episodes that are only complete at end of stream:
// open stale runs, phase 1 (per instrument and day) and phase 3 blackouts.
func (inj *Injector) finalizeEpisodes() {
	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()

	ids := make([]string, 0, len(inj.instrumentState))
	for id, state := range inj.instrumentState {
		if state.Stale != nil {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		state := inj.instrumentState[id]
		state.Stale.ep.Detail = staleDetail(state.Stale)
		inj.emitEpisode(&state.Stale.ep)
		state.Stale = nil
	}

	cfg := inj.config.Phase1
	keys := make([]string, 0, len(inj.phase1Episodes))
	for k := range inj.phase1Episodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		pe := inj.phase1Episodes[k]
		pe.ep.Detail = fmt.Sprintf("pattern=%s;initial_rate=%.3f;final_rate=%.3f;ticks_seen=%d;ticks_dropped=%d",
			cfg.DeclinePattern, cfg.InitialRate, cfg.FinalRate, pe.ticksSeen, pe.ticksDropped)
		inj.emitEpisode(&pe.ep)
	}
	inj.phase1Episodes = make(map[string]*phase1Episode)

	keys = keys[:0]
	for k := range inj.phase3Episodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		inj.emitEpisode(inj.phase3Episodes[k])
	}
	inj.phase3Episodes = make(map[string]*Episode)
}

// warnIfIdle flags enabled phases that injected nothing, which usually means a
// date_filter/window that misses the data or an exchange_filter that matches no
// tick (Exchange is the ID suffix, e.g. "ETR", not the venue name).
func (inj *Injector) warnIfIdle() {
	if inj.stats.TotalProcessed == 0 {
		return
	}
	if inj.config.Phase1.Enabled && inj.stats.Phase1Dropped == 0 {
		log.Printf("[WARN] anomaly: phase1 is enabled but dropped no ticks (check date_filter/window)")
	}
	if inj.config.Phase2.Enabled && inj.stats.Phase2Injected == 0 {
		log.Printf("[WARN] anomaly: phase2 is enabled but injected nothing (check date_filter/window)")
	}
	if inj.config.Phase3.Enabled && inj.stats.Phase3Dropped == 0 {
		log.Printf("[WARN] anomaly: phase3 is enabled but dropped no ticks (check date_filter/window/exchange_filter; Exchange is the ID suffix, e.g. ETR)")
	}
	if inj.config.Phase4.Enabled && inj.stats.Phase4Injected == 0 {
		log.Printf("[WARN] anomaly: phase4 is enabled but injected nothing (check date_filter/window)")
	}
}

// windowBounds returns the absolute start and end of a daily window on the given day.
func windowBounds(day time.Time, w TimeWindow) (time.Time, time.Time) {
	start, _ := ParseTime(w.Start)
	end, _ := ParseTime(w.End)
	y, m, d := day.Date()
	loc := day.Location()
	return time.Date(y, m, d, start.Hour(), start.Minute(), start.Second(), 0, loc),
		time.Date(y, m, d, end.Hour(), end.Minute(), end.Second(), 0, loc)
}

// recordPhase1Tick accumulates the per-instrument, per-day phase 1 counters.
func (inj *Injector) recordPhase1Tick(tick *model.RawTick, marketTime time.Time, dropped bool) {
	key := marketTime.Format("2006-01-02") + "|" + tick.ID

	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()

	pe, ok := inj.phase1Episodes[key]
	if !ok {
		start, end := windowBounds(marketTime, inj.config.Phase1.Window)
		pe = &phase1Episode{ep: Episode{
			Phase:        "phase1",
			AnomalyType:  "tick_rate_decline",
			Exchange:     tick.Exchange,
			InstrumentID: tick.ID,
			SecType:      tick.SecType,
			Start:        start,
			End:          end,
		}}
		inj.phase1Episodes[key] = pe
	}
	pe.ticksSeen++
	if dropped {
		pe.ticksDropped++
	}
}

// affectedInstruments returns the sorted, de-duplicated instruments of phase 1 and 3.
func (inj *Injector) affectedInstruments() (phase1, phase3 []string) {
	inj.stateMutex.RLock()
	defer inj.stateMutex.RUnlock()

	seen := make(map[string]bool)
	for _, pe := range inj.phase1Episodes {
		if !seen[pe.ep.InstrumentID] {
			seen[pe.ep.InstrumentID] = true
			phase1 = append(phase1, pe.ep.InstrumentID)
		}
	}
	for id := range inj.phase3Episodes {
		phase3 = append(phase3, id)
	}
	sort.Strings(phase1)
	sort.Strings(phase3)
	return phase1, phase3
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
	LogFile             string                 `json:"log_file,omitempty"`
	EpisodeFile         string                 `json:"episode_file,omitempty"`
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
	
	phase1Instruments, phase3Instruments := inj.affectedInstruments()

	manifest := Manifest{
		ExperimentID: fmt.Sprintf("exp_%d", time.Now().Unix()),
		Seed:         inj.config.Seed,
		StartTime:    inj.firstTickTime.Format(time.RFC3339),
		EndTime:      inj.lastTickTime.Format(time.RFC3339),
		Stats:        inj.stats,
		LogFile:      inj.config.LogFile,
		EpisodeFile:  inj.episodePath,
		Phases: map[string]PhaseInfo{
			"phase1": {
				Name:                "gradual_decline",
				Enabled:             inj.config.Phase1.Enabled,
				Dates:               inj.config.Phase1.DateFilter,
				Window:              map[string]string{"start": inj.config.Phase1.Window.Start, "end": inj.config.Phase1.Window.End},
				TotalDropped:        inj.stats.Phase1Dropped,
				AffectedInstruments: len(phase1Instruments),
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
					"price_deviation":   inj.stats.PriceDeviations,
				},
			},
			"phase3": {
				Name:                "feed_silence",
				Enabled:             inj.config.Phase3.Enabled,
				Dates:               inj.config.Phase3.DateFilter,
				Window:              map[string]string{"start": inj.config.Phase3.Window.Start, "end": inj.config.Phase3.Window.End},
				TotalDropped:        inj.stats.Phase3Dropped,
				AffectedInstruments: len(phase3Instruments),
			},
			"phase4": {
				Name:          "point_failures",
				Enabled:       inj.config.Phase4.Enabled,
				Dates:         inj.config.Phase4.DateFilter,
				Window:        map[string]string{"start": inj.config.Phase4.Window.Start, "end": inj.config.Phase4.Window.End},
				TotalInjected: inj.stats.Phase4Injected,
				Breakdown: map[string]uint64{
					"null_price":           inj.stats.NullPrices,
					"implausible_price":    inj.stats.ImplausiblePrices,
					"malformed_isin":       inj.stats.MalformedISINs,
					"timestamp_inversion":  inj.stats.TimestampInversions,
				},
			},
		},
		SelectedInstruments: map[string][]string{
			"phase1": phase1Instruments,
			"phase2": []string{"ALL"}, // Phase 2 has no pre-selection
			"phase3": phase3Instruments,
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
	if !inj.isInstrumentSelected("phase1", tick.ID, cfg.InstrumentRatio) {
		return false, nil
	}

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
	inj.recordPhase1Tick(tick, marketTime, !shouldKeep)
	return !shouldKeep, nil
}

// phase2ContextualAnomalies implements contextual last-price anomalies.
// It returns the episode to write now, if any: single-tick anomalies return theirs
// immediately, a stale-price run returns its episode on the tick that ends it.
//
// TODO: tune the per-tick probabilities (currently ~9% of ticks in the window are
// modified, far denser than real feed faults).
func (inj *Injector) phase2ContextualAnomalies(tick *model.RawTick, marketTime time.Time) (bool, *Episode, error) {
	cfg := inj.config.Phase2

	if !isDateInFilter(marketTime, cfg.DateFilter) {
		return false, nil, nil
	}

	inWindow, err := cfg.Window.IsInWindow(marketTime)
	if err != nil || !inWindow {
		return false, nil, err
	}

	// Most rows are quote updates with an empty last price (0). They carry no trade, so
	// there is no price to distort: price anomalies are injected on trade rows only.
	if tick.LastTradedPrice <= 0 {
		return false, nil, nil
	}

	state := inj.getInstrumentState(tick.ID)
	if state == nil || len(state.PriceHistory) < 2 {
		return false, nil, nil // Not enough history
	}

	// A stale-price run in progress takes precedence over new injections.
	if run := state.Stale; run != nil {
		return true, inj.continueStaleRun(tick, state, run), nil
	}

	// Per-instrument quota: instruments with few trades get a higher per-row probability
	// so that every instrument contributes episodes (see quota.go).
	scale := quotaScale(state, "phase2", cfg.Quota, inj.phase2BaseP, marketTime.Format("2006-01-02"))

	// Try each strategy
	for _, strategy := range cfg.Strategies {
		if inj.rng.Float64() >= strategy.Probability*scale {
			continue
		}

		switch strategy.Type {
		case "price_spike":
			if ok, detail := inj.applyPriceSpike(tick, state, strategy); ok {
				inj.stats.PriceSpikes++
				return true, pointEpisode("phase2", "price_spike", tick, marketTime, detail), nil
			}

		case "price_deviation":
			if ok, detail := inj.applyPriceDeviation(tick, state, strategy); ok {
				inj.stats.PriceDeviations++
				return true, pointEpisode("phase2", "price_deviation", tick, marketTime, detail), nil
			}

		case "stale_price":
			if ok, ep := inj.startStaleRun(tick, state, strategy); ok {
				inj.stats.StalePrices++
				return true, ep, nil
			}
		}
	}

	return false, nil, nil
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
	if !inj.isInstrumentSelected("phase3", tick.ID, cfg.InstrumentRatio) {
		return false, nil
	}

	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()

	// Check if instrument is currently in blackout
	if blackoutEnd, exists := inj.phase3Blacklisted[tick.ID]; exists {
		if marketTime.Before(blackoutEnd) {
			return true, nil // Still in blackout
		}
		// Blackout ended, no more blackouts for this instrument
		if ep := inj.phase3Episodes[tick.ID]; ep != nil && ep.Resume.IsZero() {
			ep.Resume = marketTime
		}
		return false, nil
	}

	// Start blackout for this instrument (only once)
	blackoutEnd := marketTime.Add(time.Duration(cfg.BlackoutSeconds) * time.Second)
	inj.phase3Blacklisted[tick.ID] = blackoutEnd

	ep := &Episode{
		Phase:        "phase3",
		AnomalyType:  "feed_silence",
		Exchange:     tick.Exchange,
		InstrumentID: tick.ID,
		SecType:      tick.SecType,
		Start:        marketTime,
		End:          blackoutEnd,
		Detail:       fmt.Sprintf("blackout_s=%d", cfg.BlackoutSeconds),
	}
	// Read the map directly: getInstrumentState would take the lock we already hold.
	if state := inj.instrumentState[tick.ID]; state != nil {
		ep.LastDelivered = state.LastDelivered
		// The silence detector cannot alert before its statistics are warm, so the
		// evaluation needs to know how much history the instrument had.
		ep.Detail += fmt.Sprintf(";delivered_before=%d", state.Delivered)
		if !state.LastDeliveredClock.IsZero() {
			ep.Detail += fmt.Sprintf(";last_delivered_time_ms=%d", state.LastDeliveredClock.UnixMilli())
		}
	}
	inj.phase3Episodes[tick.ID] = ep

	return true, nil
}

// phase4PointFailures implements sudden point failures. It returns the episode of
// the injected tick.
func (inj *Injector) phase4PointFailures(tick *model.RawTick, marketTime time.Time) (bool, *Episode, error) {
	cfg := inj.config.Phase4

	if !isDateInFilter(marketTime, cfg.DateFilter) {
		return false, nil, nil
	}

	inWindow, err := cfg.Window.IsInWindow(marketTime)
	if err != nil || !inWindow {
		return false, nil, err
	}

	// Per-instrument quota, for the price anomaly only (a timestamp rewind is not a
	// price effect and stays per row).
	priceScale := 1.0
	if tick.LastTradedPrice > 0 {
		priceScale = quotaScale(inj.getInstrumentState(tick.ID), "phase4", cfg.Quota, inj.phase4BaseP, marketTime.Format("2006-01-02"))
	}

	// Try each strategy
	for _, strategy := range cfg.Strategies {
		p := strategy.Probability
		if strategy.Type == "implausible_price" {
			p *= priceScale
		}
		if inj.rng.Float64() >= p {
			continue
		}

		switch strategy.Type {
		case "null_price":
			detail := inj.applyNullPrice(tick, strategy)
			inj.stats.NullPrices++
			return true, pointEpisode("phase4", "null_price", tick, marketTime, detail), nil

		case "implausible_price":
			if ok, detail := inj.applyImplausiblePrice(tick, strategy); ok {
				inj.stats.ImplausiblePrices++
				return true, pointEpisode("phase4", "implausible_price", tick, marketTime, detail), nil
			}

		case "malformed_isin":
			if ok, detail := inj.applyMalformedISIN(tick, strategy); ok {
				inj.stats.MalformedISINs++
				return true, pointEpisode("phase4", "malformed_isin", tick, marketTime, detail), nil
			}

		case "timestamp_inversion":
			var prev time.Time
			if state := inj.getInstrumentState(tick.ID); state != nil && len(state.PriceHistory) >= 2 {
				prev = state.PriceHistory[len(state.PriceHistory)-2].Timestamp
			}
			detail := inj.applyTimestampInversion(tick, strategy, prev)
			inj.stats.TimestampInversions++
			return true, pointEpisode("phase4", "timestamp_inversion", tick, marketTime, detail), nil
		}
	}

	return false, nil, nil
}

// ===== ANOMALY APPLICATION HELPERS =====
//
// Each helper returns a "key=value;..." detail string for the episode file. Helpers
// that can decline (no usable history, no effect) also return false, in which case
// the tick is left untouched and nothing is counted or logged.

func (inj *Injector) applyPriceSpike(tick *model.RawTick, state *InstrumentState, strategy ContextualStrategy) (bool, string) {
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
		return false, "" // No valid history
	}

	avg := sum / float64(count)
	minDev := strategy.DeviationRange[0]
	maxDev := strategy.DeviationRange[1]
	deviation := minDev + inj.rng.Float64()*(maxDev-minDev)

	before := tick.LastTradedPrice
	tick.LastTradedPrice = avg * deviation
	tick.AnomalyInjected = true
	tick.AnomalyType = "price_spike"
	return true, fmt.Sprintf("multiplier=%.3f;before=%.6f;after=%.6f", deviation, before, tick.LastTradedPrice)
}

// minReturnsForDeviation is the minimum number of observed returns needed to
// estimate the recent volatility for price_deviation.
const minReturnsForDeviation = 10

// applyPriceDeviation moves the last traded price by k standard deviations of the
// instrument's recent log returns (k drawn from DeviationRange, random sign). Unlike
// price_spike the result stays in a plausible range, so it is only anomalous
// relative to recent context. Bid/Ask are left untouched.
func (inj *Injector) applyPriceDeviation(tick *model.RawTick, state *InstrumentState, strategy ContextualStrategy) (bool, string) {
	if tick.LastTradedPrice <= 0 {
		return false, ""
	}

	// Returns between consecutive *trades*: quote-only rows (price 0) are skipped, so
	// the history's volatility is not that of a price jumping to 0 and back.
	var sum, sumSq float64
	n := 0
	prev := 0.0
	for _, point := range state.PriceHistory {
		p := point.LastTradedPrice
		if p <= 0 {
			continue
		}
		if prev > 0 {
			r := math.Log(p / prev)
			sum += r
			sumSq += r * r
			n++
		}
		prev = p
	}
	if n < minReturnsForDeviation {
		return false, ""
	}

	mean := sum / float64(n)
	variance := sumSq/float64(n) - mean*mean
	if variance < 0 {
		variance = 0
	}
	sigma := math.Sqrt(variance)

	floor := strategy.MinRelativeDeviation
	if floor <= 0 {
		floor = 0.002
	}
	if sigma < floor {
		sigma = floor
	}

	lo, hi := 4.0, 8.0
	if len(strategy.DeviationRange) == 2 {
		lo, hi = strategy.DeviationRange[0], strategy.DeviationRange[1]
	}
	k := lo + inj.rng.Float64()*(hi-lo)
	sign := 1.0
	if inj.rng.Intn(2) == 0 {
		sign = -1.0
	}

	before := tick.LastTradedPrice
	tick.LastTradedPrice = before * math.Exp(sign*k*sigma) // exp keeps the price positive
	tick.AnomalyInjected = true
	tick.AnomalyType = "price_deviation"
	return true, fmt.Sprintf("k_sigma=%.3f;sign=%+.0f;sigma=%.6f;before=%.6f;after=%.6f",
		k, sign, sigma, before, tick.LastTradedPrice)
}

// startStaleRun freezes the last traded price at the previous tick's value for
// RepeatCount consecutive ticks of the instrument (default [3, 10], counting this
// tick). It returns the episode immediately only for a single-tick run.
func (inj *Injector) startStaleRun(tick *model.RawTick, state *InstrumentState, strategy ContextualStrategy) (bool, *Episode) {
	// Freeze at the price of the previous trade (the last entry is the current row).
	prev := 0.0
	for i := len(state.PriceHistory) - 2; i >= 0; i-- {
		if p := state.PriceHistory[i].LastTradedPrice; p > 0 {
			prev = p
			break
		}
	}
	if prev <= 0 {
		return false, nil
	}

	lo, hi := 3, 10
	if len(strategy.RepeatCount) == 2 {
		lo, hi = strategy.RepeatCount[0], strategy.RepeatCount[1]
	}
	n := lo + inj.rng.Intn(hi-lo+1)

	run := &staleRun{
		value:     prev,
		total:     n,
		remaining: n,
		ep: Episode{
			Phase:        "phase2",
			AnomalyType:  "stale_price",
			Exchange:     tick.Exchange,
			InstrumentID: tick.ID,
			SecType:      tick.SecType,
			Start:        tick.TradingTime,
			Seq:          tick.Seq,
		},
	}
	state.Stale = run

	ep := inj.continueStaleRun(tick, state, run)
	return true, ep
}

// continueStaleRun applies the frozen price to one tick of the run. It returns the
// finished episode on the last tick of the run, nil otherwise.
func (inj *Injector) continueStaleRun(tick *model.RawTick, state *InstrumentState, run *staleRun) *Episode {
	if tick.LastTradedPrice != run.value {
		run.changed++
	}
	tick.LastTradedPrice = run.value
	tick.AnomalyInjected = true
	tick.AnomalyType = "stale_price"

	run.remaining--
	run.ep.End = tick.TradingTime
	if run.remaining > 0 {
		return nil
	}

	state.Stale = nil
	run.ep.Detail = staleDetail(run)
	return &run.ep
}

func staleDetail(run *staleRun) string {
	return fmt.Sprintf("value=%.6f;repeat_planned=%d;repeat_actual=%d;changed_ticks=%d",
		run.value, run.total, run.total-run.remaining, run.changed)
}

// applyImplausiblePrice multiplies or divides the last traded price by a factor drawn
// from MultiplierRange (random direction): a price that no real move produces, unlike
// the plausible deviations of phase 2. Trade rows only.
func (inj *Injector) applyImplausiblePrice(tick *model.RawTick, strategy PointFailureStrategy) (bool, string) {
	if tick.LastTradedPrice <= 0 {
		return false, ""
	}

	lo, hi := 10.0, 100.0
	if len(strategy.MultiplierRange) == 2 {
		lo, hi = strategy.MultiplierRange[0], strategy.MultiplierRange[1]
	}
	factor := lo + inj.rng.Float64()*(hi-lo)
	direction := "up"
	before := tick.LastTradedPrice
	if inj.rng.Intn(2) == 0 {
		tick.LastTradedPrice = before / factor
		direction = "down"
	} else {
		tick.LastTradedPrice = before * factor
	}

	tick.AnomalyInjected = true
	tick.AnomalyType = "implausible_price"
	return true, fmt.Sprintf("factor=%.2f;direction=%s;before=%.6f;after=%.6f", factor, direction, before, tick.LastTradedPrice)
}

func (inj *Injector) applyNullPrice(tick *model.RawTick, strategy PointFailureStrategy) string {
	field := strategy.Field[inj.rng.Intn(len(strategy.Field))]

	switch field {
	case "Last":
		tick.LastTradedPrice = 0
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
	return "field=" + field
}

func (inj *Injector) applyMalformedISIN(tick *model.RawTick, strategy PointFailureStrategy) (bool, string) {
	corruption := strategy.Corruption[inj.rng.Intn(len(strategy.Corruption))]
	before := tick.ISIN

	switch corruption {
	case "truncate":
		if len(tick.ISIN) > 4 {
			tick.ISIN = tick.ISIN[:len(tick.ISIN)/2]
		}
	case "random_chars":
		tick.ISIN += "XXX"
	}

	if tick.ISIN == before {
		return false, "" // nothing changed (e.g. empty ISIN), so nothing to detect
	}

	tick.AnomalyInjected = true
	tick.AnomalyType = "malformed_isin"
	return true, fmt.Sprintf("corruption=%s;before=%s;after=%s", corruption, before, tick.ISIN)
}

// applyTimestampInversion rewinds the tick's timestamp. prev is the instrument's
// previous tick time (zero if none): the rewind is only detectable by a per-instrument
// monotonicity check when the new time falls before it, so it is recorded as prev_ms.
func (inj *Injector) applyTimestampInversion(tick *model.RawTick, strategy PointFailureStrategy, prev time.Time) string {
	minRewind := strategy.RewindSeconds[0]
	maxRewind := strategy.RewindSeconds[1]
	rewind := minRewind + inj.rng.Intn(maxRewind-minRewind+1)

	tick.TradingTime = tick.TradingTime.Add(-time.Duration(rewind) * time.Second)
	tick.AnomalyInjected = true
	tick.AnomalyType = "timestamp_inversion"
	if prev.IsZero() {
		return fmt.Sprintf("rewind_s=%d", rewind)
	}
	return fmt.Sprintf("rewind_s=%d;prev_ms=%d", rewind, prev.UnixMilli())
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

