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

type Injector struct {
	config Config
	rng    *rand.Rand

	instrumentState map[string]*InstrumentState
	stateMutex      sync.RWMutex

	selectedInstruments map[string]bool
	phase3Blacklisted   map[string]time.Time

	phase1Episodes map[string]*phase1Episode
	phase3Episodes map[string]*Episode

	firstTickTime time.Time
	lastTickTime  time.Time

	logFile   *os.File
	logWriter *csv.Writer
	logMutex  sync.Mutex

	instrumentFile   *os.File
	instrumentWriter *csv.Writer
	instrumentDays   []InstrumentDay

	phase2BaseP float64
	phase4BaseP float64

	episodeFile   *os.File
	episodeWriter *csv.Writer
	episodePath   string
	episodeCount  uint64

	stats Stats
}

type InstrumentState struct {
	ID                 string
	PriceHistory       []PricePoint
	LastSeenTime       time.Time
	LastDelivered      time.Time
	Delivered          uint64
	LastDeliveredClock time.Time

	Exchange  string
	SecType   string
	Day       string
	DayRows   int
	DayTrades int
	Windows   map[string]*windowTrades
	Stale     *staleRun
}

type PricePoint struct {
	Timestamp       time.Time
	LastTradedPrice float64
	Bid             float64
	Ask             float64
}

type Stats struct {
	TotalProcessed uint64
	Phase1Dropped  uint64
	Phase2Injected uint64
	Phase3Dropped  uint64
	Phase4Injected uint64

	PriceSpikes         uint64
	StalePrices         uint64
	PriceDeviations     uint64
	NullPrices          uint64
	ImplausiblePrices   uint64
	MalformedISINs      uint64
	TimestampInversions uint64
}

func NewInjector(config Config) (*Injector, error) {
	if !config.Enabled {
		return nil, nil
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

	if config.LogFile != "" {
		file, err := os.Create(config.LogFile)
		if err != nil {
			return nil, fmt.Errorf("failed to create anomaly log file: %w", err)
		}
		inj.logFile = file
		inj.logWriter = csv.NewWriter(file)

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

func (inj *Injector) ProcessTick(tick *model.RawTick) (*model.RawTick, bool, error) {
	if inj == nil {
		return tick, false, nil
	}

	inj.stats.TotalProcessed++

	if tick.SecType == "I" {
		return tick, false, nil
	}

	if inj.firstTickTime.IsZero() {
		inj.firstTickTime = tick.TradingTime
	}
	inj.lastTickTime = tick.TradingTime

	inj.updateInstrumentState(tick)

	marketTime := tick.TradingTime
	originalTick := *tick
	dropped := false

	if inj.config.Phase3.Enabled {
		if drop, err := inj.phase3FeedSilence(tick, marketTime); err != nil {
			return nil, false, err
		} else if drop {
			dropped = true
			inj.stats.Phase3Dropped++
			inj.logAnomaly(&originalTick, tick, "feed_silence", "phase3", dropped)
			return tick, true, nil
		}
	}

	if inj.config.Phase1.Enabled {
		if drop, err := inj.phase1TickRateDecline(tick, marketTime); err != nil {
			return nil, false, err
		} else if drop {
			dropped = true
			inj.stats.Phase1Dropped++
			inj.logAnomaly(&originalTick, tick, "tick_rate_decline", "phase1", dropped)
			return tick, true, nil
		}
	}

	if inj.config.Phase2.Enabled {
		if injected, ep, err := inj.phase2ContextualAnomalies(tick, marketTime); err != nil {
			return nil, false, err
		} else if injected {
			inj.stats.Phase2Injected++
			inj.logAnomaly(&originalTick, tick, tick.AnomalyType, "phase2", false)
			inj.emitEpisode(ep)
		}
	}

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

	state.PriceHistory = append(state.PriceHistory, PricePoint{
		Timestamp:       tick.TradingTime,
		LastTradedPrice: tick.LastTradedPrice,
		Bid:             tick.Bid,
		Ask:             tick.Ask,
	})
	state.LastSeenTime = tick.TradingTime
	inj.countRow(state, tick)

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

func (inj *Injector) getInstrumentState(instrumentID string) *InstrumentState {
	inj.stateMutex.RLock()
	defer inj.stateMutex.RUnlock()
	return inj.instrumentState[instrumentID]
}

func (inj *Injector) isInstrumentSelected(phase, instrumentID string, ratio float64) bool {
	key := phase + "|" + instrumentID
	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()

	if selected, exists := inj.selectedInstruments[key]; exists {
		return selected
	}

	selected := inj.rng.Float64() < ratio
	inj.selectedInstruments[key] = selected
	return selected
}

func (inj *Injector) markDelivered(instrumentID string, tradingTime, clockTime time.Time) {
	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()

	if state := inj.instrumentState[instrumentID]; state != nil {
		state.LastDelivered = tradingTime
		state.LastDeliveredClock = clockTime
		state.Delivered++
	}
}

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

type Episode struct {
	Phase         string
	AnomalyType   string
	Exchange      string
	InstrumentID  string
	SecType       string
	Start         time.Time
	End           time.Time
	Observed      time.Time
	Resume        time.Time
	LastDelivered time.Time
	Detail        string
	Seq           uint64
}

var episodeHeader = []string{
	"EpisodeID", "Phase", "AnomalyType", "Exchange", "InstrumentID",
	"StartMs", "EndMs", "ObservedMs", "ResumeMs", "LastDeliveredMs", "Detail", "SecType", "Seq",
}

type phase1Episode struct {
	ep           Episode
	ticksSeen    uint64
	ticksDropped uint64
}

type staleRun struct {
	value     float64
	total     int
	remaining int
	changed   int
	ep        Episode
}

func epochMs(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return strconv.FormatInt(t.UnixMilli(), 10)
}

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

func windowBounds(day time.Time, w TimeWindow) (time.Time, time.Time) {
	start, _ := ParseTime(w.Start)
	end, _ := ParseTime(w.End)
	y, m, d := day.Date()
	loc := day.Location()
	return time.Date(y, m, d, start.Hour(), start.Minute(), start.Second(), 0, loc),
		time.Date(y, m, d, end.Hour(), end.Minute(), end.Second(), 0, loc)
}

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

func (inj *Injector) GetStats() Stats {
	if inj == nil {
		return Stats{}
	}
	return inj.stats
}

type Manifest struct {
	ExperimentID        string               `json:"experiment_id"`
	Seed                int64                `json:"seed"`
	StartTime           string               `json:"start_time"`
	EndTime             string               `json:"end_time"`
	Phases              map[string]PhaseInfo `json:"phases"`
	SelectedInstruments map[string][]string  `json:"selected_instruments"`
	LogFile             string               `json:"log_file,omitempty"`
	EpisodeFile         string               `json:"episode_file,omitempty"`
	Stats               Stats                `json:"stats"`
}

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
					"price_spike":     inj.stats.PriceSpikes,
					"stale_price":     inj.stats.StalePrices,
					"price_deviation": inj.stats.PriceDeviations,
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
					"null_price":          inj.stats.NullPrices,
					"implausible_price":   inj.stats.ImplausiblePrices,
					"malformed_isin":      inj.stats.MalformedISINs,
					"timestamp_inversion": inj.stats.TimestampInversions,
				},
			},
		},
		SelectedInstruments: map[string][]string{
			"phase1": phase1Instruments,
			"phase2": []string{"ALL"},
			"phase3": phase3Instruments,
			"phase4": []string{"ALL"},
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

func (inj *Injector) phase1TickRateDecline(tick *model.RawTick, marketTime time.Time) (bool, error) {
	cfg := inj.config.Phase1

	if !isDateInFilter(marketTime, cfg.DateFilter) {
		return false, nil
	}

	inWindow, err := cfg.Window.IsInWindow(marketTime)
	if err != nil || !inWindow {
		return false, err
	}

	if !inj.isInstrumentSelected("phase1", tick.ID, cfg.InstrumentRatio) {
		return false, nil
	}

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

	var currentRate float64
	if cfg.DeclinePattern == "exponential" {
		currentRate = cfg.InitialRate * math.Pow(cfg.FinalRate/cfg.InitialRate, progress)
	} else {
		currentRate = cfg.InitialRate + (cfg.FinalRate-cfg.InitialRate)*progress
	}

	shouldKeep := inj.rng.Float64() < currentRate
	inj.recordPhase1Tick(tick, marketTime, !shouldKeep)
	return !shouldKeep, nil
}

func (inj *Injector) phase2ContextualAnomalies(tick *model.RawTick, marketTime time.Time) (bool, *Episode, error) {
	cfg := inj.config.Phase2

	if !isDateInFilter(marketTime, cfg.DateFilter) {
		return false, nil, nil
	}

	inWindow, err := cfg.Window.IsInWindow(marketTime)
	if err != nil || !inWindow {
		return false, nil, err
	}

	if tick.LastTradedPrice <= 0 {
		return false, nil, nil
	}

	state := inj.getInstrumentState(tick.ID)
	if state == nil || len(state.PriceHistory) < 2 {
		return false, nil, nil
	}

	if run := state.Stale; run != nil {
		return true, inj.continueStaleRun(tick, state, run), nil
	}

	scale := quotaScale(state, "phase2", cfg.Quota, inj.phase2BaseP, marketTime.Format("2006-01-02"))

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

func (inj *Injector) phase3FeedSilence(tick *model.RawTick, marketTime time.Time) (bool, error) {
	cfg := inj.config.Phase3

	if !isDateInFilter(marketTime, cfg.DateFilter) {
		return false, nil
	}

	inWindow, err := cfg.Window.IsInWindow(marketTime)
	if err != nil || !inWindow {
		return false, err
	}

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

	if !inj.isInstrumentSelected("phase3", tick.ID, cfg.InstrumentRatio) {
		return false, nil
	}

	inj.stateMutex.Lock()
	defer inj.stateMutex.Unlock()

	if blackoutEnd, exists := inj.phase3Blacklisted[tick.ID]; exists {
		if marketTime.Before(blackoutEnd) {
			return true, nil
		}
		if ep := inj.phase3Episodes[tick.ID]; ep != nil && ep.Resume.IsZero() {
			ep.Resume = marketTime
		}
		return false, nil
	}

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
	if state := inj.instrumentState[tick.ID]; state != nil {
		ep.LastDelivered = state.LastDelivered
		ep.Detail += fmt.Sprintf(";delivered_before=%d", state.Delivered)
		if !state.LastDeliveredClock.IsZero() {
			ep.Detail += fmt.Sprintf(";last_delivered_time_ms=%d", state.LastDeliveredClock.UnixMilli())
		}
	}
	inj.phase3Episodes[tick.ID] = ep

	return true, nil
}

func (inj *Injector) phase4PointFailures(tick *model.RawTick, marketTime time.Time) (bool, *Episode, error) {
	cfg := inj.config.Phase4

	if !isDateInFilter(marketTime, cfg.DateFilter) {
		return false, nil, nil
	}

	inWindow, err := cfg.Window.IsInWindow(marketTime)
	if err != nil || !inWindow {
		return false, nil, err
	}

	priceScale := 1.0
	if tick.LastTradedPrice > 0 {
		priceScale = quotaScale(inj.getInstrumentState(tick.ID), "phase4", cfg.Quota, inj.phase4BaseP, marketTime.Format("2006-01-02"))
	}

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

func (inj *Injector) applyPriceSpike(tick *model.RawTick, state *InstrumentState, strategy ContextualStrategy) (bool, string) {
	var sum float64
	count := 0
	for _, point := range state.PriceHistory {
		if point.LastTradedPrice > 0 {
			sum += point.LastTradedPrice
			count++
		}
	}

	if count == 0 {
		return false, ""
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

func (inj *Injector) applyPriceDeviation(tick *model.RawTick, state *InstrumentState, strategy ContextualStrategy) (bool, string) {
	if tick.LastTradedPrice <= 0 {
		return false, ""
	}

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
	tick.LastTradedPrice = before * math.Exp(sign*k*sigma)
	tick.AnomalyInjected = true
	tick.AnomalyType = "price_deviation"
	return true, fmt.Sprintf("k_sigma=%.3f;sign=%+.0f;sigma=%.6f;before=%.6f;after=%.6f",
		k, sign, sigma, before, tick.LastTradedPrice)
}

func (inj *Injector) startStaleRun(tick *model.RawTick, state *InstrumentState, strategy ContextualStrategy) (bool, *Episode) {
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
		return false, ""
	}

	tick.AnomalyInjected = true
	tick.AnomalyType = "malformed_isin"
	return true, fmt.Sprintf("corruption=%s;before=%s;after=%s", corruption, before, tick.ISIN)
}

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
