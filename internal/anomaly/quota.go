package anomaly

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

// QuotaConfig gives every instrument a fair share of the price anomalies instead of a
// share proportional to its trades.
//
// A per-row probability puts about 96% of the episodes on the instruments that trade
// most, because trades are very unevenly distributed (the median instrument has about
// 10 a day). To evaluate detection by liquidity, each instrument is aimed at
// MinEpisodes episodes per injection day: its per-row probability is raised to
// MinEpisodes / expected trades in the window, never below the phase's base
// probability and never above MaxRowProbability. "Expected trades" is the number the
// instrument had in the same window on the previous day the injector saw, so the
// mechanism needs one earlier day in the same run (the schedule provides a warm-up
// day) and does nothing without it. It is a target in expectation, not a guarantee:
// the ground truth records what was actually injected.
type QuotaConfig struct {
	MinEpisodes       int     `yaml:"min_episodes"`        // target episodes per instrument and day (0 = off)
	MaxRowProbability float64 `yaml:"max_row_probability"` // cap on the per-row probability (default 0.25)
}

func (q QuotaConfig) validate(phase string) error {
	if q.MinEpisodes < 0 {
		return fmt.Errorf("%s per_instrument_quota: min_episodes must be non-negative, got %d", phase, q.MinEpisodes)
	}
	if q.MaxRowProbability < 0 || q.MaxRowProbability > 1 {
		return fmt.Errorf("%s per_instrument_quota: max_row_probability must be in [0, 1], got %v", phase, q.MaxRowProbability)
	}
	return nil
}

// windowTrades counts an instrument's trades inside a phase window per date.
type windowTrades struct {
	Date string
	Cur  int
	Prev int // trades in the window on the previous date seen
}

// InstrumentDay is one instrument's activity on one day, before any injection. The
// evaluation stratifies by it (trades per day, messages per day).
type InstrumentDay struct {
	Date     string
	Exchange string
	ID       string
	SecType  string
	Rows     int
	Trades   int
}

var instrumentDayHeader = []string{"Date", "Exchange", "InstrumentID", "SecType", "Rows", "Trades"}

// countRow updates the instrument's day and window counters. Called for every row,
// before any injection, with stateMutex held.
func (inj *Injector) countRow(state *InstrumentState, tick *model.RawTick) {
	day := tick.TradingTime.Format("2006-01-02")
	if state.Day != day {
		inj.closeDay(state)
		state.Day = day
		state.Exchange = tick.Exchange
		state.SecType = tick.SecType
	}
	state.DayRows++

	if tick.LastTradedPrice <= 0 {
		return
	}
	state.DayTrades++
	inj.countWindowTrade(state, "phase2", inj.config.Phase2.Window, tick.TradingTime, day)
	inj.countWindowTrade(state, "phase4", inj.config.Phase4.Window, tick.TradingTime, day)
}

func (inj *Injector) countWindowTrade(state *InstrumentState, key string, window TimeWindow, t time.Time, day string) {
	if in, err := window.IsInWindow(t); err != nil || !in {
		return
	}
	if state.Windows == nil {
		state.Windows = make(map[string]*windowTrades)
	}
	w := state.Windows[key]
	if w == nil {
		w = &windowTrades{}
		state.Windows[key] = w
	}
	if w.Date != day {
		w.Prev, w.Cur, w.Date = w.Cur, 0, day
	}
	w.Cur++
}

func (inj *Injector) closeDay(state *InstrumentState) {
	if state.Day == "" {
		return
	}
	inj.instrumentDays = append(inj.instrumentDays, InstrumentDay{
		Date: state.Day, Exchange: state.Exchange, ID: state.ID, SecType: state.SecType,
		Rows: state.DayRows, Trades: state.DayTrades,
	})
	state.DayRows, state.DayTrades = 0, 0
}

// quotaScale returns the factor to apply to the phase's price-anomaly probabilities for
// this instrument on this row (1 = unchanged).
func quotaScale(state *InstrumentState, key string, q QuotaConfig, baseProbability float64, day string) float64 {
	if q.MinEpisodes <= 0 || baseProbability <= 0 || state == nil {
		return 1
	}
	w := state.Windows[key]
	if w == nil || w.Date != day || w.Prev <= 0 {
		return 1
	}

	maxP := q.MaxRowProbability
	if maxP <= 0 {
		maxP = 0.25
	}
	p := math.Max(baseProbability, float64(q.MinEpisodes)/float64(w.Prev))
	p = math.Min(p, maxP)
	if p < baseProbability {
		p = baseProbability
	}
	return p / baseProbability
}

// finalizeInstrumentDays closes every open day and writes the summary file.
func (inj *Injector) finalizeInstrumentDays() {
	if inj.instrumentWriter == nil {
		return
	}

	inj.stateMutex.Lock()
	for _, state := range inj.instrumentState {
		inj.closeDay(state)
	}
	days := append([]InstrumentDay(nil), inj.instrumentDays...)
	inj.instrumentDays = nil
	inj.stateMutex.Unlock()

	sort.Slice(days, func(i, j int) bool {
		if days[i].Date != days[j].Date {
			return days[i].Date < days[j].Date
		}
		return days[i].ID < days[j].ID
	})
	for _, d := range days {
		inj.instrumentWriter.Write([]string{
			d.Date, d.Exchange, d.ID, d.SecType, strconv.Itoa(d.Rows), strconv.Itoa(d.Trades),
		})
	}
	inj.instrumentWriter.Flush()
}
