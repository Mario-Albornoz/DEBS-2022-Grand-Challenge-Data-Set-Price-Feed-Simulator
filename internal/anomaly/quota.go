package anomaly

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

type QuotaConfig struct {
	MinEpisodes       int     `yaml:"min_episodes"`
	MaxRowProbability float64 `yaml:"max_row_probability"`
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

type windowTrades struct {
	Date string
	Cur  int
	Prev int
}

type InstrumentDay struct {
	Date     string
	Exchange string
	ID       string
	SecType  string
	Rows     int
	Trades   int
}

var instrumentDayHeader = []string{"Date", "Exchange", "InstrumentID", "SecType", "Rows", "Trades"}

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
