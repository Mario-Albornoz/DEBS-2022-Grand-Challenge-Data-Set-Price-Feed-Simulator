package anomaly

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

func stateWithWindow(prev int, date string) *InstrumentState {
	return &InstrumentState{Windows: map[string]*windowTrades{"phase2": {Date: date, Prev: prev, Cur: 1}}}
}

func TestQuotaScale(t *testing.T) {
	q := QuotaConfig{MinEpisodes: 3, MaxRowProbability: 0.25}
	const base = 0.007
	const day = "2021-11-10"

	cases := []struct {
		name  string
		state *InstrumentState
		q     QuotaConfig
		want  float64 // resulting per-row probability
	}{
		{"few trades: raised to 3 per day", stateWithWindow(20, day), q, 0.15},
		{"very few trades: capped", stateWithWindow(5, day), q, 0.25},
		{"busy instrument: base probability already gives more", stateWithWindow(1000, day), q, base},
		{"no previous day: unchanged", stateWithWindow(0, day), q, base},
		{"quota off", stateWithWindow(20, day), QuotaConfig{}, base},
		{"window counted for another date: unchanged", stateWithWindow(20, "2021-11-09"), q, base},
		{"no window data at all", &InstrumentState{}, q, base},
		{"default cap is 0.25", stateWithWindow(5, day), QuotaConfig{MinEpisodes: 3}, 0.25},
	}
	for _, c := range cases {
		got := quotaScale(c.state, "phase2", c.q, base, day) * base
		if math.Abs(got-c.want) > 1e-9 {
			t.Errorf("%s: per-row probability %.5f, want %.5f", c.name, got, c.want)
		}
	}
}

// End to end: an instrument with about 12 trades a day gets about the target number of
// episodes; without the quota it gets almost none.
func TestQuotaGivesLowTradeInstrumentsEpisodes(t *testing.T) {
	run := func(minEpisodes int) (episodes int, perInstrument map[string]int) {
		cfg, epPath := baseConfig(t)
		cfg.Seed = 7
		cfg.Phase2.Enabled = true
		cfg.Phase2.DateFilter = []string{"10-11-2021"}
		cfg.Phase2.Window = TimeWindow{Start: "09:30:00", End: "15:00:00"}
		cfg.Phase2.Strategies = []ContextualStrategy{{Type: "price_spike", Probability: 0.001, DeviationRange: []float64{3, 3}}}
		cfg.Phase2.Quota = QuotaConfig{MinEpisodes: minEpisodes, MaxRowProbability: 0.5}

		inj, err := NewInjector(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, day := range []int{9, 10} { // day 9 is the warm-up day, day 10 is injected
			for i := 0; i < 40; i++ {
				id := fmt.Sprintf("LOW%d.ETR", i)
				for k := 0; k < 12; k++ { // 12 trades, spaced 20 minutes, each preceded by a quote row
					at := time.Date(2021, 11, day, 9, 40, 0, 0, time.UTC).Add(time.Duration(k) * 20 * time.Minute)
					inj.ProcessTick(&model.RawTick{ID: id, Exchange: "ETR", SecType: "E", TradingTime: at.Add(-time.Second)})
					inj.ProcessTick(&model.RawTick{ID: id, Exchange: "ETR", SecType: "E", LastTradedPrice: 100, TradingTime: at})
				}
			}
		}
		inj.Close()

		perInstrument = map[string]int{}
		for _, ep := range readEpisodes(t, epPath) {
			perInstrument[ep["InstrumentID"]]++
			episodes++
		}
		return episodes, perInstrument
	}

	without, _ := run(0)
	with, per := run(3)

	// 40 instruments x 12 trades x 0.001 = about 0.5 episodes in total without the quota
	if without > 5 {
		t.Errorf("without the quota almost nothing should be injected, got %d", without)
	}
	// with it each instrument aims at 3 (the first trade of the day has no priced history,
	// so slightly fewer): 40 x 3 = 120 in expectation
	if with < 80 || with > 150 {
		t.Errorf("with min_episodes 3 expected about 120 episodes over 40 instruments, got %d", with)
	}
	covered := 0
	for _, n := range per {
		if n > 0 {
			covered++
		}
	}
	if covered < 34 {
		t.Errorf("most instruments should receive episodes, only %d of 40 did", covered)
	}
}

func TestInstrumentDaySummary(t *testing.T) {
	cfg, _ := baseConfig(t)
	cfg.Phase2.Enabled = false
	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	send := func(id, ex, sec string, price float64, at time.Time) {
		inj.ProcessTick(&model.RawTick{ID: id, Exchange: ex, SecType: sec, LastTradedPrice: price, TradingTime: at})
	}
	d9 := time.Date(2021, 11, 9, 10, 0, 0, 0, time.UTC)
	d10 := time.Date(2021, 11, 10, 10, 0, 0, 0, time.UTC)
	send("A.ETR", "ETR", "E", 100, d9)
	send("A.ETR", "ETR", "E", 0, d9.Add(time.Second))
	send("A.ETR", "ETR", "E", 101, d9.Add(2*time.Second))
	send("A.ETR", "ETR", "E", 0, d10)
	send("IDX", "FR", "I", 0, d10) // indices never reach the detector: left out of the summary
	inj.Close()

	instPath := strings.TrimSuffix(cfg.LogFile, ".csv") + "_instruments.csv"
	f, err := os.Open(instPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, _ := csv.NewReader(f).ReadAll()

	want := [][]string{
		{"Date", "Exchange", "InstrumentID", "SecType", "Rows", "Trades"},
		{"2021-11-09", "ETR", "A.ETR", "E", "3", "2"},
		{"2021-11-10", "ETR", "A.ETR", "E", "1", "0"},
	}
	if len(rows) != len(want) {
		t.Fatalf("expected %d rows, got %d: %v", len(want), len(rows), rows)
	}
	for i := range want {
		if strings.Join(rows[i], ",") != strings.Join(want[i], ",") {
			t.Errorf("row %d: got %v, want %v", i, rows[i], want[i])
		}
	}
}

func TestValidateQuota(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.Phase2.Quota = QuotaConfig{MinEpisodes: -1}
	if err := cfg.Validate(); err == nil {
		t.Error("a negative min_episodes should be rejected")
	}
	cfg = DefaultConfig()
	cfg.Enabled = true
	cfg.Phase4.Quota = QuotaConfig{MinEpisodes: 3, MaxRowProbability: 1.5}
	if err := cfg.Validate(); err == nil {
		t.Error("a max_row_probability above 1 should be rejected")
	}
}
