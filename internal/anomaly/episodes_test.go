package anomaly

import (
	"encoding/csv"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

// baseConfig returns a config with every phase disabled and file logging pointed
// at a temp dir. The caller sets the phase's fields.
func baseConfig(t *testing.T) (Config, string) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.Phase1.Enabled = false
	cfg.Phase2.Enabled = false
	cfg.Phase3.Enabled = false
	cfg.Phase4.Enabled = false
	dir := t.TempDir()
	cfg.LogFile = filepath.Join(dir, "log.csv")
	cfg.EpisodeFile = filepath.Join(dir, "episodes.csv")
	return cfg, cfg.EpisodeFile
}

// readEpisodes loads the episode file as header-keyed rows.
func readEpisodes(t *testing.T, path string) []map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open episodes: %v", err)
	}
	defer f.Close()

	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("read episodes: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("episode file has no header")
	}
	out := make([]map[string]string, 0, len(rows)-1)
	for _, row := range rows[1:] {
		m := make(map[string]string, len(row))
		for i, h := range rows[0] {
			m[h] = row[i]
		}
		out = append(out, m)
	}
	return out
}

func ms(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }

func TestPhase2PriceDeviationOnlyTouchesLastPrice(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase2.Enabled = true
	cfg.Phase2.DateFilter = []string{"09-11-2021"}
	cfg.Phase2.Strategies = []ContextualStrategy{
		{Type: "price_deviation", Probability: 1.0, DeviationRange: []float64{5.0, 5.0}},
	}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2021, 11, 9, 10, 0, 0, 0, time.UTC)
	var last *model.RawTick
	for i := 0; i < 40; i++ {
		price := 100.0
		if i%2 == 1 {
			price = 100.5 // gives the return history a non-zero volatility
		}
		tick := &model.RawTick{
			ID: "TEST.ETR", Exchange: "ETR",
			Bid: 99.0, Ask: 101.0, LastTradedPrice: price,
			TradingTime: base.Add(time.Duration(i) * time.Second),
		}
		modified, dropped, err := inj.ProcessTick(tick)
		if err != nil || dropped {
			t.Fatalf("tick %d: err=%v dropped=%v", i, err, dropped)
		}
		last = modified
	}
	inj.Close()

	if last.AnomalyType != "price_deviation" {
		t.Fatalf("expected price_deviation, got %q", last.AnomalyType)
	}
	if last.Bid != 99.0 || last.Ask != 101.0 {
		t.Errorf("Bid/Ask must be untouched, got %.2f/%.2f", last.Bid, last.Ask)
	}
	// 5 sigma with sigma floored at 0.2% is at least a 1% move, in either direction.
	if move := math.Abs(math.Log(last.LastTradedPrice / 100.5)); move < 5*0.002*0.99 {
		t.Errorf("last price barely moved: %.6f", last.LastTradedPrice)
	}
	if last.LastTradedPrice <= 0 {
		t.Errorf("price must stay positive, got %.6f", last.LastTradedPrice)
	}

	episodes := readEpisodes(t, epPath)
	if len(episodes) == 0 {
		t.Fatal("expected episodes for injected ticks")
	}
	for _, ep := range episodes {
		if ep["Phase"] != "phase2" || ep["AnomalyType"] != "price_deviation" {
			t.Errorf("unexpected episode %v", ep)
		}
		if ep["StartMs"] != ep["EndMs"] || ep["StartMs"] != ep["ObservedMs"] {
			t.Errorf("single-tick episode should have Start==End==Observed: %v", ep)
		}
	}
}

func TestPhase2StaleRunIsOneEpisode(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase2.Enabled = true
	cfg.Phase2.DateFilter = []string{"09-11-2021"}
	cfg.Phase2.Strategies = []ContextualStrategy{
		{Type: "stale_price", Probability: 1.0, RepeatCount: []int{3, 3}},
	}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2021, 11, 9, 10, 0, 0, 0, time.UTC)
	var out []*model.RawTick
	for i := 0; i < 7; i++ {
		tick := &model.RawTick{
			ID: "TEST.ETR", Exchange: "ETR",
			LastTradedPrice: 100.0 + float64(i),
			TradingTime:     base.Add(time.Duration(i) * time.Second),
		}
		modified, _, err := inj.ProcessTick(tick)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, modified)
	}
	inj.Close()

	if out[0].AnomalyInjected {
		t.Error("tick 0 has no history and must be untouched")
	}
	// Injection starts once two ticks of history exist: runs are ticks 1-3 and 4-6.
	// Each run freezes the price of the tick before it (the true price, not an
	// injected one).
	for i := 1; i <= 3; i++ {
		if out[i].LastTradedPrice != 100.0 {
			t.Errorf("tick %d: expected frozen price 100, got %.2f", i, out[i].LastTradedPrice)
		}
	}
	for i := 4; i <= 6; i++ {
		if out[i].LastTradedPrice != 103.0 {
			t.Errorf("tick %d: expected frozen price 103, got %.2f", i, out[i].LastTradedPrice)
		}
	}

	episodes := readEpisodes(t, epPath)
	if len(episodes) != 2 {
		t.Fatalf("expected 2 stale runs, got %d: %v", len(episodes), episodes)
	}
	first := episodes[0]
	if first["StartMs"] != ms(base.Add(1*time.Second)) || first["EndMs"] != ms(base.Add(3*time.Second)) {
		t.Errorf("run bounds wrong: %v", first)
	}
	if !strings.Contains(first["Detail"], "repeat_actual=3") || !strings.Contains(first["Detail"], "changed_ticks=3") {
		t.Errorf("unexpected detail %q", first["Detail"])
	}
}

func TestPhaseSelectionIsIndependentPerPhase(t *testing.T) {
	cfg, _ := baseConfig(t)
	cfg.Phase1.Enabled = true
	cfg.Phase1.DateFilter = []string{"08-11-2021"}
	cfg.Phase1.Window = TimeWindow{Start: "09:00:00", End: "10:00:00"}
	cfg.Phase1.InstrumentRatio = 0.0 // nobody selected for phase 1
	cfg.Phase3.Enabled = true
	cfg.Phase3.DateFilter = []string{"09-11-2021"}
	cfg.Phase3.Window = TimeWindow{Start: "09:00:00", End: "10:00:00"}
	cfg.Phase3.InstrumentRatio = 1.0
	cfg.Phase3.ExchangeFilter = nil

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer inj.Close()

	// Phase 1 evaluates (and rejects) the instrument first...
	_, dropped, _ := inj.ProcessTick(&model.RawTick{
		ID: "TEST.ETR", Exchange: "ETR",
		TradingTime: time.Date(2021, 11, 8, 9, 30, 0, 0, time.UTC),
	})
	if dropped {
		t.Fatal("phase 1 with ratio 0 must not drop")
	}

	// ...which must not decide phase 3's selection.
	_, dropped, _ = inj.ProcessTick(&model.RawTick{
		ID: "TEST.ETR", Exchange: "ETR",
		TradingTime: time.Date(2021, 11, 9, 9, 30, 0, 0, time.UTC),
	})
	if !dropped {
		t.Error("phase 3 with ratio 1.0 must select the instrument regardless of phase 1")
	}
}

func TestPhase3EpisodeGroundTruth(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase3.Enabled = true
	cfg.Phase3.DateFilter = []string{"08-11-2021"}
	cfg.Phase3.Window = TimeWindow{Start: "09:30:00", End: "10:00:00"}
	cfg.Phase3.BlackoutSeconds = 10
	cfg.Phase3.InstrumentRatio = 1.0
	cfg.Phase3.ExchangeFilter = []string{"ETR"}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}

	day := time.Date(2021, 11, 8, 9, 29, 58, 0, time.UTC)
	at := func(sec int) time.Time { return day.Add(time.Duration(sec) * time.Second) }
	send := func(id, ex string, tm time.Time) bool {
		// the update time is the whole second of the (millisecond) trading time
		_, dropped, err := inj.ProcessTick(&model.RawTick{ID: id, Exchange: ex, TradingTime: tm.Add(300 * time.Millisecond), Time: tm})
		if err != nil {
			t.Fatal(err)
		}
		return dropped
	}

	if send("A.ETR", "ETR", at(0)) { // 09:29:58, before the window
		t.Fatal("tick before window must be delivered")
	}
	if !send("A.ETR", "ETR", at(3)) { // 09:30:01, starts the blackout
		t.Fatal("first tick in window should start the blackout")
	}
	if !send("A.ETR", "ETR", at(7)) { // 09:30:05
		t.Fatal("tick inside the blackout should be dropped")
	}
	if send("A.ETR", "ETR", at(14)) { // 09:30:12, after 09:30:11
		t.Fatal("tick after the blackout should be delivered")
	}
	if send("B.FR", "FR", at(3)) { // exchange filter excludes it
		t.Fatal("instrument on another exchange must not be dropped")
	}
	inj.Close()

	episodes := readEpisodes(t, epPath)
	if len(episodes) != 1 {
		t.Fatalf("expected 1 blackout episode, got %d: %v", len(episodes), episodes)
	}
	ep := episodes[0]
	want := map[string]string{
		"Phase":        "phase3",
		"InstrumentID": "A.ETR",
		"Exchange":     "ETR",
		// TradingTime columns carry the 300 ms the test gives every tick
		"StartMs":         ms(at(3).Add(300 * time.Millisecond)),
		"EndMs":           ms(at(13).Add(300 * time.Millisecond)),
		"LastDeliveredMs": ms(at(0).Add(300 * time.Millisecond)),
		"ResumeMs":        ms(at(14).Add(300 * time.Millisecond)),
	}
	for k, v := range want {
		if ep[k] != v {
			t.Errorf("%s: got %q, want %q", k, ep[k], v)
		}
	}
	// silence runs on the whole-second update time, so the ground truth carries the last
	// delivered message on that clock too
	if !strings.Contains(ep["Detail"], "last_delivered_time_ms="+ms(at(0))) {
		t.Errorf("Detail should carry the last delivered update time: %q", ep["Detail"])
	}
	if !strings.Contains(ep["Detail"], "blackout_s=10") || !strings.Contains(ep["Detail"], "delivered_before=1") {
		t.Errorf("Detail should carry the blackout length and prior delivered ticks: %q", ep["Detail"])
	}
}

func TestPhase1EpisodeCountsPerInstrumentDay(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase1.Enabled = true
	cfg.Phase1.DateFilter = []string{"08-11-2021"}
	cfg.Phase1.Window = TimeWindow{Start: "09:00:00", End: "10:00:00"}
	cfg.Phase1.InitialRate = 1.0
	cfg.Phase1.FinalRate = 1.0 // keep everything: only the counters matter here
	cfg.Phase1.InstrumentRatio = 1.0

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2021, 11, 8, 9, 10, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		inj.ProcessTick(&model.RawTick{ID: "A.ETR", Exchange: "ETR", TradingTime: base.Add(time.Duration(i) * time.Minute)})
	}
	inj.ProcessTick(&model.RawTick{ID: "B.ETR", Exchange: "ETR", TradingTime: base})
	inj.Close()

	episodes := readEpisodes(t, epPath)
	if len(episodes) != 2 {
		t.Fatalf("expected one episode per instrument, got %d", len(episodes))
	}
	a := episodes[0]
	day := time.Date(2021, 11, 8, 0, 0, 0, 0, time.UTC)
	if a["InstrumentID"] != "A.ETR" ||
		a["StartMs"] != ms(day.Add(9*time.Hour)) || a["EndMs"] != ms(day.Add(10*time.Hour)) {
		t.Errorf("phase 1 window bounds wrong: %v", a)
	}
	if !strings.Contains(a["Detail"], "ticks_seen=5") || !strings.Contains(a["Detail"], "ticks_dropped=0") {
		t.Errorf("unexpected detail %q", a["Detail"])
	}
}

func TestPhase4NullLastPriceAndTimestampInversion(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase4.Enabled = true
	cfg.Phase4.DateFilter = []string{"10-11-2021"}
	cfg.Phase4.Strategies = []PointFailureStrategy{
		{Type: "null_price", Probability: 1.0, Field: []string{"Last"}},
	}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tm := time.Date(2021, 11, 10, 10, 30, 0, 0, time.UTC)
	modified, _, _ := inj.ProcessTick(&model.RawTick{
		ID: "A.ETR", Exchange: "ETR", Bid: 100, Ask: 101, LastTradedPrice: 100.5, TradingTime: tm,
	})
	if modified.LastTradedPrice != 0 || modified.Bid != 100 || modified.Ask != 101 {
		t.Errorf("only Last should be nulled: %+v", modified)
	}
	inj.Close()

	cfg2, epPath2 := baseConfig(t)
	cfg2.Phase4.Enabled = true
	cfg2.Phase4.DateFilter = []string{"10-11-2021"}
	cfg2.Phase4.Strategies = []PointFailureStrategy{
		{Type: "timestamp_inversion", Probability: 1.0, RewindSeconds: []int{60, 60}},
	}
	inj2, err := NewInjector(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	inj2.ProcessTick(&model.RawTick{ID: "A.ETR", Exchange: "ETR", SecType: "E", LastTradedPrice: 100, TradingTime: tm})
	inj2.Close()

	ep := readEpisodes(t, epPath2)[0]
	if ep["StartMs"] != ms(tm) || ep["ObservedMs"] != ms(tm.Add(-time.Minute)) {
		t.Errorf("start should be the original time and observed the rewound one: %v", ep)
	}
	if ep["SecType"] != "E" {
		t.Errorf("SecType should be carried into the episode, got %q", ep["SecType"])
	}

	if got := readEpisodes(t, epPath)[0]["Detail"]; got != "field=Last" {
		t.Errorf("unexpected detail %q", got)
	}
}

func TestMalformedISINNoOpIsNotInjected(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase4.Enabled = true
	cfg.Phase4.DateFilter = []string{"10-11-2021"}
	cfg.Phase4.Strategies = []PointFailureStrategy{
		{Type: "malformed_isin", Probability: 1.0, Corruption: []string{"truncate"}},
	}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tm := time.Date(2021, 11, 10, 10, 30, 0, 0, time.UTC)

	// Too short to truncate: nothing changes, so it must not be labelled an anomaly.
	modified, _, _ := inj.ProcessTick(&model.RawTick{ID: "A.ETR", Exchange: "ETR", ISIN: "AB", TradingTime: tm})
	if modified.AnomalyInjected {
		t.Error("unchanged ISIN must not be marked as injected")
	}

	modified, _, _ = inj.ProcessTick(&model.RawTick{ID: "A.ETR", Exchange: "ETR", ISIN: "DE0007164600", TradingTime: tm})
	if !modified.AnomalyInjected || modified.ISIN != "DE0007" {
		t.Errorf("expected truncated ISIN, got %+v", modified)
	}
	inj.Close()

	if n := len(readEpisodes(t, epPath)); n != 1 {
		t.Errorf("expected exactly 1 episode, got %d", n)
	}
}

func TestValidateRejectsUnknownOrMalformedStrategies(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}

	removed := DefaultConfig()
	removed.Enabled = true
	removed.Phase2.Strategies = append(removed.Phase2.Strategies, ContextualStrategy{Type: "bid_ask_inversion", Probability: 0.01})
	if err := removed.Validate(); err == nil {
		t.Error("removed strategy type bid_ask_inversion should be rejected")
	}

	noRange := DefaultConfig()
	noRange.Enabled = true
	noRange.Phase2.Strategies = []ContextualStrategy{{Type: "price_spike", Probability: 0.01}}
	if err := noRange.Validate(); err == nil {
		t.Error("price_spike without deviation_range should be rejected")
	}

	badField := DefaultConfig()
	badField.Enabled = true
	badField.Phase4.Strategies = []PointFailureStrategy{{Type: "null_price", Probability: 0.01, Field: []string{"Volume"}}}
	if err := badField.Validate(); err == nil {
		t.Error("unknown null_price field should be rejected")
	}
}

func TestTimestampInversionRecordsPreviousTick(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase4.Enabled = true
	cfg.Phase4.DateFilter = []string{"10-11-2021"}
	cfg.Phase4.Strategies = []PointFailureStrategy{
		{Type: "timestamp_inversion", Probability: 1.0, RewindSeconds: []int{30, 30}},
	}
	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}

	first := time.Date(2021, 11, 10, 10, 30, 0, 0, time.UTC)
	second := first.Add(5 * time.Second)
	inj.ProcessTick(&model.RawTick{ID: "A.ETR", Exchange: "ETR", SecType: "E", TradingTime: first})
	inj.ProcessTick(&model.RawTick{ID: "A.ETR", Exchange: "ETR", SecType: "E", TradingTime: second})
	inj.Close()

	eps := readEpisodes(t, epPath)
	if len(eps) != 2 {
		t.Fatalf("expected 2 episodes, got %d", len(eps))
	}
	if strings.Contains(eps[0]["Detail"], "prev_ms") {
		t.Errorf("first tick has no previous tick: %q", eps[0]["Detail"])
	}
	// second tick rewound to 10:29:35, before the previous tick at 10:30:00: detectable
	if want := "prev_ms=" + ms(first); !strings.Contains(eps[1]["Detail"], want) {
		t.Errorf("Detail %q should contain %q", eps[1]["Detail"], want)
	}
}

// ---- price anomalies only apply to trade rows (rows with a last price) ----

func TestPriceAnomaliesSkipQuoteRows(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase2.Enabled = true
	cfg.Phase2.DateFilter = []string{"09-11-2021"}
	cfg.Phase2.Strategies = []ContextualStrategy{{Type: "price_spike", Probability: 1.0, DeviationRange: []float64{3, 3}}}
	cfg.Phase4.Enabled = true
	cfg.Phase4.DateFilter = []string{"09-11-2021"}
	cfg.Phase4.Strategies = []PointFailureStrategy{{Type: "implausible_price", Probability: 1.0, MultiplierRange: []float64{10, 10}}}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2021, 11, 9, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 6; i++ {
		price := 0.0 // quote update: empty last price
		if i%2 == 0 {
			price = 100
		}
		tick := &model.RawTick{ID: "A.ETR", Exchange: "ETR", SecType: "E", LastTradedPrice: price,
			TradingTime: base.Add(time.Duration(i) * time.Second)}
		out, _, _ := inj.ProcessTick(tick)
		if price == 0 && (out.AnomalyInjected || out.LastTradedPrice != 0) {
			t.Errorf("tick %d is a quote update and must be left alone: %+v", i, out)
		}
	}
	inj.Close()

	for _, ep := range readEpisodes(t, epPath) {
		// every episode starts at an even second, i.e. on a trade row
		var start int64
		start, _ = strconv.ParseInt(ep["StartMs"], 10, 64)
		if (start-base.UnixMilli())/1000%2 != 0 {
			t.Errorf("episode on a quote row: %v", ep)
		}
	}
}

func TestPriceDeviationVolatilityUsesTradesOnly(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase2.Enabled = true
	cfg.Phase2.DateFilter = []string{"09-11-2021"}
	cfg.Phase2.Strategies = []ContextualStrategy{
		{Type: "price_deviation", Probability: 1.0, DeviationRange: []float64{5, 5}, MinRelativeDeviation: 1e-9},
	}
	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// trades alternate 100 / 101 with a quote update (price 0) between every two trades;
	// if the zeros counted, the "returns" would be huge (ln(100/0) is undefined)
	base := time.Date(2021, 11, 9, 10, 0, 0, 0, time.UTC)
	var last *model.RawTick
	for i := 0; i < 60; i++ {
		price := 0.0
		if i%2 == 0 {
			price = 100 + float64((i/2)%2)
		}
		tick := &model.RawTick{ID: "A.ETR", Exchange: "ETR", SecType: "E", LastTradedPrice: price,
			TradingTime: base.Add(time.Duration(i) * time.Second)}
		out, _, _ := inj.ProcessTick(tick)
		if price > 0 {
			last = out
		}
	}
	inj.Close()

	if last == nil || last.AnomalyType != "price_deviation" {
		t.Fatalf("expected an injected trade, got %+v", last)
	}
	// each trade-to-trade return is ln(101/100) = 0.00995, so 5 sigma is a ~5% move
	move := math.Abs(math.Log(last.LastTradedPrice / (100 + 1)))
	if move < 0.03 || move > 0.08 {
		t.Errorf("a 5-sigma move on trade-to-trade volatility should be ~5%%, got %.4f (price %.4f)", move, last.LastTradedPrice)
	}
	if len(readEpisodes(t, epPath)) == 0 {
		t.Error("expected episodes")
	}
}

func TestStaleRunFreezesPreviousTradeAndCountsTradeRowsOnly(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase2.Enabled = true
	cfg.Phase2.DateFilter = []string{"09-11-2021"}
	cfg.Phase2.Strategies = []ContextualStrategy{{Type: "stale_price", Probability: 1.0, RepeatCount: []int{3, 3}}}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2021, 11, 9, 10, 0, 0, 0, time.UTC)
	// trade, quote, trade, quote, trade, quote, trade, quote, trade
	prices := []float64{100, 0, 101, 0, 102, 0, 103, 0, 104}
	var out []*model.RawTick
	for i, p := range prices {
		tick := &model.RawTick{ID: "A.ETR", Exchange: "ETR", SecType: "E", LastTradedPrice: p,
			TradingTime: base.Add(time.Duration(i) * time.Second)}
		o, _, _ := inj.ProcessTick(tick)
		out = append(out, o)
	}
	inj.Close()

	// The first trade has no previous trade to freeze at, so the run starts at the second
	// trade (index 2) and covers three trade rows: indices 2, 4, 6, all frozen at 100.
	for _, i := range []int{2, 4, 6} {
		if out[i].LastTradedPrice != 100 {
			t.Errorf("row %d: expected the previous trade price 100, got %v", i, out[i].LastTradedPrice)
		}
	}
	for _, i := range []int{1, 3, 5, 7} {
		if out[i].AnomalyInjected || out[i].LastTradedPrice != 0 {
			t.Errorf("quote row %d must not be touched: %+v", i, out[i])
		}
	}
	eps := readEpisodes(t, epPath)
	if len(eps) < 1 || !strings.Contains(eps[0]["Detail"], "repeat_actual=3") {
		t.Errorf("expected one 3-trade run first, got %v", eps)
	}
}

func TestImplausiblePriceFactor(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase4.Enabled = true
	cfg.Phase4.DateFilter = []string{"10-11-2021"}
	cfg.Phase4.Strategies = []PointFailureStrategy{{Type: "implausible_price", Probability: 1.0, MultiplierRange: []float64{10, 20}}}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2021, 11, 10, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 50; i++ {
		out, _, _ := inj.ProcessTick(&model.RawTick{ID: "A.ETR", Exchange: "ETR", SecType: "E",
			LastTradedPrice: 50, TradingTime: base.Add(time.Duration(i) * time.Second)})
		ratio := out.LastTradedPrice / 50
		if ratio < 1 {
			ratio = 1 / ratio
		}
		if ratio < 10-1e-9 || ratio > 20+1e-9 {
			t.Fatalf("factor outside [10, 20]: price %v", out.LastTradedPrice)
		}
	}
	inj.Close()

	eps := readEpisodes(t, epPath)
	if len(eps) != 50 || eps[0]["AnomalyType"] != "implausible_price" || eps[0]["Phase"] != "phase4" {
		t.Errorf("expected 50 phase4 implausible_price episodes, got %d (%v)", len(eps), eps[:1])
	}
}

func TestValidateImplausiblePriceRange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.Phase4.Strategies = []PointFailureStrategy{{Type: "implausible_price", Probability: 0.01, MultiplierRange: []float64{0.5, 2}}}
	if err := cfg.Validate(); err == nil {
		t.Error("a multiplier of at most 1 is not implausible and should be rejected")
	}
}
