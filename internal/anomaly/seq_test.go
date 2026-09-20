package anomaly

import (
	"strconv"
	"testing"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

// The ground truth names the exact message, so a score can be matched to it even when
// several of the instrument's messages share a millisecond.
func TestEpisodesCarryTheMessageSequenceNumber(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase4.Enabled = true
	cfg.Phase4.DateFilter = []string{"10-11-2021"}
	cfg.Phase4.Strategies = []PointFailureStrategy{{Type: "implausible_price", Probability: 1.0, MultiplierRange: []float64{10, 10}}}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2021, 11, 10, 10, 0, 0, 412_000_000, time.UTC)
	want := map[uint64]bool{}
	for i := uint64(1); i <= 5; i++ {
		// five trades in the same millisecond, as the real data has
		tick := &model.RawTick{ID: "A.ETR", Exchange: "ETR", SecType: "E", LastTradedPrice: 100, TradingTime: at, Seq: 1000 + i}
		inj.ProcessTick(tick)
		want[1000+i] = true
	}
	inj.Close()

	eps := readEpisodes(t, epPath)
	if len(eps) != 5 {
		t.Fatalf("expected 5 episodes, got %d", len(eps))
	}
	for _, ep := range eps {
		seq, err := strconv.ParseUint(ep["Seq"], 10, 64)
		if err != nil || !want[seq] {
			t.Errorf("episode has Seq %q, want one of 1001..1005", ep["Seq"])
		}
		if ep["StartMs"] != eps[0]["StartMs"] {
			t.Error("test setup: all five share one millisecond")
		}
	}
}
