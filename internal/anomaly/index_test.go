package anomaly

import (
	"testing"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

// Index rows never reach the detector, so nothing is injected into them and they leave no
// trace in the ground truth.
func TestIndexRowsAreNeverInjected(t *testing.T) {
	cfg, epPath := baseConfig(t)
	cfg.Phase1.Enabled = true
	cfg.Phase1.DateFilter = []string{"10-11-2021"}
	cfg.Phase1.Window = TimeWindow{Start: "09:00:00", End: "17:00:00"}
	cfg.Phase1.InstrumentRatio = 1.0
	cfg.Phase1.InitialRate, cfg.Phase1.FinalRate = 0, 0 // drop everything it touches
	cfg.Phase2.Enabled = true
	cfg.Phase2.DateFilter = []string{"10-11-2021"}
	cfg.Phase2.Window = TimeWindow{Start: "09:00:00", End: "17:00:00"}
	cfg.Phase2.Strategies = []ContextualStrategy{{Type: "price_spike", Probability: 1.0, DeviationRange: []float64{3, 3}}}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2021, 11, 10, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		out, dropped, _ := inj.ProcessTick(&model.RawTick{ID: "DAX.ETR", Exchange: "ETR", SecType: "I",
			LastTradedPrice: 15000, TradingTime: base.Add(time.Duration(i) * time.Second)})
		if dropped || out.AnomalyInjected || out.LastTradedPrice != 15000 {
			t.Fatalf("an index row was modified: dropped=%v %+v", dropped, out)
		}
	}
	// an equity in the same run is injected as configured
	dropped := false
	for i := 0; i < 5; i++ {
		_, d, _ := inj.ProcessTick(&model.RawTick{ID: "SAP.ETR", Exchange: "ETR", SecType: "E",
			LastTradedPrice: 100, TradingTime: base.Add(time.Duration(i) * time.Second)})
		dropped = dropped || d
	}
	inj.Close()

	if !dropped {
		t.Error("equities should still be injected")
	}
	for _, ep := range readEpisodes(t, epPath) {
		if ep["InstrumentID"] == "DAX.ETR" {
			t.Errorf("the ground truth must not contain index episodes: %v", ep)
		}
	}
}
