package anomaly

import (
	"testing"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

func TestAnomalyInjectorDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatalf("Failed to create disabled injector: %v", err)
	}

	if inj != nil {
		t.Error("Expected nil injector when disabled")
	}
}

func TestAnomalyInjectorCreation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.LogFile = "" // Disable file logging for test

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatalf("Failed to create injector: %v", err)
	}
	defer inj.Close()

	if inj == nil {
		t.Fatal("Expected non-nil injector when enabled")
	}

	stats := inj.GetStats()
	if stats.TotalProcessed != 0 {
		t.Errorf("Expected 0 processed, got %d", stats.TotalProcessed)
	}
}

func TestPhase1TickRateDecline(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.LogFile = ""
	cfg.Phase1.Enabled = true
	cfg.Phase1.Window = TimeWindow{Start: "09:00:00", End: "10:00:00"}
	cfg.Phase1.InitialRate = 1.0
	cfg.Phase1.FinalRate = 0.0  // Drop all by end
	cfg.Phase1.InstrumentRatio = 1.0 // All instruments

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatalf("Failed to create injector: %v", err)
	}
	defer inj.Close()

	// Test tick at start of window (should mostly pass)
	tick1 := &model.RawTick{
		ID:          "TEST.ETR",
		Exchange:    "ETR",
		TradingTime: time.Date(2021, 11, 8, 9, 0, 0, 0, time.UTC),
	}

	passCount := 0
	for i := 0; i < 100; i++ {
		_, dropped, err := inj.ProcessTick(tick1)
		if err != nil {
			t.Fatalf("ProcessTick error: %v", err)
		}
		if !dropped {
			passCount++
		}
	}

	if passCount < 80 { // At least 80% should pass at start
		t.Errorf("Expected ~100%% pass rate at start, got %d%%", passCount)
	}

	// Test tick at end of window (should mostly drop)
	tick2 := &model.RawTick{
		ID:          "TEST.ETR",
		Exchange:    "ETR",
		TradingTime: time.Date(2021, 11, 8, 9, 59, 0, 0, time.UTC),
	}

	dropCount := 0
	for i := 0; i < 100; i++ {
		_, dropped, err := inj.ProcessTick(tick2)
		if err != nil {
			t.Fatalf("ProcessTick error: %v", err)
		}
		if dropped {
			dropCount++
		}
	}

	if dropCount < 80 { // At least 80% should drop at end
		t.Errorf("Expected ~100%% drop rate at end, got %d%%", dropCount)
	}
}

func TestPhase2ContextualAnomalies(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.LogFile = ""
	cfg.Phase1.Enabled = false
	cfg.Phase2.Enabled = true
	cfg.Phase2.DateFilter = []string{"09-11-2021"}
	cfg.Phase3.Enabled = false
	cfg.Phase4.Enabled = false
	cfg.Phase2.Strategies = []ContextualStrategy{
		{
			Type:           "price_spike",
			Probability:    1.0,
			DeviationRange: []float64{2.0, 3.0},
		},
	}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatalf("Failed to create injector: %v", err)
	}
	defer inj.Close()

	baseTime := time.Date(2021, 11, 9, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 50; i++ {
		tick := &model.RawTick{
			ID:              "TEST.ETR",
			Exchange:        "ETR",
			LastTradedPrice: 100.0,
			TradingTime:     baseTime.Add(time.Duration(i) * time.Second),
		}
		inj.ProcessTick(tick)
	}

	tick := &model.RawTick{
		ID:              "TEST.ETR",
		Exchange:        "ETR",
		LastTradedPrice: 100.0,
		TradingTime:     baseTime.Add(time.Minute),
	}

	modified, dropped, err := inj.ProcessTick(tick)
	if err != nil {
		t.Fatalf("ProcessTick error: %v", err)
	}

	if dropped {
		t.Error("Tick should not be dropped in Phase 2")
	}

	if !modified.AnomalyInjected {
		t.Error("Expected anomaly to be injected")
	}

	if modified.AnomalyType != "price_spike" {
		t.Errorf("Expected price_spike, got %s", modified.AnomalyType)
	}

	if modified.LastTradedPrice < 200 || modified.LastTradedPrice > 300 {
		t.Errorf("Expected price 200-300, got %.2f", modified.LastTradedPrice)
	}
}

func TestPhase3FeedSilence(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.LogFile = ""
	cfg.Phase3.Enabled = true
	cfg.Phase3.Window = TimeWindow{Start: "09:00:00", End: "10:00:00"}
	cfg.Phase3.BlackoutSeconds = 10
	cfg.Phase3.InstrumentRatio = 1.0
	cfg.Phase3.ExchangeFilter = []string{} // No filter for test

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatalf("Failed to create injector: %v", err)
	}
	defer inj.Close()

	baseTime := time.Date(2021, 11, 8, 9, 30, 0, 0, time.UTC)

	// First tick should trigger blackout
	tick1 := &model.RawTick{
		ID:          "TEST.ETR",
		Exchange:    "ETR",
		TradingTime: baseTime,
	}

	_, dropped1, _ := inj.ProcessTick(tick1)
	if !dropped1 {
		t.Error("First tick should be dropped (blackout start)")
	}

	// Tick 5 seconds later should still be dropped (within 10s blackout)
	tick2 := &model.RawTick{
		ID:          "TEST.ETR",
		Exchange:    "ETR",
		TradingTime: baseTime.Add(5 * time.Second),
	}

	_, dropped2, _ := inj.ProcessTick(tick2)
	if !dropped2 {
		t.Error("Tick within blackout should be dropped")
	}

	// Tick 11 seconds later should pass (blackout expired)
	tick3 := &model.RawTick{
		ID:          "TEST.ETR",
		Exchange:    "ETR",
		TradingTime: baseTime.Add(11 * time.Second),
	}

	_, dropped3, _ := inj.ProcessTick(tick3)
	if dropped3 {
		t.Error("Tick after blackout should not be dropped")
	}
	
	// Test with different instrument to verify independence
	tick4 := &model.RawTick{
		ID:          "OTHER.FR",
		Exchange:    "FR",
		TradingTime: baseTime.Add(1 * time.Second),
	}
	
	_, dropped4, _ := inj.ProcessTick(tick4)
	if !dropped4 {
		t.Error("Different instrument should also be blacklisted")
	}
}

func TestPhase4PointFailures(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.LogFile = ""
	cfg.Phase1.Enabled = false
	cfg.Phase2.Enabled = false
	cfg.Phase3.Enabled = false
	cfg.Phase4.Enabled = true
	cfg.Phase4.DateFilter = []string{"10-11-2021"}
	cfg.Phase4.Strategies = []PointFailureStrategy{
		{
			Type:        "null_price",
			Probability: 1.0,
			Field:       []string{"Bid"},
		},
	}

	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatalf("Failed to create injector: %v", err)
	}
	defer inj.Close()

	tick := &model.RawTick{
		ID:          "TEST.ETR",
		Exchange:    "ETR",
		Bid:         100.0,
		Ask:         101.0,
		TradingTime: time.Date(2021, 11, 10, 10, 30, 0, 0, time.UTC),
	}

	modified, dropped, err := inj.ProcessTick(tick)
	if err != nil {
		t.Fatalf("ProcessTick error: %v", err)
	}

	if dropped {
		t.Error("Phase 4 should not drop ticks")
	}

	if !modified.AnomalyInjected {
		t.Error("Expected anomaly to be injected")
	}

	if modified.Bid != 0 {
		t.Errorf("Expected Bid to be nulled, got %.2f", modified.Bid)
	}

	if modified.Ask != 101.0 {
		t.Errorf("Ask should be unchanged, got %.2f", modified.Ask)
	}
}

func TestTimeWindowChecking(t *testing.T) {
	window := TimeWindow{Start: "09:30:00", End: "10:30:00"}

	testCases := []struct {
		time     time.Time
		expected bool
	}{
		{time.Date(2021, 11, 8, 9, 0, 0, 0, time.UTC), false},  // Before
		{time.Date(2021, 11, 8, 9, 30, 0, 0, time.UTC), true},  // Start
		{time.Date(2021, 11, 8, 10, 0, 0, 0, time.UTC), true},  // Middle
		{time.Date(2021, 11, 8, 10, 30, 0, 0, time.UTC), true},  // End
		{time.Date(2021, 11, 8, 11, 0, 0, 0, time.UTC), false}, // After
	}

	for _, tc := range testCases {
		inWindow, err := window.IsInWindow(tc.time)
		if err != nil {
			t.Errorf("Error checking window for %v: %v", tc.time, err)
		}
		if inWindow != tc.expected {
			t.Errorf("Time %v: expected %v, got %v", 
				tc.time.Format("15:04:05"), tc.expected, inWindow)
		}
	}
}

func TestSequentialValidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	
	if err := cfg.Validate(); err != nil {
		t.Errorf("Sequential default config should be valid: %v", err)
	}
	
	overlappingCfg := DefaultConfig()
	overlappingCfg.Enabled = true
	overlappingCfg.Phase2.DateFilter = []string{"08-11-2021"}
	overlappingCfg.Phase2.Window.Start = "10:00:00"
	overlappingCfg.Phase2.Window.End = "15:00:00"
	
	if err := overlappingCfg.Validate(); err == nil {
		t.Error("Overlapping phases on same date should fail validation")
	} else {
		t.Logf("Correctly rejected overlapping config: %v", err)
	}
}

func TestDateFiltering(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.LogFile = ""
	cfg.Phase1.Enabled = true
	cfg.Phase1.DateFilter = []string{"08-11-2021"}
	cfg.Phase2.Enabled = false
	cfg.Phase3.Enabled = false
	cfg.Phase4.Enabled = false
	
	inj, err := NewInjector(cfg)
	if err != nil {
		t.Fatalf("Failed to create injector: %v", err)
	}
	defer inj.Close()
	
	monday := time.Date(2021, 11, 8, 9, 45, 0, 0, time.UTC)
	tuesday := time.Date(2021, 11, 9, 9, 45, 0, 0, time.UTC)
	
	tickMonday := &model.RawTick{
		ID:          "TEST.ETR",
		Exchange:    "ETR",
		TradingTime: monday,
	}
	
	tickTuesday := &model.RawTick{
		ID:          "TEST.ETR",
		Exchange:    "ETR",
		TradingTime: tuesday,
	}
	
	mondayDropped := 0
	for i := 0; i < 100; i++ {
		_, dropped, _ := inj.ProcessTick(tickMonday)
		if dropped {
			mondayDropped++
		}
	}
	
	tuesdayDropped := 0
	for i := 0; i < 100; i++ {
		_, dropped, _ := inj.ProcessTick(tickTuesday)
		if dropped {
			tuesdayDropped++
		}
	}
	
	if mondayDropped == 0 {
		t.Error("Expected some ticks dropped on Monday (date filter match)")
	}
	
	if tuesdayDropped > 0 {
		t.Errorf("Expected NO ticks dropped on Tuesday (date filter mismatch), got %d", tuesdayDropped)
	}
	
	t.Logf("Monday (08-11-2021): %d/100 dropped ✓", mondayDropped)
	t.Logf("Tuesday (09-11-2021): %d/100 dropped ✓", tuesdayDropped)
}

