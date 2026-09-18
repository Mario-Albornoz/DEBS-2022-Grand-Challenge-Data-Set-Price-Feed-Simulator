package anomaly

import (
	"fmt"
	"time"
)

type Config struct {
	Enabled bool  `yaml:"enabled"`
	Seed    int64 `yaml:"seed"`
	LogFile string `yaml:"log_file"`

	Phase1 Phase1Config `yaml:"phase1_tick_rate_decline"`
	Phase2 Phase2Config `yaml:"phase2_contextual_anomalies"`
	Phase3 Phase3Config `yaml:"phase3_feed_silence"`
	Phase4 Phase4Config `yaml:"phase4_point_failures"`
}

type TimeWindow struct {
	Start string `yaml:"start"`
	End   string `yaml:"end"`
}

type Phase1Config struct {
	Enabled         bool       `yaml:"enabled"`
	DateFilter      []string   `yaml:"date_filter,omitempty"` // ["08-11-2021"] = only these dates
	Window          TimeWindow `yaml:"window"`
	DeclinePattern  string     `yaml:"decline_pattern"`
	InitialRate     float64    `yaml:"initial_rate"`     // 1.0 = 100%
	FinalRate       float64    `yaml:"final_rate"`       // 0.3 = 30%
	InstrumentRatio float64    `yaml:"instrument_ratio"` // 0.4 = 40% of instruments
}

type Phase2Config struct {
	Enabled            bool       `yaml:"enabled"`
	DateFilter         []string   `yaml:"date_filter,omitempty"` // ["09-11-2021"]
	Window             TimeWindow `yaml:"window"`
	ContextWindowHours float64    `yaml:"context_window_hours"` // Hours of market history
	
	Strategies []ContextualStrategy `yaml:"strategies"`
}

type ContextualStrategy struct {
	Type        string    `yaml:"type"`
	Probability float64   `yaml:"probability"` // 0.03 = 3%
	
	DeviationRange []float64 `yaml:"deviation_range,omitempty"` // [2.0, 5.0] = 2x-5x multiplier
	RepeatCount    []int     `yaml:"repeat_count,omitempty"`    // [3, 10] times
}

type Phase3Config struct {
	Enabled         bool       `yaml:"enabled"`
	DateFilter      []string   `yaml:"date_filter,omitempty"` // ["08-11-2021"]
	Window          TimeWindow `yaml:"window"`
	BlackoutSeconds int        `yaml:"blackout_seconds"` // Total blackout duration
	InstrumentRatio float64    `yaml:"instrument_ratio"` // 0.7 = 70%
	ExchangeFilter  []string   `yaml:"exchange_filter,omitempty"`
}

type Phase4Config struct {
	Enabled    bool                   `yaml:"enabled"`
	DateFilter []string               `yaml:"date_filter,omitempty"` // ["10-11-2021"]
	Window     TimeWindow             `yaml:"window"`
	Strategies []PointFailureStrategy `yaml:"strategies"`
}

type PointFailureStrategy struct {
	Type        string   `yaml:"type"`
	Probability float64  `yaml:"probability"` // 0.02 = 2%
	
	Field         []string `yaml:"field,omitempty"`
	Corruption    []string `yaml:"corruption,omitempty"`
	RewindSeconds []int    `yaml:"rewind_seconds,omitempty"` // [1, 300] seconds
}

func DefaultConfig() Config {
	return Config{
		Enabled: false,
		Seed:    42,
		LogFile: "anomaly_log.csv",
		
		Phase1: Phase1Config{
			Enabled: true,
			DateFilter: []string{"08-11-2021"},
			Window: TimeWindow{
				Start: "09:30:00",
				End:   "14:00:00",
			},
			DeclinePattern:  "linear",
			InitialRate:     1.0,
			FinalRate:       0.3,
			InstrumentRatio: 0.4,
		},
		
		Phase2: Phase2Config{
			Enabled: true,
			DateFilter: []string{"09-11-2021"},
			Window: TimeWindow{
				Start: "09:30:00",
				End:   "15:00:00",
			},
			ContextWindowHours: 1.0,
			Strategies: []ContextualStrategy{
				{
					Type:           "price_spike",
					Probability:    0.03,
					DeviationRange: []float64{2.0, 5.0},
				},
				{
					Type:        "stale_price",
					Probability: 0.05,
					RepeatCount: []int{3, 10},
				},
				{
					Type:        "bid_ask_inversion",
					Probability: 0.01,
				},
			},
		},
		
		Phase3: Phase3Config{
			Enabled: true,
			DateFilter: []string{"08-11-2021"},
			Window: TimeWindow{
				Start: "14:30:00",
				End:   "16:00:00",
			},
			BlackoutSeconds: 30,
			InstrumentRatio: 0.7,
			ExchangeFilter:  []string{"XETRA"},
		},
		
		Phase4: Phase4Config{
			Enabled: true,
			DateFilter: []string{"10-11-2021"},
			Window: TimeWindow{
				Start: "09:30:00",
				End:   "15:30:00",
			},
			Strategies: []PointFailureStrategy{
				{
					Type:        "null_price",
					Probability: 0.02,
					Field:       []string{"Bid", "Ask", "both"},
				},
				{
					Type:        "malformed_isin",
					Probability: 0.01,
					Corruption:  []string{"truncate", "random_chars"},
				},
				{
					Type:          "timestamp_inversion",
					Probability:   0.015,
					RewindSeconds: []int{1, 300},
				},
			},
		},
	}
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	
	phases := []struct {
		name       string
		enabled    bool
		window     TimeWindow
		dateFilter []string
	}{
		{"Phase1", c.Phase1.Enabled, c.Phase1.Window, c.Phase1.DateFilter},
		{"Phase2", c.Phase2.Enabled, c.Phase2.Window, c.Phase2.DateFilter},
		{"Phase3", c.Phase3.Enabled, c.Phase3.Window, c.Phase3.DateFilter},
		{"Phase4", c.Phase4.Enabled, c.Phase4.Window, c.Phase4.DateFilter},
	}
	
	for i, p1 := range phases {
		if !p1.enabled {
			continue
		}
		
		start1, err := ParseTime(p1.window.Start)
		if err != nil {
			return fmt.Errorf("%s: invalid start time: %w", p1.name, err)
		}
		
		end1, err := ParseTime(p1.window.End)
		if err != nil {
			return fmt.Errorf("%s: invalid end time: %w", p1.name, err)
		}
		
		if !start1.Before(end1) {
			return fmt.Errorf("%s: start time must be before end time", p1.name)
		}
		
		for j := i + 1; j < len(phases); j++ {
			p2 := phases[j]
			if !p2.enabled {
				continue
			}
			
			if !datesOverlap(p1.dateFilter, p2.dateFilter) {
				continue
			}
			
			start2, _ := ParseTime(p2.window.Start)
			end2, _ := ParseTime(p2.window.End)
			
			if overlaps(start1, end1, start2, end2) {
				return fmt.Errorf("%s and %s have overlapping time windows on same date (sequential phases required)", 
					p1.name, p2.name)
			}
		}
	}
	
	return nil
}

func datesOverlap(dates1, dates2 []string) bool {
	if len(dates1) == 0 || len(dates2) == 0 {
		return true
	}
	
	for _, d1 := range dates1 {
		for _, d2 := range dates2 {
			if d1 == d2 {
				return true
			}
		}
	}
	
	return false
}

func overlaps(start1, end1, start2, end2 time.Time) bool {
	return !(end1.Before(start2) || start2.Equal(end1) || end2.Before(start1) || start1.Equal(end2))
}

func ParseTime(timeStr string) (time.Time, error) {
	return time.Parse("15:04:05", timeStr)
}

func (tw TimeWindow) IsInWindow(marketTime time.Time) (bool, error) {
	start, err := ParseTime(tw.Start)
	if err != nil {
		return false, err
	}
	
	end, err := ParseTime(tw.End)
	if err != nil {
		return false, err
	}
	
	checkTime := time.Date(0, 1, 1, marketTime.Hour(), marketTime.Minute(), marketTime.Second(), 0, time.UTC)
	
	return !checkTime.Before(start) && !checkTime.After(end), nil
}
