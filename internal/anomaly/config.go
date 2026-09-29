package anomaly

import (
	"fmt"
	"time"
)

type Config struct {
	Enabled bool   `yaml:"enabled"`
	Seed    int64  `yaml:"seed"`
	LogFile string `yaml:"log_file"`

	EpisodeFile    string `yaml:"episode_file,omitempty"`
	InstrumentFile string `yaml:"instrument_file,omitempty"`

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
	DateFilter      []string   `yaml:"date_filter,omitempty"`
	Window          TimeWindow `yaml:"window"`
	DeclinePattern  string     `yaml:"decline_pattern"`
	InitialRate     float64    `yaml:"initial_rate"`
	FinalRate       float64    `yaml:"final_rate"`
	InstrumentRatio float64    `yaml:"instrument_ratio"`
}

type Phase2Config struct {
	Enabled            bool        `yaml:"enabled"`
	DateFilter         []string    `yaml:"date_filter,omitempty"`
	Window             TimeWindow  `yaml:"window"`
	ContextWindowHours float64     `yaml:"context_window_hours"`
	Quota              QuotaConfig `yaml:"per_instrument_quota"`

	Strategies []ContextualStrategy `yaml:"strategies"`
}

type ContextualStrategy struct {
	Type        string  `yaml:"type"`
	Probability float64 `yaml:"probability"`

	DeviationRange       []float64 `yaml:"deviation_range,omitempty"`
	RepeatCount          []int     `yaml:"repeat_count,omitempty"`
	MinRelativeDeviation float64   `yaml:"min_relative_deviation,omitempty"`
}

type Phase3Config struct {
	Enabled         bool       `yaml:"enabled"`
	DateFilter      []string   `yaml:"date_filter,omitempty"`
	Window          TimeWindow `yaml:"window"`
	BlackoutSeconds int        `yaml:"blackout_seconds"`
	InstrumentRatio float64    `yaml:"instrument_ratio"`
	ExchangeFilter  []string   `yaml:"exchange_filter,omitempty"`
}

type Phase4Config struct {
	Enabled    bool                   `yaml:"enabled"`
	DateFilter []string               `yaml:"date_filter,omitempty"`
	Window     TimeWindow             `yaml:"window"`
	Strategies []PointFailureStrategy `yaml:"strategies"`
	Quota      QuotaConfig            `yaml:"per_instrument_quota"`
}

type PointFailureStrategy struct {
	Type        string  `yaml:"type"`
	Probability float64 `yaml:"probability"`

	Field           []string  `yaml:"field,omitempty"`
	MultiplierRange []float64 `yaml:"multiplier_range,omitempty"`
	Corruption      []string  `yaml:"corruption,omitempty"`
	RewindSeconds   []int     `yaml:"rewind_seconds,omitempty"`
}

func DefaultConfig() Config {
	return Config{
		Enabled: false,
		Seed:    42,
		LogFile: "anomaly_log.csv",

		Phase1: Phase1Config{
			Enabled:    true,
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
			Enabled:    true,
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
					Type:           "price_deviation",
					Probability:    0.01,
					DeviationRange: []float64{4.0, 8.0},
				},
			},
		},

		Phase3: Phase3Config{
			Enabled:    true,
			DateFilter: []string{"08-11-2021"},
			Window: TimeWindow{
				Start: "14:30:00",
				End:   "16:00:00",
			},
			BlackoutSeconds: 30,
			InstrumentRatio: 0.7,
			ExchangeFilter:  []string{"ETR"},
		},

		Phase4: Phase4Config{
			Enabled:    true,
			DateFilter: []string{"10-11-2021"},
			Window: TimeWindow{
				Start: "09:30:00",
				End:   "15:30:00",
			},
			Strategies: []PointFailureStrategy{
				{
					Type:            "implausible_price",
					Probability:     0.02,
					MultiplierRange: []float64{10, 100},
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

	return c.validateStrategies()
}

func (c Config) validateStrategies() error {
	if err := c.Phase2.Quota.validate("Phase2"); err != nil {
		return err
	}
	if err := c.Phase4.Quota.validate("Phase4"); err != nil {
		return err
	}

	checkRange := func(name string, lo, hi float64) error {
		if lo > hi {
			return fmt.Errorf("%s: range [%v, %v] has min > max", name, lo, hi)
		}
		return nil
	}

	if c.Phase2.Enabled {
		for _, s := range c.Phase2.Strategies {
			if s.Probability < 0 || s.Probability > 1 {
				return fmt.Errorf("Phase2 %s: probability %v not in [0, 1]", s.Type, s.Probability)
			}
			switch s.Type {
			case "price_spike":
				if len(s.DeviationRange) != 2 {
					return fmt.Errorf("Phase2 price_spike: deviation_range must have 2 values")
				}
				if err := checkRange("Phase2 price_spike deviation_range", s.DeviationRange[0], s.DeviationRange[1]); err != nil {
					return err
				}
			case "price_deviation":
				if len(s.DeviationRange) != 0 {
					if len(s.DeviationRange) != 2 {
						return fmt.Errorf("Phase2 price_deviation: deviation_range must have 2 values")
					}
					if err := checkRange("Phase2 price_deviation deviation_range", s.DeviationRange[0], s.DeviationRange[1]); err != nil {
						return err
					}
				}
			case "stale_price":
				if len(s.RepeatCount) != 0 {
					if len(s.RepeatCount) != 2 || s.RepeatCount[0] < 1 {
						return fmt.Errorf("Phase2 stale_price: repeat_count must be [min>=1, max]")
					}
					if err := checkRange("Phase2 stale_price repeat_count", float64(s.RepeatCount[0]), float64(s.RepeatCount[1])); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("Phase2: unknown strategy type %q (valid: price_spike, price_deviation, stale_price)", s.Type)
			}
		}
	}

	if c.Phase4.Enabled {
		for _, s := range c.Phase4.Strategies {
			if s.Probability < 0 || s.Probability > 1 {
				return fmt.Errorf("Phase4 %s: probability %v not in [0, 1]", s.Type, s.Probability)
			}
			switch s.Type {
			case "null_price":
				if len(s.Field) == 0 {
					return fmt.Errorf("Phase4 null_price: field must list at least one of Last, Bid, Ask, both")
				}
				for _, f := range s.Field {
					if f != "Last" && f != "Bid" && f != "Ask" && f != "both" {
						return fmt.Errorf("Phase4 null_price: unknown field %q (valid: Last, Bid, Ask, both)", f)
					}
				}
			case "implausible_price":
				if len(s.MultiplierRange) != 0 {
					if len(s.MultiplierRange) != 2 || s.MultiplierRange[0] <= 1 {
						return fmt.Errorf("Phase4 implausible_price: multiplier_range must be [min>1, max]")
					}
					if err := checkRange("Phase4 implausible_price multiplier_range", s.MultiplierRange[0], s.MultiplierRange[1]); err != nil {
						return err
					}
				}
			case "malformed_isin":
				if len(s.Corruption) == 0 {
					return fmt.Errorf("Phase4 malformed_isin: corruption must list at least one of truncate, random_chars")
				}
				for _, k := range s.Corruption {
					if k != "truncate" && k != "random_chars" {
						return fmt.Errorf("Phase4 malformed_isin: unknown corruption %q", k)
					}
				}
			case "timestamp_inversion":
				if len(s.RewindSeconds) != 2 || s.RewindSeconds[0] < 0 {
					return fmt.Errorf("Phase4 timestamp_inversion: rewind_seconds must be [min>=0, max]")
				}
				if err := checkRange("Phase4 timestamp_inversion rewind_seconds", float64(s.RewindSeconds[0]), float64(s.RewindSeconds[1])); err != nil {
					return err
				}
			default:
				return fmt.Errorf("Phase4: unknown strategy type %q (valid: null_price, implausible_price, malformed_isin, timestamp_inversion)", s.Type)
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
