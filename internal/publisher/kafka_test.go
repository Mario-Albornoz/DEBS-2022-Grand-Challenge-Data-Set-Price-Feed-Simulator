package publisher

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/config"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

func TestNewKafkaPublisher(t *testing.T) {
	cfg := config.Default()
	pub, err := NewKafkaPublisher(&cfg)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}

	if pub.writer == nil {
		t.Error("Writer is nil")
	}
	if pub.config == nil {
		t.Error("Config is nil")
	}

	pub.Close()
}

func TestPublisherStats(t *testing.T) {
	cfg := config.Default()
	pub, err := NewKafkaPublisher(&cfg)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}
	defer pub.Close()

	stats := pub.GetStats()
	if stats.Published != 0 {
		t.Errorf("Initial Published = %d, want 0", stats.Published)
	}
	if stats.Failed != 0 {
		t.Errorf("Initial Failed = %d, want 0", stats.Failed)
	}
}

func TestJSONSerialization(t *testing.T) {
	tick := &model.RawTick{
		ID:          "RDSA.NL",
		Exchange:    "NL",
		SecType:     "E",
		ISIN:        "NL0011267392",
		Bid:         100.50,
		Ask:         100.75,
		TotalVolume: 12345.67,
		TradingTime: time.Now(),
		Date:        time.Now(),
		Time:        time.Now(),
	}

	data, err := json.Marshal(tick)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded model.RawTick
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.ID != tick.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, tick.ID)
	}
	if decoded.Bid != tick.Bid {
		t.Errorf("Bid = %f, want %f", decoded.Bid, tick.Bid)
	}
}

func BenchmarkPublish(b *testing.B) {
	tick := &model.RawTick{
		ID:          "RDSA.NL",
		Exchange:    "NL",
		SecType:     "E",
		ISIN:        "NL0011267392",
		Bid:         100.50,
		Ask:         100.75,
		TotalVolume: 12345.67,
		TradingTime: time.Now(),
		Date:        time.Now(),
		Time:        time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(tick)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONMarshal(b *testing.B) {
	tick := &model.RawTick{
		ID:          "RDSA.NL",
		Exchange:    "NL",
		SecType:     "E",
		ISIN:        "NL0011267392",
		Bid:         100.50,
		Ask:         100.75,
		TotalVolume: 12345.67,
		TradingTime: time.Date(2021, 11, 8, 9, 30, 45, 0, time.UTC),
		Date:        time.Date(2021, 11, 8, 0, 0, 0, 0, time.UTC),
		Time:        time.Date(2021, 11, 8, 9, 30, 45, 0, time.UTC),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data, err := json.Marshal(tick)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
	}
}
