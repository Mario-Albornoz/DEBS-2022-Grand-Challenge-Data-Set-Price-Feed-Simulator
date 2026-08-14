package simulator

import (
	"testing"
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

func TestNewTimingSimulator(t *testing.T) {
	sim := NewTimingSimulator(ModeRealtime, 1.0)
	if sim.mode != ModeRealtime {
		t.Errorf("Mode = %v, want %v", sim.mode, ModeRealtime)
	}
	if sim.accelerationFactor != 1.0 {
		t.Errorf("AccelerationFactor = %f, want %f", sim.accelerationFactor, 1.0)
	}
	if !sim.firstTick {
		t.Error("Expected firstTick to be true")
	}
}

func TestCalculateDelayFullSpeed(t *testing.T) {
	sim := NewTimingSimulator(ModeFullSpeed, 1.0)

	tick1 := &model.RawTick{TradingTime: time.Now()}
	tick2 := &model.RawTick{TradingTime: time.Now().Add(1 * time.Second)}

	sim.WaitForNextTick(tick1)
	delay := sim.CalculateDelay(tick2)

	if delay != 0 {
		t.Errorf("FullSpeed delay = %v, want 0", delay)
	}
}

func TestCalculateDelayRealtime(t *testing.T) {
	sim := NewTimingSimulator(ModeRealtime, 1.0)

	baseTime := time.Date(2021, 11, 8, 9, 30, 0, 0, time.UTC)
	tick1 := &model.RawTick{TradingTime: baseTime}
	tick2 := &model.RawTick{TradingTime: baseTime.Add(5 * time.Second)}

	sim.WaitForNextTick(tick1)
	delay := sim.CalculateDelay(tick2)

	expected := 5 * time.Second
	if delay != expected {
		t.Errorf("Realtime delay = %v, want %v", delay, expected)
	}
}

func TestCalculateDelayAccelerated(t *testing.T) {
	sim := NewTimingSimulator(ModeAccelerated, 10.0)

	baseTime := time.Date(2021, 11, 8, 9, 30, 0, 0, time.UTC)
	tick1 := &model.RawTick{TradingTime: baseTime}
	tick2 := &model.RawTick{TradingTime: baseTime.Add(10 * time.Second)}

	sim.WaitForNextTick(tick1)
	delay := sim.CalculateDelay(tick2)

	expected := 1 * time.Second
	if delay != expected {
		t.Errorf("Accelerated delay = %v, want %v", delay, expected)
	}
}

func TestCalculateDelayBackwardsTime(t *testing.T) {
	sim := NewTimingSimulator(ModeRealtime, 1.0)

	baseTime := time.Date(2021, 11, 8, 9, 30, 0, 0, time.UTC)
	tick1 := &model.RawTick{TradingTime: baseTime}
	tick2 := &model.RawTick{TradingTime: baseTime.Add(-5 * time.Second)}

	sim.WaitForNextTick(tick1)
	delay := sim.CalculateDelay(tick2)

	if delay != 0 {
		t.Errorf("Backwards time delay = %v, want 0", delay)
	}
}

func TestCalculateDelaySameTime(t *testing.T) {
	sim := NewTimingSimulator(ModeRealtime, 1.0)

	baseTime := time.Date(2021, 11, 8, 9, 30, 0, 0, time.UTC)
	tick1 := &model.RawTick{TradingTime: baseTime}
	tick2 := &model.RawTick{TradingTime: baseTime}

	sim.WaitForNextTick(tick1)
	delay := sim.CalculateDelay(tick2)

	if delay != 0 {
		t.Errorf("Same time delay = %v, want 0", delay)
	}
}

func TestReset(t *testing.T) {
	sim := NewTimingSimulator(ModeRealtime, 1.0)

	baseTime := time.Date(2021, 11, 8, 9, 30, 0, 0, time.UTC)
	tick := &model.RawTick{TradingTime: baseTime}

	sim.WaitForNextTick(tick)

	if sim.firstTick {
		t.Error("Expected firstTick to be false after first tick")
	}

	sim.Reset()

	if !sim.firstTick {
		t.Error("Expected firstTick to be true after reset")
	}
	if !sim.lastTickTime.IsZero() {
		t.Error("Expected lastTickTime to be zero after reset")
	}
}
