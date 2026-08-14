// Package simulator provides timing control and orchestration for market data replay.
package simulator

import (
	"time"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

type SimulationMode string

const (
	ModeRealtime    SimulationMode = "realtime"
	ModeAccelerated SimulationMode = "accelerated"
	ModeFullSpeed   SimulationMode = "fullspeed"
)

type TimingSimulator struct {
	mode               SimulationMode
	accelerationFactor float64
	lastTickTime       time.Time
	firstTick          bool
}

// NewTimingSimulator creates a new timing simulator with the specified mode and acceleration factor.
func NewTimingSimulator(mode SimulationMode, factor float64) *TimingSimulator {
	return &TimingSimulator{
		mode:               mode,
		accelerationFactor: factor,
		firstTick:          true,
	}
}

// WaitForNextTick delays execution to maintain simulation timing based on the mode.
func (s *TimingSimulator) WaitForNextTick(tick *model.RawTick) {
	if s.mode == ModeFullSpeed {
		return
	}

	if s.firstTick {
		s.lastTickTime = tick.TradingTime
		s.firstTick = false
		return
	}

	delay := s.CalculateDelay(tick)
	if delay > 0 {
		time.Sleep(delay)
	}

	s.lastTickTime = tick.TradingTime
}

// CalculateDelay computes the delay needed before processing the next tick.
func (s *TimingSimulator) CalculateDelay(tick *model.RawTick) time.Duration {
	if s.mode == ModeFullSpeed {
		return 0
	}

	if s.firstTick {
		return 0
	}

	diff := tick.TradingTime.Sub(s.lastTickTime)
	if diff <= 0 {
		return 0
	}

	if s.mode == ModeAccelerated {
		diff = time.Duration(float64(diff) / s.accelerationFactor)
	}

	return diff
}

// Reset clears the timing state for a new simulation run.
func (s *TimingSimulator) Reset() {
	s.firstTick = true
	s.lastTickTime = time.Time{}
}
