package worker

import (
	"testing"
	"time"
)

func TestBackoffCalculator_ComputeDuration(t *testing.T) {
	initial := 10 * time.Second
	maxInterval := 1 * time.Hour
	multiplier := 2.0

	// Use deterministic randFn returning 1.0 (maximum jitter)
	calcMax := &BackoffCalculator{
		InitialInterval: initial,
		MaxInterval:     maxInterval,
		Multiplier:      multiplier,
		RandFn:          func() float64 { return 1.0 },
	}

	// Use deterministic randFn returning 0.0 (minimum jitter floor: 25%)
	calcMin := &BackoffCalculator{
		InitialInterval: initial,
		MaxInterval:     maxInterval,
		Multiplier:      multiplier,
		RandFn:          func() float64 { return 0.0 },
	}

	tests := []struct {
		attempt      int
		expectedMax  time.Duration
		expectedMin  time.Duration
	}{
		{attempt: 0, expectedMax: 10 * time.Second, expectedMin: 2500 * time.Millisecond},
		{attempt: 1, expectedMax: 20 * time.Second, expectedMin: 5 * time.Second},
		{attempt: 2, expectedMax: 40 * time.Second, expectedMin: 10 * time.Second},
		{attempt: 3, expectedMax: 80 * time.Second, expectedMin: 20 * time.Second},
		{attempt: 4, expectedMax: 160 * time.Second, expectedMin: 40 * time.Second},
	}

	for _, tt := range tests {
		gotMax := calcMax.ComputeDuration(tt.attempt)
		if gotMax != tt.expectedMax {
			t.Errorf("attempt %d max: expected %v, got %v", tt.attempt, tt.expectedMax, gotMax)
		}

		gotMin := calcMin.ComputeDuration(tt.attempt)
		if gotMin != tt.expectedMin {
			t.Errorf("attempt %d min: expected %v, got %v", tt.attempt, tt.expectedMin, gotMin)
		}
	}
}

func TestBackoffCalculator_MaxCap(t *testing.T) {
	initial := 10 * time.Second
	maxInterval := 100 * time.Second
	multiplier := 2.0

	calc := &BackoffCalculator{
		InitialInterval: initial,
		MaxInterval:     maxInterval,
		Multiplier:      multiplier,
		RandFn:          func() float64 { return 1.0 },
	}

	// 10 * 2^10 = 10240s, capped at 100s
	got := calc.ComputeDuration(10)
	if got != maxInterval {
		t.Fatalf("expected capped duration %v, got %v", maxInterval, got)
	}
}

func TestBackoffCalculator_NegativeAttempt(t *testing.T) {
	calc := NewBackoffCalculator(5*time.Second, 1*time.Minute, 2.0)
	dur := calc.ComputeDuration(-5)
	if dur <= 0 || dur > 5*time.Second {
		t.Fatalf("unexpected duration for negative attempt: %v", dur)
	}
}
