package worker

import (
	"math"
	"math/rand"
	"time"
)

// BackoffCalculator computes retry intervals with exponential backoff and jitter.
type BackoffCalculator struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	RandFn          func() float64
}

// NewBackoffCalculator creates a new calculator with default settings and crypto-safe/pseudo random generator.
func NewBackoffCalculator(initialInterval, maxInterval time.Duration, multiplier float64) *BackoffCalculator {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &BackoffCalculator{
		InitialInterval: initialInterval,
		MaxInterval:     maxInterval,
		Multiplier:      multiplier,
		RandFn:          r.Float64,
	}
}

// ComputeDuration calculates the next delay using exponential backoff with full jitter:
// Delay = rand(0, min(maxInterval, initialInterval * multiplier^attempt))
// A minimum floor of initialInterval/4 is enforced to prevent 0s immediate spin.
func (b *BackoffCalculator) ComputeDuration(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	mult := math.Pow(b.Multiplier, float64(attempt))
	rawDuration := float64(b.InitialInterval) * mult

	maxDur := float64(b.MaxInterval)
	if rawDuration > maxDur || math.IsInf(rawDuration, 0) {
		rawDuration = maxDur
	}

	// Full Jitter with minimum floor (25% of raw duration) to avoid immediate re-attempts
	minFloor := rawDuration * 0.25
	randomComponent := (rawDuration - minFloor) * b.RandFn()
	jittered := minFloor + randomComponent

	return time.Duration(jittered)
}
