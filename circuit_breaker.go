package main

import (
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned when the circuit breaker is open and calls are rejected.
var ErrCircuitOpen = errors.New("circuit breaker open: DB unavailable")

type cbState int

const (
	cbClosed   cbState = iota // normal operation
	cbOpen                    // DB failing — reject calls immediately
	cbHalfOpen                // probing — allow one call through to test recovery
)

// CircuitBreaker protects DB writes from cascading failures.
// Transitions: Closed → Open (on threshold failures) → HalfOpen (after resetAfter) → Closed (on success).
type CircuitBreaker struct {
	mu            sync.Mutex
	state         cbState
	failures      int
	threshold     int
	resetAfter    time.Duration
	lastFailure   time.Time
	onStateChange func(healthy bool)
}

func NewCircuitBreaker(threshold int, resetAfter time.Duration, onStateChange func(bool)) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:     threshold,
		resetAfter:    resetAfter,
		onStateChange: onStateChange,
	}
}

// Call executes fn if the circuit allows it, tracking successes and failures.
func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mu.Lock()
	switch cb.state {
	case cbOpen:
		if time.Since(cb.lastFailure) < cb.resetAfter {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
		cb.state = cbHalfOpen
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	var notify func(bool)
	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()
		if cb.state == cbHalfOpen || cb.failures >= cb.threshold {
			if cb.state != cbOpen {
				cb.state = cbOpen
				notify = cb.onStateChange
			}
		}
	} else if cb.state == cbHalfOpen {
		cb.state = cbClosed
		cb.failures = 0
		notify = cb.onStateChange
	}
	cb.mu.Unlock()

	if notify != nil {
		notify(err == nil)
	}
	return err
}

func (cb *CircuitBreaker) IsHealthy() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state == cbClosed
}
