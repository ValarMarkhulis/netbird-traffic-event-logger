package main

import (
	"sync"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
)

// retryBuffer holds batches that could not be written due to an open circuit.
// It is bounded: when full, new batches are rejected and must be dropped.
type retryBuffer struct {
	mu         sync.Mutex
	batches    [][]database.EventLog
	maxBatches int
}

func newRetryBuffer(max int) *retryBuffer {
	return &retryBuffer{maxBatches: max}
}

// add copies and enqueues a batch. Returns false if the buffer is full.
func (r *retryBuffer) add(batch []database.EventLog) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.batches) >= r.maxBatches {
		return false
	}
	cp := make([]database.EventLog, len(batch))
	copy(cp, batch)
	r.batches = append(r.batches, cp)
	return true
}

// drain removes and returns all buffered batches.
func (r *retryBuffer) drain() [][]database.EventLog {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.batches
	r.batches = nil
	return b
}

// size returns the number of buffered batches.
func (r *retryBuffer) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.batches)
}
