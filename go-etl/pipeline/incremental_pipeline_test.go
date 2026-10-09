package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
)

// mockIncrementalDB is a minimal Database stub for incremental pipeline tests.
// Each field is a function so individual tests can override only what they need;
// unset fields fall through to safe no-op defaults.
type mockIncrementalDB struct {
	tryLockFn      func(int64) (bool, error)
	releaseLockFn  func(int64) error
	getWatermarkFn func() (time.Time, error)
	setWatermarkFn func(time.Time) error
	queryEventsFn  func(context.Context, time.Time) ([]database.EventLog, error)
	upsertFn       func(*database.SessionRecord) error
	closeFn        func(time.Duration) (int64, error)
}

func (m *mockIncrementalDB) AutoMigrateDB() error { return nil }
func (m *mockIncrementalDB) CreateEventLog(_ context.Context, _ *database.EventLog) error {
	return nil
}
func (m *mockIncrementalDB) CreateEventLogBatch(_ context.Context, _ []database.EventLog) error {
	return nil
}
func (m *mockIncrementalDB) GetOpenSession(_, _, _ string) (*database.SessionRecord, error) {
	return nil, nil
}
func (m *mockIncrementalDB) GetAllOpenSessions() ([]database.SessionRecord, error) {
	return nil, nil
}

func (m *mockIncrementalDB) TryAdvisoryLock(key int64) (bool, error) {
	if m.tryLockFn != nil {
		return m.tryLockFn(key)
	}
	return true, nil
}
func (m *mockIncrementalDB) ReleaseAdvisoryLock(key int64) error {
	if m.releaseLockFn != nil {
		return m.releaseLockFn(key)
	}
	return nil
}
func (m *mockIncrementalDB) GetWatermark() (time.Time, error) {
	if m.getWatermarkFn != nil {
		return m.getWatermarkFn()
	}
	return time.Time{}, nil
}
func (m *mockIncrementalDB) SetWatermark(t time.Time) error {
	if m.setWatermarkFn != nil {
		return m.setWatermarkFn(t)
	}
	return nil
}
func (m *mockIncrementalDB) QueryEvents(ctx context.Context, since time.Time) ([]database.EventLog, error) {
	if m.queryEventsFn != nil {
		return m.queryEventsFn(ctx, since)
	}
	return nil, nil
}
func (m *mockIncrementalDB) QueryEventsPaged(ctx context.Context, since time.Time, _ uint, _ int) ([]database.EventLog, error) {
	if m.queryEventsFn != nil {
		return m.queryEventsFn(ctx, since)
	}
	return nil, nil
}
func (m *mockIncrementalDB) UpsertSession(s *database.SessionRecord) error {
	if m.upsertFn != nil {
		return m.upsertFn(s)
	}
	return nil
}
func (m *mockIncrementalDB) CloseInactiveSessions(d time.Duration) (int64, error) {
	if m.closeFn != nil {
		return m.closeFn(d)
	}
	return 0, nil
}
func (m *mockIncrementalDB) PruneEventLogs(_ time.Duration) (int64, error) { return 0, nil }

// --- Advisory lock tests ---

func TestRunIncremental_SkipsWhenLockHeld(t *testing.T) {
	// When another instance holds the lock, the run should exit immediately
	// without touching the watermark or events.
	watermarkCalled := false
	mock := &mockIncrementalDB{
		tryLockFn: func(_ int64) (bool, error) { return false, nil },
		getWatermarkFn: func() (time.Time, error) {
			watermarkCalled = true
			return time.Time{}, nil
		},
	}

	result := RunIncremental(mock, 0.5, 0)

	assert.Equal(t, 0, result)
	assert.False(t, watermarkCalled, "should not read watermark when lock is not acquired")
}

func TestRunIncremental_ReturnsZeroOnLockError(t *testing.T) {
	// A DB error when trying to acquire the lock should abort the run.
	watermarkCalled := false
	mock := &mockIncrementalDB{
		tryLockFn: func(_ int64) (bool, error) { return false, errors.New("connection error") },
		getWatermarkFn: func() (time.Time, error) {
			watermarkCalled = true
			return time.Time{}, nil
		},
	}

	result := RunIncremental(mock, 0.5, 0)

	assert.Equal(t, 0, result)
	assert.False(t, watermarkCalled, "should not read watermark when lock acquisition fails")
}

func TestRunIncremental_ReleasesLockAfterRun(t *testing.T) {
	// The advisory lock must be released even when the run processes no events.
	released := false
	mock := &mockIncrementalDB{
		tryLockFn:     func(_ int64) (bool, error) { return true, nil },
		releaseLockFn: func(_ int64) error { released = true; return nil },
	}

	RunIncremental(mock, 0.5, 0)

	assert.True(t, released, "advisory lock must be released after the run completes")
}

func TestRunIncremental_ReleasesLockOnWatermarkError(t *testing.T) {
	// The advisory lock must be released even when an early step (watermark read)
	// returns an error.
	released := false
	mock := &mockIncrementalDB{
		tryLockFn:      func(_ int64) (bool, error) { return true, nil },
		releaseLockFn:  func(_ int64) error { released = true; return nil },
		getWatermarkFn: func() (time.Time, error) { return time.Time{}, errors.New("db down") },
	}

	result := RunIncremental(mock, 0.5, 0)

	assert.Equal(t, 0, result)
	assert.True(t, released, "advisory lock must be released even when the run errors early")
}

func TestRunIncremental_ProcessesEventsWhenLockAcquired(t *testing.T) {
	// When the lock is acquired and there are new events, sessions should be upserted.
	upsertCalled := 0
	watermarkUpdated := false

	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	// Two events that form a session: one egress (sender) and one ingress (router).
	events := []database.EventLog{
		{
			EventId:   "evt-egress",
			PublicKey: "senderKey",
			FlowFields: &database.FlowFields{
				FlowId:         "flow-1",
				Type:           "TYPE_START",
				Direction:      "egress",
				Protocol:       "TCP",
				SourceIp:       "10.0.0.1",
				DestIp:         "10.0.0.2",
				ConnectionInfo: &database.PortInfo{SourcePort: 54321, DestPort: 3389},
			},
		},
		{
			EventId:   "evt-ingress",
			PublicKey: "routerKey",
			FlowFields: &database.FlowFields{
				FlowId:         "flow-1",
				Type:           "TYPE_START",
				Direction:      "ingress",
				Protocol:       "TCP",
				SourceIp:       "10.0.0.1",
				DestIp:         "10.0.0.2",
				ConnectionInfo: &database.PortInfo{SourcePort: 54321, DestPort: 3389},
			},
		},
	}
	for i := range events {
		events[i].CreatedAt = t0.Add(time.Duration(i) * time.Second)
	}

	mock := &mockIncrementalDB{
		tryLockFn:      func(_ int64) (bool, error) { return true, nil },
		getWatermarkFn: func() (time.Time, error) { return time.Time{}, nil },
		queryEventsFn:  func(_ context.Context, _ time.Time) ([]database.EventLog, error) { return events, nil },
		upsertFn:       func(_ *database.SessionRecord) error { upsertCalled++; return nil },
		setWatermarkFn: func(_ time.Time) error { watermarkUpdated = true; return nil },
	}

	result := RunIncremental(mock, 0.5, 0)

	assert.GreaterOrEqual(t, upsertCalled, 1, "at least one session should be upserted")
	assert.True(t, watermarkUpdated, "watermark should be advanced after a successful run")
	assert.GreaterOrEqual(t, result, 1)
}
