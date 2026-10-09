package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
	"github.com/google/uuid"
	proto "github.com/netbirdio/netbird/flow/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockDB implements database.Database for testing without a real DB.
type mockDB struct {
	batchFn  func(events []database.EventLog) error
	singleFn func(event *database.EventLog) error
}

func (m *mockDB) CreateEventLogBatch(_ context.Context, events []database.EventLog) error {
	if m.batchFn != nil {
		return m.batchFn(events)
	}
	return nil
}
func (m *mockDB) CreateEventLog(_ context.Context, event *database.EventLog) error {
	if m.singleFn != nil {
		return m.singleFn(event)
	}
	return nil
}
func (m *mockDB) AutoMigrateDB() error { return nil }
func (m *mockDB) QueryEvents(_ context.Context, _ time.Time) ([]database.EventLog, error) {
	return nil, nil
}
func (m *mockDB) QueryEventsPaged(_ context.Context, _ time.Time, _ uint, _ int) ([]database.EventLog, error) {
	return nil, nil
}
func (m *mockDB) UpsertSession(_ *database.SessionRecord) error                  { return nil }
func (m *mockDB) GetOpenSession(_, _, _ string) (*database.SessionRecord, error) { return nil, nil }
func (m *mockDB) GetWatermark() (time.Time, error)                               { return time.Time{}, nil }
func (m *mockDB) SetWatermark(_ time.Time) error                                 { return nil }
func (m *mockDB) CloseInactiveSessions(_ time.Duration) (int64, error)           { return 0, nil }
func (m *mockDB) TryAdvisoryLock(_ int64) (bool, error)                          { return true, nil }
func (m *mockDB) ReleaseAdvisoryLock(_ int64) error                              { return nil }
func (m *mockDB) GetAllOpenSessions() ([]database.SessionRecord, error)          { return nil, nil }
func (m *mockDB) PruneEventLogs(_ time.Duration) (int64, error)                  { return 0, nil }

// makeTestEvent returns a minimal valid TCP FlowEvent with the given event UUID.
func makeTestEvent(eventID uuid.UUID) *proto.FlowEvent {
	flowID := uuid.New()
	return &proto.FlowEvent{
		EventId:   eventID[:],
		Timestamp: timestamppb.Now(),
		PublicKey: []byte("testkey"),
		FlowFields: &proto.FlowFields{
			FlowId:    flowID[:],
			RuleId:    []byte("rule-1"),
			Protocol:  uint32(database.TCP),
			Type:      proto.Type_TYPE_START,
			Direction: proto.Direction_EGRESS,
			SourceIp:  net.ParseIP("10.0.0.1"),
			DestIp:    net.ParseIP("10.0.0.2"),
			ConnectionInfo: &proto.FlowFields_PortInfo{
				PortInfo: &proto.PortInfo{SourcePort: 1234, DestPort: 80},
			},
		},
	}
}

func TestFlowEventToDatabaseEvent(t *testing.T) {
	uuid01 := uuid.New()
	uuid02 := uuid.New()
	uuid03 := uuid.New()
	uuid04 := uuid.New()

	publicKey1String := "PublicKey1"
	publicKey1Encoded := base64.StdEncoding.EncodeToString([]byte(publicKey1String)) // "PublicKey1" in base64
	publicKey1, err := base64.StdEncoding.DecodeString(publicKey1Encoded)            // []byte representation of "PublicKey1"
	if err != nil {
		t.Fatalf("Failed to decode public key: %v", err)
	}

	publicKey2String := "publicKey2"
	publicKey2Encoded := base64.StdEncoding.EncodeToString([]byte(publicKey2String)) // "publicKey2" in base64
	publicKey2, err := base64.StdEncoding.DecodeString(publicKey2Encoded)            // []byte representation of "PublicKey2"
	if err != nil {
		t.Fatalf("Failed to decode public key: %v", err)
	}

	eventTime := time.Date(2026, 10, 9, 18, 31, 2, 123456000, time.UTC)
	eventTime2 := eventTime.Add(42 * time.Second)

	tests := []struct {
		name     string
		input    *proto.FlowEvent
		expected database.EventLog
	}{
		{
			name: "TCP Event",
			input: &proto.FlowEvent{
				EventId:   uuid01[:],
				Timestamp: timestamppb.New(eventTime),
				PublicKey: publicKey1,
				FlowFields: &proto.FlowFields{
					FlowId:           uuid02[:],
					RuleId:           []byte("rule-id-123"),
					SourceResourceId: []byte("source-resource-id"),
					DestResourceId:   []byte("dest-resource-id"),
					Protocol:         uint32(database.TCP),
					Type:             proto.Type_TYPE_START,
					Direction:        proto.Direction_EGRESS,
					SourceIp:         net.ParseIP("192.168.1.1"),
					DestIp:           net.ParseIP("192.168.1.2"),
					ConnectionInfo: &proto.FlowFields_PortInfo{
						PortInfo: &proto.PortInfo{
							SourcePort: 12345,
							DestPort:   80,
						},
					},
					RxPackets: 100,
					TxPackets: 200,
					RxBytes:   1500,
					TxBytes:   3000,
				},
			},
			expected: database.EventLog{
				EventTime: eventTime,
				EventId:   uuid01.String(),
				PublicKey: publicKey1Encoded,
				FlowFields: &database.FlowFields{
					FlowId:           uuid02.String(),
					RuleID:           "rule-id-123",
					SourceResourceID: "source-resource-id",
					DestResourceID:   "dest-resource-id",
					Protocol:         database.TCP.String(),
					Type:             proto.Type_TYPE_START.String(),
					Direction:        strings.ToLower(proto.Direction_EGRESS.String()),
					SourceIp:         "192.168.1.1",
					DestIp:           "192.168.1.2",
					ConnectionInfo: &database.PortInfo{
						SourcePort: 12345,
						DestPort:   80,
					},
					RxPackets: 100,
					TxPackets: 200,
					RxBytes:   1500,
					TxBytes:   3000,
				},
			},
		},
		{
			name: "ICMP Event",
			input: &proto.FlowEvent{
				EventId:   uuid03[:],
				Timestamp: timestamppb.New(eventTime2),
				PublicKey: publicKey2,
				FlowFields: &proto.FlowFields{
					FlowId:           uuid04[:],
					RuleId:           []byte("rule-id-456"),
					SourceResourceId: []byte("source-resource-id-2"),
					DestResourceId:   []byte("dest-resource-id-2"),
					Protocol:         uint32(database.ICMP),
					Type:             proto.Type_TYPE_END,
					Direction:        proto.Direction_INGRESS,
					SourceIp:         net.ParseIP("10.0.0.1"),
					DestIp:           net.ParseIP("10.0.0.2"),
					ConnectionInfo: &proto.FlowFields_IcmpInfo{
						IcmpInfo: &proto.ICMPInfo{
							IcmpType: 8,
							IcmpCode: 0,
						},
					},
					RxPackets: 100,
					TxPackets: 200,
					RxBytes:   1500,
					TxBytes:   3000,
				},
			},
			expected: database.EventLog{
				EventTime: eventTime2,
				EventId:   uuid03.String(),
				PublicKey: publicKey2Encoded,
				FlowFields: &database.FlowFields{
					FlowId:           uuid04.String(),
					RuleID:           "rule-id-456",
					SourceResourceID: "source-resource-id-2",
					DestResourceID:   "dest-resource-id-2",
					Protocol:         database.ICMP.String(),
					Type:             proto.Type_TYPE_END.String(),
					Direction:        strings.ToLower(proto.Direction_INGRESS.String()),
					SourceIp:         "10.0.0.1",
					DestIp:           "10.0.0.2",
					ConnectionInfoIcmp: &database.IcmpInfo{
						IcmpType: 8,
						IcmpCode: 0,
					},
					RxPackets: 100,
					TxPackets: 200,
					RxBytes:   1500,
					TxBytes:   3000,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := FlowEventToDatabaseEvent(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFlowEventToDatabaseEvent_KeepsAnyIPProtocolNumber(t *testing.T) {
	newEvent := func(protocol uint32) *proto.FlowEvent {
		eventID, flowID := uuid.New(), uuid.New()
		return &proto.FlowEvent{
			EventId:    eventID[:],
			Timestamp:  timestamppb.Now(),
			FlowFields: &proto.FlowFields{FlowId: flowID[:], Protocol: protocol},
		}
	}

	for protocol, want := range map[uint32]string{
		0:   "0",    // userspace drop of a protocol the client doesn't classify
		47:  "47",   // GRE, reported by kernel conntrack
		50:  "50",   // ESP
		132: "SCTP", // named
	} {
		got, err := FlowEventToDatabaseEvent(newEvent(protocol))
		assert.NoError(t, err, "protocol %d", protocol)
		assert.Equal(t, want, got.FlowFields.Protocol, "protocol %d", protocol)
	}

	_, err := FlowEventToDatabaseEvent(newEvent(256))
	assert.ErrorContains(t, err, "invalid IP protocol number 256")
}

func TestFlowEventToDatabaseEvent_RequiresTimestamp(t *testing.T) {
	eventID, flowID := uuid.New(), uuid.New()
	newEvent := func(ts *timestamppb.Timestamp) *proto.FlowEvent {
		return &proto.FlowEvent{
			EventId:    eventID[:],
			Timestamp:  ts,
			FlowFields: &proto.FlowFields{FlowId: flowID[:], Protocol: uint32(database.TCP)},
		}
	}

	_, err := FlowEventToDatabaseEvent(newEvent(nil))
	assert.ErrorContains(t, err, "no valid timestamp", "missing timestamp")

	_, err = FlowEventToDatabaseEvent(newEvent(&timestamppb.Timestamp{Seconds: -62135596801}))
	assert.ErrorContains(t, err, "no valid timestamp", "out-of-range timestamp")
}

func TestFlowEventToDatabaseEvent_PanicHandling(t *testing.T) {
	tests := []struct {
		name        string
		input       *proto.FlowEvent
		expectError bool
	}{
		{
			name:        "nil event",
			input:       nil,
			expectError: true,
		},
		{
			name: "nil FlowFields",
			input: &proto.FlowEvent{
				EventId:    func() []byte { u := uuid.New(); return u[:] }(),
				PublicKey:  []byte("key"),
				FlowFields: nil,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Ensure no panic reaches the test runner
			assert.NotPanics(t, func() {
				_, err := FlowEventToDatabaseEvent(tt.input)
				if tt.expectError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})
		})
	}
}

// sendAndClose sends events into ch and closes it, then calls runBatchWorkers with 1 worker.
func runWorkerWithEvents(events []*proto.FlowEvent, db database.Database, cb *CircuitBreaker) {
	ch := make(chan *proto.FlowEvent, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	runBatchWorkers(ch, 1, db, cb)
}

func TestBatchWorker_BatchSuccess(t *testing.T) {
	batchCalls := 0
	singleCalls := 0
	mock := &mockDB{
		batchFn:  func(_ []database.EventLog) error { batchCalls++; return nil },
		singleFn: func(_ *database.EventLog) error { singleCalls++; return nil },
	}
	cb := NewCircuitBreaker(5, 30*time.Second, func(bool) {})

	id1, id2, id3 := uuid.New(), uuid.New(), uuid.New()
	runWorkerWithEvents([]*proto.FlowEvent{
		makeTestEvent(id1), makeTestEvent(id2), makeTestEvent(id3),
	}, mock, cb)

	assert.Equal(t, 1, batchCalls, "expected one batch insert")
	assert.Equal(t, 0, singleCalls, "expected no individual inserts on batch success")
}

func TestBatchWorker_FallbackOnBatchError(t *testing.T) {
	var mu sync.Mutex
	var savedIDs []string
	mock := &mockDB{
		batchFn: func(_ []database.EventLog) error { return errors.New("batch error") },
		singleFn: func(e *database.EventLog) error {
			mu.Lock()
			savedIDs = append(savedIDs, e.EventId)
			mu.Unlock()
			return nil
		},
	}
	cb := NewCircuitBreaker(5, 30*time.Second, func(bool) {})

	id1, id2, id3 := uuid.New(), uuid.New(), uuid.New()
	runWorkerWithEvents([]*proto.FlowEvent{
		makeTestEvent(id1), makeTestEvent(id2), makeTestEvent(id3),
	}, mock, cb)

	assert.ElementsMatch(t, []string{id1.String(), id2.String(), id3.String()}, savedIDs,
		"all events should be saved via individual fallback")
}

func TestBatchWorker_FallbackIsolatesBadRow(t *testing.T) {
	var mu sync.Mutex
	var savedIDs []string
	callCount := 0
	id1, id2, id3 := uuid.New(), uuid.New(), uuid.New()

	mock := &mockDB{
		batchFn: func(_ []database.EventLog) error { return errors.New("batch error") },
		singleFn: func(e *database.EventLog) error {
			mu.Lock()
			callCount++
			n := callCount
			mu.Unlock()
			if n == 2 {
				return errors.New("bad row")
			}
			mu.Lock()
			savedIDs = append(savedIDs, e.EventId)
			mu.Unlock()
			return nil
		},
	}
	cb := NewCircuitBreaker(5, 30*time.Second, func(bool) {})

	runWorkerWithEvents([]*proto.FlowEvent{
		makeTestEvent(id1), makeTestEvent(id2), makeTestEvent(id3),
	}, mock, cb)

	assert.ElementsMatch(t, []string{id1.String(), id3.String()}, savedIDs,
		"only the bad row should be dropped; others should be saved")
}

func TestBatchWorker_CircuitOpen_FlushesOnShutdown(t *testing.T) {
	// When the circuit is open, batches are buffered. On clean shutdown the
	// retry buffer is flushed synchronously (bypassing the circuit breaker).
	batchCalls := 0
	mock := &mockDB{
		batchFn: func(_ []database.EventLog) error { batchCalls++; return nil },
	}
	cb := NewCircuitBreaker(1, 30*time.Second, func(bool) {})
	_ = cb.Call(func() error { return errors.New("force open") })

	runWorkerWithEvents([]*proto.FlowEvent{
		makeTestEvent(uuid.New()), makeTestEvent(uuid.New()),
	}, mock, cb)

	assert.Equal(t, 1, batchCalls, "shutdown flush should write the buffered batch")
}

func TestBatchWorker_RetryBuffer_Full_DropsBatch(t *testing.T) {
	// When the retry buffer is full, additional open-circuit batches are dropped.
	origMax := retryBufferMaxBatches
	retryBufferMaxBatches = 1
	defer func() { retryBufferMaxBatches = origMax }()

	batchCalls := 0
	mock := &mockDB{
		batchFn: func(_ []database.EventLog) error { batchCalls++; return nil },
	}
	cb := NewCircuitBreaker(1, 30*time.Second, func(bool) {})
	_ = cb.Call(func() error { return errors.New("force open") })

	// 3 events → 1 batch (shutdown flush) → retry buffer max=1, so second batch is dropped.
	// We send two separate batches by closing the channel twice via two worker runs.
	ch := make(chan *proto.FlowEvent, 10)
	ch <- makeTestEvent(uuid.New()) // first batch → buffered (buffer now full)
	ch <- makeTestEvent(uuid.New()) // second batch → dropped (buffer full)
	close(ch)
	runBatchWorkers(ch, 1, mock, cb)

	// Shutdown flush writes exactly one batch (the one that fit in the buffer).
	assert.Equal(t, 1, batchCalls, "only the batch that fit in the retry buffer should be flushed on shutdown")
}

func TestBatchWorker_RetryBuffer_FlushedOnCircuitRecovery(t *testing.T) {
	// Speed up ticker intervals for this test.
	origRetry := retryFlushInterval
	retryFlushInterval = 1 * time.Millisecond
	defer func() { retryFlushInterval = origRetry }()

	var mu sync.Mutex
	batchCalls := 0
	mock := &mockDB{
		batchFn: func(_ []database.EventLog) error {
			mu.Lock()
			batchCalls++
			mu.Unlock()
			return nil
		},
	}

	// Circuit opens on first failure, recovers after 1ms.
	cb := NewCircuitBreaker(1, 1*time.Millisecond, func(bool) {})
	_ = cb.Call(func() error { return errors.New("force open") })

	// Send events while the circuit is open so they land in the retry buffer,
	// then keep the channel open long enough for the background retry goroutine
	// to fire after the circuit resets to HalfOpen.
	ch := make(chan *proto.FlowEvent, 10)
	ch <- makeTestEvent(uuid.New())
	ch <- makeTestEvent(uuid.New())

	// Run workers in background; close the channel after giving the retry
	// goroutine time to flush (circuit resets after 1ms, retry polls every 1ms).
	done := make(chan struct{})
	go func() {
		defer close(done)
		runBatchWorkers(ch, 1, mock, cb)
	}()

	// Wait until at least one batch write has occurred via background retry.
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return batchCalls >= 1
	}, 2*time.Second, time.Millisecond)

	close(ch)
	<-done
}
