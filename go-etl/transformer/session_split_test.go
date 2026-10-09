package transformer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeGrouped is a helper that wraps a slice of events into the map
// SplitSessions expects, using a fixed key.
func makeGrouped(key [3]string, events []Event) map[[3]string][]Event {
	return map[[3]string][]Event{key: events}
}

var defaultKey = [3]string{"senderPeer", "routerPeer", "3389"}

func t0() time.Time {
	return time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
}

// TestSplitSessions_SingleSession verifies that a TYPE_START + TYPE_END pair
// produces exactly one session with correct peer metadata.
func TestSplitSessions_SingleSession(t *testing.T) {
	start := t0()
	end := start.Add(30 * time.Second)

	events := []Event{
		{Type: "TYPE_START", EventTime: start, FlowID: "flow1", ConnSourcePort: "54321", SourceIP: "10.0.0.1", DestIP: "10.0.0.2", RxPackets: 0},
		{Type: "TYPE_END", EventTime: end, FlowID: "flow1", ConnSourcePort: "54321", SourceIP: "10.0.0.1", DestIP: "10.0.0.2", RxPackets: 50},
	}

	sessions := SplitSessions(makeGrouped(defaultKey, events), 0.5)

	require.Len(t, sessions, 1)
	assert.Equal(t, "senderPeer", sessions[0].SenderPeer)
	assert.Equal(t, "routerPeer", sessions[0].RoutingPeer)
	assert.Equal(t, "3389", sessions[0].DestPort)
	assert.Contains(t, sessions[0].FlowIDs, "flow1")
}

// TestSplitSessions_TwoSessionsSplitByTrafficAndTimeGap verifies that when a
// TYPE_END dominates accumulated traffic and the next TYPE_START is >2s away,
// the events are split into two separate sessions.
func TestSplitSessions_TwoSessionsSplitByTrafficAndTimeGap(t *testing.T) {
	base := t0()

	events := []Event{
		{Type: "TYPE_START", EventTime: base, FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 0},
		{Type: "TYPE_END", EventTime: base.Add(10 * time.Second), FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 100},
		// 15s gap to next TYPE_START — triggers split
		{Type: "TYPE_START", EventTime: base.Add(25 * time.Second), FlowID: "flow2", ConnSourcePort: "10002", RxPackets: 0},
		{Type: "TYPE_END", EventTime: base.Add(35 * time.Second), FlowID: "flow2", ConnSourcePort: "10002", RxPackets: 100},
	}

	sessions := SplitSessions(makeGrouped(defaultKey, events), 0.5)

	require.Len(t, sessions, 2)
	assert.Contains(t, sessions[0].FlowIDs, "flow1")
	assert.Contains(t, sessions[1].FlowIDs, "flow2")
}

// TestSplitSessions_NoSplitWhenTimeGapTooSmall verifies that a TYPE_END
// followed by a TYPE_START within 2 seconds does not trigger a split.
func TestSplitSessions_NoSplitWhenTimeGapTooSmall(t *testing.T) {
	base := t0()

	events := []Event{
		{Type: "TYPE_START", EventTime: base, FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 0},
		{Type: "TYPE_END", EventTime: base.Add(10 * time.Second), FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 100},
		// Only 1s gap — too small to split
		{Type: "TYPE_START", EventTime: base.Add(11 * time.Second), FlowID: "flow2", ConnSourcePort: "10002", RxPackets: 0},
		{Type: "TYPE_END", EventTime: base.Add(20 * time.Second), FlowID: "flow2", ConnSourcePort: "10002", RxPackets: 100},
	}

	sessions := SplitSessions(makeGrouped(defaultKey, events), 0.5)

	require.Len(t, sessions, 1)
}

// TestSplitSessions_ZeroTrafficNoEndDropped verifies that a session with no
// TYPE_END event and zero traffic is silently discarded.
func TestSplitSessions_ZeroTrafficNoEndDropped(t *testing.T) {
	base := t0()

	events := []Event{
		{Type: "TYPE_START", EventTime: base, FlowID: "flow1", ConnSourcePort: "10001",
			RxPackets: 0, TxPackets: 0, RxBytes: 0, TxBytes: 0},
	}

	sessions := SplitSessions(makeGrouped(defaultKey, events), 0.5)
	assert.Empty(t, sessions)
}

// TestSplitSessions_StartAndEndTimestampsSet verifies that Start and End
// are populated from the TYPE_START and TYPE_END events.
func TestSplitSessions_StartAndEndTimestampsSet(t *testing.T) {
	start := t0()
	end := start.Add(30 * time.Second)

	events := []Event{
		{Type: "TYPE_START", EventTime: start, FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 0},
		{Type: "TYPE_END", EventTime: end, FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 50},
	}

	sessions := SplitSessions(makeGrouped(defaultKey, events), 0.5)

	require.Len(t, sessions, 1)
	require.NotNil(t, sessions[0].Start)
	require.NotNil(t, sessions[0].End)
	assert.Equal(t, start, *sessions[0].Start)
	assert.Equal(t, end, *sessions[0].End)
}

// TestSplitSessions_PacketCountsAccumulated verifies that RxPackets and
// TxPackets from all events in a session are summed correctly.
func TestSplitSessions_PacketCountsAccumulated(t *testing.T) {
	base := t0()

	events := []Event{
		{Type: "TYPE_START", EventTime: base, FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 5, TxPackets: 3},
		{Type: "TYPE_END", EventTime: base.Add(30 * time.Second), FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 95, TxPackets: 47},
	}

	sessions := SplitSessions(makeGrouped(defaultKey, events), 0.5)

	require.Len(t, sessions, 1)
	assert.Equal(t, 100, sessions[0].RxPackets)
	assert.Equal(t, 50, sessions[0].TxPackets)
}

// TestMergeSessions_SharedSourcePortMerges verifies that two sessions sharing
// a source port are merged into one.
func TestMergeSessions_SharedSourcePortMerges(t *testing.T) {
	base := t0()

	// Two separate sessions (large gap) but with the same source port
	events := []Event{
		{Type: "TYPE_START", EventTime: base, FlowID: "flow1", ConnSourcePort: "54321", RxPackets: 0},
		{Type: "TYPE_END", EventTime: base.Add(10 * time.Second), FlowID: "flow1", ConnSourcePort: "54321", RxPackets: 100},
		{Type: "TYPE_START", EventTime: base.Add(25 * time.Second), FlowID: "flow2", ConnSourcePort: "54321", RxPackets: 0},
		{Type: "TYPE_END", EventTime: base.Add(35 * time.Second), FlowID: "flow2", ConnSourcePort: "54321", RxPackets: 100},
	}

	sessions := SplitSessions(makeGrouped(defaultKey, events), 0.5)

	// The two sessions share port 54321, so they must be merged back
	require.Len(t, sessions, 1)
	assert.Contains(t, sessions[0].FlowIDs, "flow1")
	assert.Contains(t, sessions[0].FlowIDs, "flow2")
	assert.Contains(t, sessions[0].SrcPorts, "54321")
}

// TestMergeSessions_SharedFlowIDMerges verifies that two sessions sharing a
// flow ID are merged into one.
func TestMergeSessions_SharedFlowIDMerges(t *testing.T) {
	base := t0()

	// flow1 appears in both split halves
	events := []Event{
		{Type: "TYPE_START", EventTime: base, FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 0},
		{Type: "TYPE_END", EventTime: base.Add(10 * time.Second), FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 100},
		{Type: "TYPE_START", EventTime: base.Add(25 * time.Second), FlowID: "flow1", ConnSourcePort: "10002", RxPackets: 0},
		{Type: "TYPE_END", EventTime: base.Add(35 * time.Second), FlowID: "flow1", ConnSourcePort: "10002", RxPackets: 100},
	}

	sessions := SplitSessions(makeGrouped(defaultKey, events), 0.5)

	require.Len(t, sessions, 1)
	assert.Contains(t, sessions[0].SrcPorts, "10001")
	assert.Contains(t, sessions[0].SrcPorts, "10002")
}

// TestSplitSessions_RuleIDTakenFromFirstNonEmpty verifies that RuleID is set
// from the first event in the group that carries a non-empty value.
func TestSplitSessions_RuleIDTakenFromFirstNonEmpty(t *testing.T) {
	base := t0()

	events := []Event{
		{Type: "TYPE_START", EventTime: base, FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 0, RuleID: ""},
		{Type: "TYPE_END", EventTime: base.Add(10 * time.Second), FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 50, RuleID: "rule-first"},
		{Type: "TYPE_END", EventTime: base.Add(20 * time.Second), FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 10, RuleID: "rule-second"},
	}

	sessions := SplitSessions(makeGrouped(defaultKey, events), 0.5)

	require.Len(t, sessions, 1)
	assert.Equal(t, "rule-first", sessions[0].RuleID)
}

// TestSplitSessions_ResourceIDsPropagated verifies that SourceResourceID and
// DestResourceID are carried through from the events into the session.
func TestSplitSessions_ResourceIDsPropagated(t *testing.T) {
	base := t0()

	events := []Event{
		{Type: "TYPE_START", EventTime: base, FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 0,
			SourceResourceID: "src-res-1", DestResourceID: "dst-res-1"},
		{Type: "TYPE_END", EventTime: base.Add(10 * time.Second), FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 50},
	}

	sessions := SplitSessions(makeGrouped(defaultKey, events), 0.5)

	require.Len(t, sessions, 1)
	assert.Equal(t, "src-res-1", sessions[0].SourceResourceID)
	assert.Equal(t, "dst-res-1", sessions[0].DestResourceID)
}

// TestMergeSessions_RuleIDFromFirstSessionPreserved verifies that when two
// split sessions are merged, the RuleID from the first session is kept.
func TestMergeSessions_RuleIDFromFirstSessionPreserved(t *testing.T) {
	base := t0()

	// Two segments sharing source port 54321 — will be split then merged.
	events := []Event{
		{Type: "TYPE_START", EventTime: base, FlowID: "flow1", ConnSourcePort: "54321", RxPackets: 0, RuleID: "rule-A"},
		{Type: "TYPE_END", EventTime: base.Add(10 * time.Second), FlowID: "flow1", ConnSourcePort: "54321", RxPackets: 100, RuleID: "rule-A"},
		{Type: "TYPE_START", EventTime: base.Add(25 * time.Second), FlowID: "flow2", ConnSourcePort: "54321", RxPackets: 0, RuleID: "rule-B"},
		{Type: "TYPE_END", EventTime: base.Add(35 * time.Second), FlowID: "flow2", ConnSourcePort: "54321", RxPackets: 100, RuleID: "rule-B"},
	}

	sessions := SplitSessions(makeGrouped(defaultKey, events), 0.5)

	require.Len(t, sessions, 1)
	assert.Equal(t, "rule-A", sessions[0].RuleID)
}

// TestSplitSessions_OutputOrderedByStartTime verifies that multiple groups are
// returned in ascending start-time order regardless of map iteration order.
func TestSplitSessions_OutputOrderedByStartTime(t *testing.T) {
	base := t0()

	grouped := map[[3]string][]Event{
		{"sender", "router", "22"}: {
			{Type: "TYPE_START", EventTime: base.Add(20 * time.Second), FlowID: "flow2", ConnSourcePort: "20002", RxPackets: 0},
			{Type: "TYPE_END", EventTime: base.Add(30 * time.Second), FlowID: "flow2", ConnSourcePort: "20002", RxPackets: 50},
		},
		{"sender", "router", "3389"}: {
			{Type: "TYPE_START", EventTime: base, FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 0},
			{Type: "TYPE_END", EventTime: base.Add(10 * time.Second), FlowID: "flow1", ConnSourcePort: "10001", RxPackets: 50},
		},
	}

	sessions := SplitSessions(grouped, 0.5)

	require.Len(t, sessions, 2)
	assert.Equal(t, "3389", sessions[0].DestPort, "earlier session should come first")
	assert.Equal(t, "22", sessions[1].DestPort)
}
