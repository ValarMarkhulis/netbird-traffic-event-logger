package pipeline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ValarMarkhulis/traffic-event-logger/go-etl/data"
)

func makeSession(senderPeer, routingPeer, destPort string, start, end *time.Time, flowIDs, srcPorts, srcIPs, destIPs []string, rx, tx, rxB, txB int) data.Session {
	s := data.Session{
		SenderPeer:    senderPeer,
		RoutingPeer:   routingPeer,
		DestPort:      destPort,
		Start:         start,
		End:           end,
		FlowIDs:       make(map[string]struct{}),
		SrcPorts:      make(map[string]struct{}),
		SrcIPs:        make(map[string]struct{}),
		DestIPs:       make(map[string]struct{}),
		RxPackets:     rx,
		TxPackets:     tx,
		RxBytes:       rxB,
		TxBytes:       txB,
		TypeEndEvents: []data.TypeEndEvent{},
	}
	for _, f := range flowIDs {
		s.FlowIDs[f] = struct{}{}
	}
	for _, p := range srcPorts {
		s.SrcPorts[p] = struct{}{}
	}
	for _, ip := range srcIPs {
		s.SrcIPs[ip] = struct{}{}
	}
	for _, ip := range destIPs {
		s.DestIPs[ip] = struct{}{}
	}
	return s
}

func TestSessionToRecord_BasicFields(t *testing.T) {
	start := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(30 * time.Second)

	s := makeSession(
		"senderPeer", "routerPeer", "3389",
		&start, &end,
		[]string{"flow1", "flow2"},
		[]string{"54321", "54322"},
		[]string{"10.0.0.1"},
		[]string{"192.168.1.1"},
		100, 50, 10000, 5000,
	)

	rec := sessionToRecord(s)

	assert.Equal(t, "senderPeer", rec.SenderPeer)
	assert.Equal(t, "routerPeer", rec.RoutingPeer)
	assert.Equal(t, "3389", rec.DestPort)
	assert.Equal(t, start, rec.StartTime)
	require.NotNil(t, rec.EndTime)
	assert.Equal(t, end, *rec.EndTime)
	assert.Equal(t, int64(100), rec.RxPackets)
	assert.Equal(t, int64(50), rec.TxPackets)
	assert.Equal(t, int64(10000), rec.RxBytes)
	assert.Equal(t, int64(5000), rec.TxBytes)
}

func TestSessionToRecord_FlowIDsSorted(t *testing.T) {
	s := makeSession(
		"s", "r", "22", nil, nil,
		[]string{"flow-c", "flow-a", "flow-b"},
		nil, nil, nil, 0, 0, 0, 0,
	)

	rec := sessionToRecord(s)

	assert.Equal(t, []string{"flow-a", "flow-b", "flow-c"}, rec.FlowIDs)
}

func TestSessionToRecord_SrcPortsSorted(t *testing.T) {
	s := makeSession(
		"s", "r", "3389", nil, nil,
		nil,
		[]string{"60000", "10001", "30000"},
		nil, nil, 0, 0, 0, 0,
	)

	rec := sessionToRecord(s)

	assert.Equal(t, []string{"10001", "30000", "60000"}, rec.SrcPorts)
}

func TestSessionToRecord_NilStartUsesZeroTime(t *testing.T) {
	s := makeSession("s", "r", "3389", nil, nil, nil, nil, nil, nil, 0, 0, 0, 0)

	rec := sessionToRecord(s)

	assert.True(t, rec.StartTime.IsZero())
}

func TestSessionToRecord_NilEndIsNil(t *testing.T) {
	s := makeSession("s", "r", "3389", nil, nil, nil, nil, nil, nil, 0, 0, 0, 0)

	rec := sessionToRecord(s)

	assert.Nil(t, rec.EndTime)
}

func TestSessionToRecord_ICMPDestPort(t *testing.T) {
	s := makeSession("s", "r", "ICMP", nil, nil, nil, nil, nil, nil, 10, 10, 840, 840)

	rec := sessionToRecord(s)

	assert.Equal(t, "ICMP", rec.DestPort)
}

func TestSessionToRecord_RuleAndResourceIDs(t *testing.T) {
	s := makeSession("s", "r", "3389", nil, nil, nil, nil, nil, nil, 0, 0, 0, 0)
	s.RuleID = "rule-123"
	s.SourceResourceID = "src-res"
	s.DestResourceID = "dst-res"

	rec := sessionToRecord(s)

	assert.Equal(t, "rule-123", rec.RuleID)
	assert.Equal(t, "src-res", rec.SourceResourceID)
	assert.Equal(t, "dst-res", rec.DestResourceID)
}

func TestSessionToRecord_SrcIPsAndDestIPsSorted(t *testing.T) {
	s := makeSession(
		"s", "r", "3389", nil, nil,
		nil, nil,
		[]string{"10.0.0.3", "10.0.0.1", "10.0.0.2"},
		[]string{"192.168.1.2", "192.168.1.1"},
		0, 0, 0, 0,
	)

	rec := sessionToRecord(s)

	assert.Equal(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, rec.SrcIPs)
	assert.Equal(t, []string{"192.168.1.1", "192.168.1.2"}, rec.DestIPs)
}
