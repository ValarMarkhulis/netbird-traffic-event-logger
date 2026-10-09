package pipeline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ValarMarkhulis/traffic-event-logger/go-etl/transformer"
)

func tcpEvent(createdAt time.Time) transformer.Event {
	return transformer.Event{
		Protocol:         "TCP",
		ConnSourcePort:   "54321",
		ConnDestPort:     "3389",
		FlowID:           "flow1",
		PublicKey:        "key1",
		SourceIP:         "10.0.0.1",
		DestIP:           "10.0.0.2",
		Type:             "TYPE_START",
		Direction:        "ingress",
		RxPackets:        10,
		TxPackets:        5,
		RxBytes:          1000,
		TxBytes:          500,
		RuleID:           "rule-1",
		SourceResourceID: "src-1",
		DestResourceID:   "dst-1",
		EventTime:        createdAt,
	}
}

func TestEventToEventLog_TCPMapsAllFields(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	e := tcpEvent(ts)

	rec := eventToEventLog(e)

	assert.Equal(t, "flow1", rec.EventId)
	assert.Equal(t, "key1", rec.PublicKey)
	assert.Equal(t, ts, rec.CreatedAt)

	require.NotNil(t, rec.FlowFields)
	ff := rec.FlowFields
	assert.Equal(t, "flow1", ff.FlowId)
	assert.Equal(t, "TYPE_START", ff.Type)
	assert.Equal(t, "ingress", ff.Direction)
	assert.Equal(t, "TCP", ff.Protocol)
	assert.Equal(t, "10.0.0.1", ff.SourceIp)
	assert.Equal(t, "10.0.0.2", ff.DestIp)
	assert.Equal(t, uint64(10), ff.RxPackets)
	assert.Equal(t, uint64(5), ff.TxPackets)
	assert.Equal(t, uint64(1000), ff.RxBytes)
	assert.Equal(t, uint64(500), ff.TxBytes)
	assert.Equal(t, "rule-1", ff.RuleID)
	assert.Equal(t, "src-1", ff.SourceResourceID)
	assert.Equal(t, "dst-1", ff.DestResourceID)

	require.NotNil(t, ff.ConnectionInfo)
	assert.Equal(t, uint32(54321), ff.ConnectionInfo.SourcePort)
	assert.Equal(t, uint32(3389), ff.ConnectionInfo.DestPort)
	assert.Nil(t, ff.ConnectionInfoIcmp)
}

func TestEventToEventLog_ICMPMapsIcmpInfo(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	e := transformer.Event{
		Protocol:  "ICMP",
		IcmpType:  "8",
		IcmpCode:  "0",
		FlowID:    "flow2",
		PublicKey: "key2",
		SourceIP:  "10.0.0.1",
		DestIP:    "10.0.0.2",
		RxPackets: 4,
		TxPackets: 4,
		RxBytes:   336,
		TxBytes:   336,
		EventTime: ts,
	}

	rec := eventToEventLog(e)

	require.NotNil(t, rec.FlowFields.ConnectionInfoIcmp)
	assert.Equal(t, uint32(8), rec.FlowFields.ConnectionInfoIcmp.IcmpType)
	assert.Equal(t, uint32(0), rec.FlowFields.ConnectionInfoIcmp.IcmpCode)
	assert.Nil(t, rec.FlowFields.ConnectionInfo)
}

func TestEventToEventLog_PreservesCreatedAt(t *testing.T) {
	ts := time.Date(2025, 6, 15, 9, 30, 0, 0, time.UTC)
	e := transformer.Event{Protocol: "TCP", EventTime: ts}

	rec := eventToEventLog(e)

	assert.Equal(t, ts, rec.CreatedAt)
}

func TestEventToEventLog_EmptyPortsOmitConnectionInfo(t *testing.T) {
	e := transformer.Event{Protocol: "TCP", ConnSourcePort: "", ConnDestPort: ""}

	rec := eventToEventLog(e)

	assert.Nil(t, rec.FlowFields.ConnectionInfo)
}

func TestEventToEventLog_EmptyIcmpFieldsOmitIcmpInfo(t *testing.T) {
	e := transformer.Event{Protocol: "ICMP", IcmpType: "", IcmpCode: ""}

	rec := eventToEventLog(e)

	assert.Nil(t, rec.FlowFields.ConnectionInfoIcmp)
}
