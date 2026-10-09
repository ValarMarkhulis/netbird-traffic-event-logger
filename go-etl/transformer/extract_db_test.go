package transformer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
)

func makeEventLog(createdAt time.Time, ff *database.FlowFields, publicKey string) database.EventLog {
	return database.EventLog{
		Model:      gorm.Model{CreatedAt: createdAt},
		PublicKey:  publicKey,
		FlowFields: ff,
	}
}

func TestExtractFromDB_TCPEvent(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	dbEvents := []database.EventLog{
		makeEventLog(ts, &database.FlowFields{
			FlowId:    "flow1",
			Type:      "TYPE_START",
			Direction: "ingress",
			Protocol:  "TCP",
			SourceIp:  "10.0.0.1",
			DestIp:    "10.0.0.2",
			ConnectionInfo: &database.PortInfo{
				SourcePort: 54321,
				DestPort:   3389,
			},
			RxPackets: 10,
			TxPackets: 5,
			RxBytes:   1000,
			TxBytes:   500,
		}, "peer1"),
	}

	events := ExtractFromDB(dbEvents)

	require.Len(t, events, 1)
	e := events[0]
	assert.Equal(t, "TCP", e.Protocol)
	assert.Equal(t, "ingress", e.Direction)
	assert.Equal(t, "TYPE_START", e.Type)
	assert.Equal(t, "peer1", e.PublicKey)
	assert.Equal(t, "flow1", e.FlowID)
	assert.Equal(t, "10.0.0.1", e.SourceIP)
	assert.Equal(t, "10.0.0.2", e.DestIP)
	assert.Equal(t, "54321", e.ConnSourcePort)
	assert.Equal(t, "3389", e.ConnDestPort)
	assert.Equal(t, 10, e.RxPackets)
	assert.Equal(t, 5, e.TxPackets)
	assert.Equal(t, ts, e.CreatedAt)
}

func TestExtractFromDB_ICMPEvent(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	dbEvents := []database.EventLog{
		makeEventLog(ts, &database.FlowFields{
			FlowId:    "flow2",
			Type:      "TYPE_END",
			Direction: "ingress",
			Protocol:  "ICMP",
			SourceIp:  "10.0.0.1",
			DestIp:    "10.0.0.2",
			ConnectionInfoIcmp: &database.IcmpInfo{
				IcmpType: 8,
				IcmpCode: 0,
			},
			RxPackets: 4,
			TxPackets: 4,
		}, "routerPeer"),
	}

	events := ExtractFromDB(dbEvents)

	require.Len(t, events, 1)
	e := events[0]
	assert.Equal(t, "ICMP", e.Protocol)
	assert.Equal(t, "8", e.IcmpType)
	assert.Equal(t, "0", e.IcmpCode)
	assert.Empty(t, e.ConnSourcePort)
	assert.Empty(t, e.ConnDestPort)
}

func TestExtractFromDB_NilFlowFieldsSkipped(t *testing.T) {
	dbEvents := []database.EventLog{
		{Model: gorm.Model{}, PublicKey: "peer1", FlowFields: nil},
	}

	events := ExtractFromDB(dbEvents)
	assert.Empty(t, events)
}

func TestExtractFromDB_ZeroPortOmitted(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	dbEvents := []database.EventLog{
		makeEventLog(ts, &database.FlowFields{
			FlowId:    "flow3",
			Type:      "TYPE_START",
			Direction: "ingress",
			Protocol:  "TCP",
			ConnectionInfo: &database.PortInfo{
				SourcePort: 0,
				DestPort:   0,
			},
		}, "peer1"),
	}

	events := ExtractFromDB(dbEvents)

	require.Len(t, events, 1)
	assert.Empty(t, events[0].ConnSourcePort)
	assert.Empty(t, events[0].ConnDestPort)
}

func TestExtractFromDB_EmptyInput(t *testing.T) {
	events := ExtractFromDB(nil)
	assert.Empty(t, events)
}

func TestExtractFromDB_PreservesCreatedAt(t *testing.T) {
	ts := time.Date(2025, 6, 15, 9, 30, 0, 0, time.UTC)
	dbEvents := []database.EventLog{
		makeEventLog(ts, &database.FlowFields{
			FlowId:    "flow4",
			Type:      "TYPE_START",
			Direction: "egress",
			Protocol:  "UDP",
		}, "peer2"),
	}

	events := ExtractFromDB(dbEvents)

	require.Len(t, events, 1)
	assert.Equal(t, ts, events[0].CreatedAt)
}
