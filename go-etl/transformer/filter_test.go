package transformer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterByProtocol_SplitsTCPByDirection(t *testing.T) {
	events := []Event{
		{Protocol: "TCP", Direction: "ingress"},
		{Protocol: "TCP", Direction: "egress"},
	}
	ingress, egress := FilterByProtocol(events)
	assert.Len(t, ingress, 1)
	assert.Len(t, egress, 1)
}

func TestFilterByProtocol_IncludesUDP(t *testing.T) {
	events := []Event{
		{Protocol: "UDP", Direction: "ingress"},
		{Protocol: "UDP", Direction: "egress"},
	}
	ingress, egress := FilterByProtocol(events)
	assert.Len(t, ingress, 1)
	assert.Len(t, egress, 1)
}

func TestFilterByProtocol_IncludesICMP(t *testing.T) {
	events := []Event{
		{Protocol: "ICMP", Direction: "ingress"},
		{Protocol: "ICMP", Direction: "egress"},
	}
	ingress, egress := FilterByProtocol(events)
	assert.Len(t, ingress, 1)
	assert.Len(t, egress, 1)
}

func TestFilterByProtocol_ExcludesUnknownProtocol(t *testing.T) {
	events := []Event{
		{Protocol: "SCTP", Direction: "ingress"},
		{Protocol: "", Direction: "egress"},
		{Protocol: "GRE", Direction: "ingress"},
	}
	ingress, egress := FilterByProtocol(events)
	assert.Empty(t, ingress)
	assert.Empty(t, egress)
}

func TestFilterByProtocol_MixedBatch(t *testing.T) {
	events := []Event{
		{Protocol: "TCP", Direction: "ingress"},
		{Protocol: "UDP", Direction: "ingress"},
		{Protocol: "ICMP", Direction: "ingress"},
		{Protocol: "TCP", Direction: "egress"},
		{Protocol: "SCTP", Direction: "ingress"}, // dropped
	}
	ingress, egress := FilterByProtocol(events)
	assert.Len(t, ingress, 3)
	assert.Len(t, egress, 1)
}

func TestFilterByProtocol_EmptyInput(t *testing.T) {
	ingress, egress := FilterByProtocol(nil)
	assert.Empty(t, ingress)
	assert.Empty(t, egress)
}

func TestFilterByProtocol_UnknownDirectionDropped(t *testing.T) {
	events := []Event{
		{Protocol: "TCP", Direction: "unknown"},
		{Protocol: "UDP", Direction: ""},
	}
	ingress, egress := FilterByProtocol(events)
	assert.Empty(t, ingress)
	assert.Empty(t, egress)
}
