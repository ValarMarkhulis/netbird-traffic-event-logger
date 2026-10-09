package transformer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupByPeers_TCPMatchesBySourceAndDestPort(t *testing.T) {
	ingress := []Event{
		{Protocol: "TCP", ConnSourcePort: "54321", ConnDestPort: "3389", PublicKey: "routerPeer"},
	}
	egress := []Event{
		{Protocol: "TCP", ConnSourcePort: "54321", ConnDestPort: "3389", PublicKey: "senderPeer", FlowID: "flow1"},
	}

	grouped := GroupByPeers(ingress, egress)

	key := [3]string{"senderPeer", "routerPeer", "3389/TCP"}
	assert.Contains(t, grouped, key)
	assert.Len(t, grouped[key], 1)
	assert.Equal(t, "flow1", grouped[key][0].FlowID)
}

func TestGroupByPeers_TCPNoMatchingIngressDropped(t *testing.T) {
	ingress := []Event{
		{Protocol: "TCP", ConnSourcePort: "54321", ConnDestPort: "3389", PublicKey: "routerPeer"},
	}
	egress := []Event{
		{Protocol: "TCP", ConnSourcePort: "99999", ConnDestPort: "3389", PublicKey: "senderPeer"},
	}

	grouped := GroupByPeers(ingress, egress)
	assert.Empty(t, grouped)
}

func TestGroupByPeers_MultipleFlowsSamePeerGroup(t *testing.T) {
	ingress := []Event{
		{Protocol: "TCP", ConnSourcePort: "10001", ConnDestPort: "3389", PublicKey: "routerPeer", FlowID: "flow1"},
		{Protocol: "TCP", ConnSourcePort: "10002", ConnDestPort: "3389", PublicKey: "routerPeer", FlowID: "flow2"},
	}
	egress := []Event{
		{Protocol: "TCP", ConnSourcePort: "10001", ConnDestPort: "3389", PublicKey: "senderPeer"},
		{Protocol: "TCP", ConnSourcePort: "10002", ConnDestPort: "3389", PublicKey: "senderPeer"},
	}

	grouped := GroupByPeers(ingress, egress)

	key := [3]string{"senderPeer", "routerPeer", "3389/TCP"}
	assert.Contains(t, grouped, key)
	assert.Len(t, grouped[key], 2)
}

func TestGroupByPeers_DifferentDestPortsProduceSeparateGroups(t *testing.T) {
	ingress := []Event{
		{Protocol: "TCP", ConnSourcePort: "10001", ConnDestPort: "3389", PublicKey: "routerPeer", FlowID: "flow1"},
		{Protocol: "TCP", ConnSourcePort: "10002", ConnDestPort: "22", PublicKey: "routerPeer", FlowID: "flow2"},
	}
	egress := []Event{
		{Protocol: "TCP", ConnSourcePort: "10001", ConnDestPort: "3389", PublicKey: "senderPeer"},
		{Protocol: "TCP", ConnSourcePort: "10002", ConnDestPort: "22", PublicKey: "senderPeer"},
	}

	grouped := GroupByPeers(ingress, egress)

	assert.Contains(t, grouped, [3]string{"senderPeer", "routerPeer", "3389/TCP"})
	assert.Contains(t, grouped, [3]string{"senderPeer", "routerPeer", "22/TCP"})
	assert.Len(t, grouped, 2)
}

func TestGroupByPeers_ICMPMatchesByIPAndTypeCode(t *testing.T) {
	ingress := []Event{
		{Protocol: "ICMP", SourceIP: "10.0.0.1", DestIP: "10.0.0.2", IcmpType: "8", IcmpCode: "0", PublicKey: "routerPeer", FlowID: "flow1"},
	}
	egress := []Event{
		{Protocol: "ICMP", SourceIP: "10.0.0.1", DestIP: "10.0.0.2", IcmpType: "8", IcmpCode: "0", PublicKey: "senderPeer"},
	}

	grouped := GroupByPeers(ingress, egress)

	key := [3]string{"senderPeer", "routerPeer", "ICMP"}
	assert.Contains(t, grouped, key)
	assert.Len(t, grouped[key], 1)
}

func TestGroupByPeers_ICMPMismatchedDestIPDropped(t *testing.T) {
	ingress := []Event{
		{Protocol: "ICMP", SourceIP: "10.0.0.1", DestIP: "10.0.0.2", IcmpType: "8", IcmpCode: "0", PublicKey: "routerPeer"},
	}
	egress := []Event{
		{Protocol: "ICMP", SourceIP: "10.0.0.1", DestIP: "10.0.0.3", IcmpType: "8", IcmpCode: "0", PublicKey: "senderPeer"},
	}

	grouped := GroupByPeers(ingress, egress)
	assert.Empty(t, grouped)
}

func TestGroupByPeers_ICMPMismatchedTypeDropped(t *testing.T) {
	ingress := []Event{
		{Protocol: "ICMP", SourceIP: "10.0.0.1", DestIP: "10.0.0.2", IcmpType: "8", IcmpCode: "0", PublicKey: "routerPeer"},
	}
	egress := []Event{
		{Protocol: "ICMP", SourceIP: "10.0.0.1", DestIP: "10.0.0.2", IcmpType: "0", IcmpCode: "0", PublicKey: "senderPeer"},
	}

	grouped := GroupByPeers(ingress, egress)
	assert.Empty(t, grouped)
}

func TestGroupByPeers_EmptyInputs(t *testing.T) {
	grouped := GroupByPeers(nil, nil)
	assert.Empty(t, grouped)
}

func TestGroupByPeers_ProtocolMismatchICMPIngressTCPEgressDropped(t *testing.T) {
	ingress := []Event{
		{Protocol: "ICMP", SourceIP: "10.0.0.1", DestIP: "10.0.0.2", IcmpType: "8", IcmpCode: "0", PublicKey: "routerPeer"},
	}
	egress := []Event{
		{Protocol: "TCP", ConnSourcePort: "54321", ConnDestPort: "3389", PublicKey: "senderPeer"},
	}

	grouped := GroupByPeers(ingress, egress)
	assert.Empty(t, grouped)
}
