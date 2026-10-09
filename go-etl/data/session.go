package data

import "time"

type TypeEndEvent struct {
	RxPackets int
	TxPackets int
	RxBytes   int
	TxBytes   int
	Timestamp time.Time
}

type Session struct {
	FlowIDs          map[string]struct{}
	SrcPorts         map[string]struct{}
	SrcIPs           map[string]struct{}
	DestIPs          map[string]struct{}
	Start            *time.Time
	End              *time.Time
	RxPackets        int
	TxPackets        int
	RxBytes          int
	TxBytes          int
	TypeEndEvents    []TypeEndEvent
	LastEndTs        *time.Time
	SenderPeer       string
	RoutingPeer      string
	DestPort         string
	RuleID           string
	SourceResourceID string
	DestResourceID   string
}
