package main

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	flow "github.com/netbirdio/netbird/flow/client"
	proto "github.com/netbirdio/netbird/flow/proto"
)

func main() {
	if len(os.Args) < 2 {
		panic("Usage: csv_client <csv_file>")
	}
	csvFile := os.Args[1]
	file, err := os.Open(csvFile)
	if err != nil {
		panic(err)
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(h))

	client, err := flow.NewClient("tcp://localhost:9000", "test-payload", "test-signature", 1*time.Second)
	if err != nil {
		slog.Error("failed to create client", "err", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ackReceived := make(chan *proto.FlowEventAck)

	go func() {
		err := client.Receive(ctx, 30*time.Second, func(ack *proto.FlowEventAck) error {
			if len(ack.EventId) != 16 {
				return errors.New("invalid EventId length")
			}
			eventId, err := uuid.FromBytes(ack.EventId)
			if err != nil {
				return err
			}
			slog.Info("Received ack for eventId", "eventId", eventId)
			ackReceived <- ack
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("receive error", "err", err)
		}
	}()
	time.Sleep(500 * time.Millisecond) // Ensure stream is initialized before sending events

	for i, rec := range records[1:] { // skip header
		eventId, _ := uuid.Parse(rec[4])
		publicKey, _ := base64.StdEncoding.DecodeString(rec[5])
		flowId, _ := uuid.Parse(rec[6])
		typeVal := parseType(rec[7])
		ruleId := []byte(rec[8])
		sourceResourceId := []byte(rec[9])
		destResourceId := []byte(rec[10])
		direction := parseDirection(rec[11])
		protocol := parseProtocol(rec[12])
		sourceIp := parseIP(rec[13])
		destIp := parseIP(rec[14])
		sourcePort := parseInt(rec[15])
		destPort := parseInt(rec[16])
		icmpType, _ := strconv.Atoi(rec[17])
		icmpCode, _ := strconv.Atoi(rec[18])
		rxPackets := parseInt(rec[19])
		txPackets := parseInt(rec[20])
		rxBytes := parseInt(rec[21])
		txBytes := parseInt(rec[22])

		slog.Info("Parsed event fields", "index", i,
			"EventID", eventId,
			"PublicKey", publicKey,
			"FlowID", flowId,
			"Type", typeVal,
			"RuleID", ruleId,
			"SourceResourceId", sourceResourceId,
			"DestResourceId", destResourceId,
			"Direction", direction,
			"Protocol", protocol,
			"SourceIp", sourceIp,
			"DestIp", destIp,
			"SourcePort", sourcePort,
			"DestPort", destPort,
			"RxPackets", rxPackets,
			"TxPackets", txPackets,
			"RxBytes", rxBytes,
			"TxBytes", txBytes)

		event := &proto.FlowEvent{
			EventId:   eventId[:],
			PublicKey: publicKey,
			FlowFields: &proto.FlowFields{
				RuleId:           ruleId,
				SourceResourceId: sourceResourceId,
				DestResourceId:   destResourceId,
				Type:             typeVal,
				FlowId:           flowId[:],
				Direction:        direction,
				Protocol:         uint32(protocol),
				SourceIp:         sourceIp,
				DestIp:           destIp,
				RxPackets:        uint64(rxPackets),
				TxPackets:        uint64(txPackets),
				RxBytes:          uint64(rxBytes),
				TxBytes:          uint64(txBytes),
				ConnectionInfo: &proto.FlowFields_PortInfo{
					PortInfo: &proto.PortInfo{
						SourcePort: uint32(sourcePort),
						DestPort:   uint32(destPort),
					},
				},
			},
		}
		if protocol == 1 { // ICMP
			event.FlowFields.ConnectionInfo = &proto.FlowFields_IcmpInfo{
				IcmpInfo: &proto.ICMPInfo{
					IcmpType: uint32(icmpType),
					IcmpCode: uint32(icmpCode),
				},
			}
		}
		slog.Info("Sending event", "index", i, "EventID", eventId)
		err := client.Send(event)
		if err != nil {
			slog.Error("failed to send event", "err", err)
			break
		}
		slog.Info("Event sent, waiting for ack...")
		select {
		case <-ackReceived:
			slog.Info("Ack received")
		case <-time.After(2 * time.Second):
			slog.Warn("timeout waiting for ack to be received by flow")
		}
	}

	slog.Info("Exiting...")
	client.Close()
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func parseIP(ip string) []byte {
	// Accepts dotted decimal IPv4
	var out [4]byte
	fmt.Sscanf(ip, "%d.%d.%d.%d", &out[0], &out[1], &out[2], &out[3])
	return out[:]
}

func parseType(s string) proto.Type {
	switch s {
	case "TYPE_START":
		return proto.Type_TYPE_START
	case "TYPE_END":
		return proto.Type_TYPE_END
	case "TYPE_DROP":
		return proto.Type_TYPE_DROP
	default:
		return proto.Type_TYPE_UNKNOWN
	}
}

func parseDirection(s string) proto.Direction {
	switch s {
	case "ingress":
		return proto.Direction_INGRESS
	case "egress":
		return proto.Direction_EGRESS
	default:
		return proto.Direction_DIRECTION_UNKNOWN
	}
}

func parseProtocol(s string) int {
	switch s {
	case "TCP":
		return 6
	case "UDP":
		return 17
	case "ICMP":
		return 1
	default:
		return parseInt(s)
	}
}
