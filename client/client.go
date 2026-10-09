package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	flow "github.com/netbirdio/netbird/flow/client"
	proto "github.com/netbirdio/netbird/flow/proto"
)

// buildToken generates a signed Bearer token compatible with the flow server's
// flowAuthStreamInterceptor. Key derivation matches the server: key = sha256(NB_FLOW_SECRET).
// Returns payload and signature as separate strings for flow.NewClient.
func buildToken(secret string, ttl time.Duration) (payload, signature string) {
	expiry := time.Now().Add(ttl).Unix()
	payload = fmt.Sprintf("%d", expiry)
	hashedSecret := sha256.Sum256([]byte(secret))
	h := hmac.New(sha256.New, hashedSecret[:])
	h.Write([]byte(payload))
	signature = base64.StdEncoding.EncodeToString(h.Sum(nil))
	return payload, signature
}

func main() {
	// Initialize slog logger
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(h))

	// Build auth token. If NB_FLOW_SECRET is not set the server must also have
	// it unset (dev/no-auth mode); we fall back to placeholder values so the
	// client still connects.
	payload, signature := "test-payload", "test-signature"
	if secret := os.Getenv("NB_FLOW_SECRET"); secret != "" {
		payload, signature = buildToken(secret, 5*time.Minute)
		slog.Info("NB_FLOW_SECRET set, using signed token")
	} else {
		slog.Warn("NB_FLOW_SECRET not set — connecting without authentication")
	}

	addr := os.Getenv("FLOW_SERVER_ADDR")
	if addr == "" {
		addr = "tcp://127.0.0.1:9000"
	}

	client, err := flow.NewClient(addr, payload, signature, 1*time.Second)
	// To connect via TLS through Caddy:
	// client, err := flow.NewClient("https://domain.com:443", payload, signature, 1*time.Second)

	if err != nil {
		slog.Error("failed to create client", "err", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ackReceived := make(chan *proto.FlowEventAck)

	go func() {
		err := client.Receive(ctx, 15*time.Second, func(ack *proto.FlowEventAck) error {
			// Parse the ack EventId to a UUID
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

	time.Sleep(500 * time.Millisecond)

	uuidVal2 := uuid.New()
	publicKey1String := "PublicKey1"
	publicKey1Encoded := base64.StdEncoding.EncodeToString([]byte(publicKey1String)) // "PublicKey1" in base64
	publicKey1, err := base64.StdEncoding.DecodeString(publicKey1Encoded)            // []byte representation of "PublicKey1"
	if err != nil {
		slog.Error("Failed to decode public key", "err", err)
		return
	}
	ruleID := "test-rule-id"
	// Encode the rule ID to base64
	ruleIDEncoded := base64.StdEncoding.EncodeToString([]byte(ruleID)) // "test-rule-id" in base64
	// Decode the rule ID back to bytes
	ruleID1, err := base64.StdEncoding.DecodeString(ruleIDEncoded)
	if err != nil {
		slog.Error("failed to decode rule ID", "err", err)
		return
	}
	testEvent := &proto.FlowEvent{
		PublicKey: publicKey1,
		FlowFields: &proto.FlowFields{
			RuleId:           ruleID1,
			SourceResourceId: []byte("source-resource-id"),
			DestResourceId:   []byte("dest-resource-id"),
			Type:             proto.Type_TYPE_START,
			FlowId:           uuidVal2[:],
			Direction:        proto.Direction_INGRESS,
			Protocol:         6,
			SourceIp:         []byte{192, 168, 1, 1},
			DestIp:           []byte{192, 168, 1, 2},
			ConnectionInfo: &proto.FlowFields_PortInfo{
				PortInfo: &proto.PortInfo{
					SourcePort: 12345,
					DestPort:   443,
				},
			},
		},
	}
	const amountOfTestMessages = 10
	for i := range amountOfTestMessages {
		uuidVal := uuid.New()
		slog.Info("Sending event", "index", i, "total", amountOfTestMessages, "EventID", uuidVal)

		testEvent.EventId = uuidVal[:]
		err = client.Send(testEvent)
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
		time.Sleep(1 * time.Second)
	}

	slog.Info("Exiting...")
	client.Close()
}
