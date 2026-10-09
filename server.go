package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	proto "github.com/netbirdio/netbird/flow/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	eventChannelBuffer = 10_000
	eventWorkers       = 4
	batchSize          = 100
	dbWriteTimeout     = 5 * time.Second
)

var (
	batchFlushInterval    = 500 * time.Millisecond
	retryFlushInterval    = 5 * time.Second
	retryBufferMaxBatches = 50 // 50 × batchSize = up to 5 000 events buffered
)

func (s *FlowServer) Events(stream proto.FlowService_EventsServer) error {
	slog.Debug("Client connected to Events stream")
	md, ok := metadata.FromIncomingContext(stream.Context())
	if ok && len(md.Get("x-forwarded-for")) > 0 {
		slog.Debug("headers", "x-forwarded-for", md.Get("x-forwarded-for"))
	}

	err := stream.Send(&proto.FlowEventAck{IsInitiator: true})
	if err != nil {
		slog.Error("Error sending initial ack", "err", err)
		return err
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	var peerPublicKey string
	peerIdentified := false

	for {
		event, err := stream.Recv()
		if err != nil {
			if grpcErr, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
				code := grpcErr.GRPCStatus().Code()
				if code == codes.Canceled || code == codes.DeadlineExceeded {
					slog.Warn("Stream closed or timed out, stopping event processing!", "public_key", peerPublicKey)
					return nil
				}
			} else {
				slog.Error("Error receiving event", "err", err)
				return err
			}
		}

		if !event.IsInitiator {
			if !peerIdentified && len(event.PublicKey) > 0 {
				peerPublicKey = base64.StdEncoding.EncodeToString(event.PublicKey)
				peerIdentified = true
				slog.Info("Identified peer for stream", "public_key", peerPublicKey)
			}
			slog.Debug("Received event from non-initiator", "event", event)

			select {
			// Send the event to the events channel for processing
			case s.events <- event:
				ack := &proto.FlowEventAck{
					EventId: event.EventId,
				}
				sendAck(stream, ack)
			case <-ctx.Done():
				slog.Warn("Context canceled, stopping event processing")
				return nil
			}
		} else {
			slog.Debug("Received initiator handshake")
			ack := &proto.FlowEventAck{IsInitiator: true}
			sendAck(stream, ack)
		}
	}
}

func sendAck(stream proto.FlowService_EventsServer, ack *proto.FlowEventAck) {
	if err := stream.Send(ack); err != nil {
		slog.Warn("Error sending ack", "err", err)
	}
}

type FlowServer struct {
	proto.UnimplementedFlowServiceServer
	events  chan *proto.FlowEvent
	grpcSrv *grpc.Server
	addr    string
}

func main() {
	// Load .env file if present
	_ = godotenv.Load()

	debug := flag.Bool("debug", false, "Enable debug-level logging (default: info)")
	port := flag.Int("port", 9000, "gRPC server listen port")
	flag.Parse()

	// Setup structured logging
	var logHandlerOpt slog.HandlerOptions
	logHandlerOpt.Level = slog.LevelInfo
	if *debug {
		logHandlerOpt.Level = slog.LevelDebug
	}
	logHandlerOpt.AddSource = true
	logHandlerOpt.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == slog.SourceKey {
			s := a.Value.Any().(*slog.Source)

			s.File = fmt.Sprintf("%s:%d", path.Base(s.File), s.Line)
			s.Function = path.Ext(s.Function)[1:] // Extract function name after the last dot
			s.Line = 0                            // Hide line number
		}
		return a
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &logHandlerOpt))
	slog.SetDefault(logger)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		slog.Error("failed to listen", "err", err)
		return
	}

	s := &FlowServer{
		events: make(chan *proto.FlowEvent, eventChannelBuffer),
		grpcSrv: grpc.NewServer(
			grpc.StreamInterceptor(flowAuthStreamInterceptor),
			grpc.MaxConcurrentStreams(100),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             10 * time.Second, // close connections that ping more frequently
				PermitWithoutStream: false,            // close connections that send keepalives with no active stream
			}),
		),
		addr: lis.Addr().String(),
	}

	proto.RegisterFlowServiceServer(s.grpcSrv, s)

	healthSrv := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s.grpcSrv, healthSrv)
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	setHealth := func(healthy bool) {
		if healthy {
			healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
		} else {
			healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		}
	}

	var workersDone sync.WaitGroup
	workersDone.Add(1)
	go func() {
		defer workersDone.Done()
		StartEventLogging(s.events, eventWorkers, setHealth, s.grpcSrv.GracefulStop)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("Received signal, shutting down gracefully", "signal", sig)
		s.grpcSrv.GracefulStop()
	}()

	if err := s.grpcSrv.Serve(lis); err != nil {
		slog.Error("failed to serve", "err", err)
	}

	slog.Info("gRPC server stopped, draining event queue")
	close(s.events)
	workersDone.Wait()
	slog.Info("Server stopped")

}

// flowAuthStreamInterceptor rejects streaming RPCs whose Bearer token fails HMAC-SHA256 verification.
// If NB_FLOW_SECRET is not set verification is skipped (development / no-auth mode).
func flowAuthStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}

	authValues := md.Get("authorization")
	if len(authValues) == 0 {
		return status.Error(codes.Unauthenticated, "missing authorization header")
	}

	authValue := authValues[0]
	if !strings.HasPrefix(authValue, "Bearer ") {
		return status.Error(codes.Unauthenticated, "invalid authorization header format")
	}

	token := strings.TrimPrefix(authValue, "Bearer ")
	// token format: "<signature>.<payload>"  (see flow/client/auth.go)
	dotIdx := strings.Index(token, ".")
	if dotIdx < 0 {
		return status.Error(codes.Unauthenticated, "invalid token format")
	}
	sig := token[:dotIdx]
	payload := token[dotIdx+1:]

	if err := verifyFlowToken(payload, sig); err != nil {
		slog.Warn("Flow token verification failed", "err", err)
		return status.Error(codes.Unauthenticated, "invalid token")
	}

	return handler(srv, ss)
}

// verifyFlowToken checks the HMAC-SHA256 signature and expiry of a flow token.
// Key derivation matches the management-integrations generator: key = sha256(NB_FLOW_SECRET).
func verifyFlowToken(payload, signature string) error {
	secret := os.Getenv("NB_FLOW_SECRET")
	if secret == "" {
		slog.Warn("NB_FLOW_SECRET is not set — skipping flow token verification")
		return nil
	}

	hashedSecret := sha256.Sum256([]byte(secret))
	h := hmac.New(sha256.New, hashedSecret[:])
	h.Write([]byte(payload))
	expectedSig := base64.StdEncoding.EncodeToString(h.Sum(nil))

	if !hmac.Equal([]byte(expectedSig), []byte(signature)) {
		return errors.New("signature mismatch")
	}

	expiry, err := strconv.ParseInt(payload, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}
	if time.Now().Unix() > expiry {
		return errors.New("token expired")
	}

	return nil
}

// StartEventLogging connects to the database, starts the worker pool, and
// blocks until all workers have drained. stop is called when DB initialisation
// fails so the gRPC server shuts down rather than silently dropping events.
func StartEventLogging(events chan *proto.FlowEvent, numWorkers int, setHealth func(bool), stop func()) {
	db, err := database.DBInit()
	if err != nil {
		slog.Error("Failed to connect to database", "err", err)
		setHealth(false)
		stop()
		return
	}

	cb := NewCircuitBreaker(5, 30*time.Second, setHealth)

	slog.Info("StartEventLogging: starting workers", "count", numWorkers)
	runBatchWorkers(events, numWorkers, db, cb)
	slog.Info("StartEventLogging: all workers stopped")
}

func runBatchWorkers(events chan *proto.FlowEvent, numWorkers int, db database.Database, cb *CircuitBreaker) {
	retry := newRetryBuffer(retryBufferMaxBatches)

	ctx, cancel := context.WithCancel(context.Background())

	// Background goroutine: when the circuit recovers, flush the retry buffer.
	var retryWg sync.WaitGroup
	retryWg.Add(1)
	go func() {
		defer retryWg.Done()
		ticker := time.NewTicker(retryFlushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !cb.IsHealthy() || retry.size() == 0 {
					continue
				}
				batches := retry.drain()
				slog.Info("Circuit recovered, retrying buffered batches", "count", len(batches))
				for i, batch := range batches {
					retryCtx, retryCancel := context.WithTimeout(context.Background(), dbWriteTimeout)
					err := cb.Call(func() error { return db.CreateEventLogBatch(retryCtx, batch) })
					retryCancel()
					if err != nil {
						// Circuit re-opened — put unwritten batches back
						for _, remaining := range batches[i:] {
							if !retry.add(remaining) {
								slog.Warn("Retry buffer full on re-queue, dropping batch", "size", len(remaining))
							}
						}
						break
					}
					slog.Debug("Retry: logged batch to database", "size", len(batch))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(batchFlushInterval)
			defer ticker.Stop()
			batch := make([]database.EventLog, 0, batchSize)

			flush := func() {
				if len(batch) == 0 {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), dbWriteTimeout)
				defer cancel()
				if err := cb.Call(func() error { return db.CreateEventLogBatch(ctx, batch) }); err != nil {
					if err == ErrCircuitOpen {
						if retry.add(batch) {
							slog.Warn("DB circuit open, batch buffered for retry", "size", len(batch), "buffered_batches", retry.size())
						} else {
							slog.Warn("DB circuit open and retry buffer full, dropping batch", "size", len(batch))
						}
					} else {
						// Batch failed — fall back to individual inserts to isolate bad rows
						slog.Warn("Batch insert failed, falling back to individual inserts", "err", err, "size", len(batch))
						for i := range batch {
							event := &batch[i]
							rowCtx, rowCancel := context.WithTimeout(context.Background(), dbWriteTimeout)
							cbErr := cb.Call(func() error { return db.CreateEventLog(rowCtx, event) })
							rowCancel()
							if cbErr != nil {
								if cbErr == ErrCircuitOpen {
									// Buffer the remaining unwritten events
									remaining := batch[i:]
									if retry.add(remaining) {
										slog.Warn("DB circuit open during fallback, remaining events buffered", "remaining", len(remaining))
									} else {
										slog.Warn("DB circuit open during fallback, retry buffer full, dropping remaining", "remaining", len(remaining))
									}
									break
								}
								slog.Warn("Failed to log event in fallback, dropping", "err", cbErr, "event_id", event.EventId)
							}
						}
					}
				} else {
					slog.Debug("Logged batch to database", "size", len(batch))
				}
				batch = batch[:0]
			}

			for {
				select {
				case event, ok := <-events:
					if !ok {
						flush()
						return
					}
					dbEvent, err := FlowEventToDatabaseEvent(event)
					if err != nil {
						slog.Warn("Failed to convert event to database event", "err", err)
						continue
					}
					batch = append(batch, dbEvent)
					if len(batch) >= batchSize {
						flush()
					}
				case <-ticker.C:
					flush()
				}
			}
		}()
	}
	wg.Wait()

	// Stop the background retry flusher now that no new events are coming.
	cancel()
	retryWg.Wait()

	// Best-effort synchronous flush of anything still in the retry buffer on shutdown.
	if batches := retry.drain(); len(batches) > 0 {
		slog.Info("Shutdown: flushing retry buffer", "batches", len(batches))
		for _, batch := range batches {
			shutCtx, shutCancel := context.WithTimeout(context.Background(), dbWriteTimeout)
			err := db.CreateEventLogBatch(shutCtx, batch)
			shutCancel()
			if err != nil {
				slog.Warn("Shutdown: failed to flush retry batch, dropping", "err", err, "size", len(batch))
			} else {
				slog.Debug("Shutdown: flushed retry batch", "size", len(batch))
			}
		}
	}
}

func FlowEventToDatabaseEvent(event *proto.FlowEvent) (dbEvent database.EventLog, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic processing event: %v", r)
		}
	}()

	if event == nil {
		return dbEvent, fmt.Errorf("event is nil")
	}
	if event.FlowFields == nil {
		return dbEvent, fmt.Errorf("event.FlowFields is nil")
	}

	eventUUID, uuidErr := uuid.FromBytes(event.EventId)
	if uuidErr != nil {
		return dbEvent, fmt.Errorf("invalid EventId: %w", uuidErr)
	}
	FlowIDUUID, uuidErr := uuid.FromBytes(event.FlowFields.FlowId)
	if uuidErr != nil {
		return dbEvent, fmt.Errorf("invalid FlowId: %w", uuidErr)
	}

	// Parse the wireguard public key from the event
	publicKey := base64.StdEncoding.EncodeToString(event.PublicKey)

	// Parse the flow type from the event
	var flowType proto.Type
	switch event.FlowFields.Type {
	case proto.Type_TYPE_START:
		flowType = proto.Type_TYPE_START
	case proto.Type_TYPE_END:
		flowType = proto.Type_TYPE_END
	case proto.Type_TYPE_DROP:
		flowType = proto.Type_TYPE_DROP
	case proto.Type_TYPE_UNKNOWN:
		flowType = proto.Type_TYPE_UNKNOWN
	default:
		slog.Warn("Invalid Type value", "value", event.FlowFields.Type)
		flowType = proto.Type_TYPE_UNKNOWN // Default to UNKNOWN if the value is invalid
	}

	// Parse the Direction from the event
	var direction database.Direction
	switch event.FlowFields.Direction {
	case proto.Direction_INGRESS:
		direction = database.Ingress
	case proto.Direction_EGRESS:
		direction = database.Egress
	default:
		slog.Warn("Invalid Direction value", "value", event.FlowFields.Direction)
		direction = database.DirectionUnknown // Default to Unknown if the value is invalid
	}

	// Parse the protocol from the event
	var protocol database.Protocol
	switch event.FlowFields.Protocol {
	case uint32(database.ICMP):
		protocol = database.ICMP
	case uint32(database.TCP):
		protocol = database.TCP
	case uint32(database.UDP):
		protocol = database.UDP
	case uint32(database.ICMPv6):
		protocol = database.ICMPv6
	case uint32(database.SCTP):
		protocol = database.SCTP
	default:
		slog.Warn("Invalid Protocol value", "value", event.FlowFields.Protocol)
		// protocol = database.ProtocolUnknown // Don't set a default value if the protocol is unknown, so we don't loose data
	}

	// Parse RuleID from the event
	ruleID := string(event.FlowFields.RuleId)

	// Parse SourceResourceID and DestResourceID from the event
	var DestResID string
	if event.FlowFields.DestResourceId != nil {
		DestResID = string(event.FlowFields.DestResourceId)
	} else {
		DestResID = ""
	}

	var SrcResID string
	if event.FlowFields.SourceResourceId != nil {
		SrcResID = string(event.FlowFields.SourceResourceId)
	} else {
		SrcResID = ""
	}

	// Overwrite the String() method for pretty printing
	slog.Debug("Received event",
		"EventId", eventUUID.String(),
		"FlowID", FlowIDUUID.String(),
		"PublicKey", publicKey,
		"RuleId",
		ruleID,
		"SourceResourceId", SrcResID,
		"DestResourceId", DestResID,
		"FlowType",
		flowType.String(),
		"Direction", direction.String(),
		"Protocol", protocol.String(),
		"SRC_IP", net.IP(event.FlowFields.SourceIp).String(),
		"SRC_PORT", event.FlowFields.GetPortInfo().GetSourcePort(),
		"DST_IP", net.IP(event.FlowFields.DestIp).String(), "DST_PORT",
		event.FlowFields.GetPortInfo().GetDestPort(), "RX_Packets",
		event.FlowFields.RxPackets, "TX_Packets", event.FlowFields.TxPackets, "RX_Bytes",
		event.FlowFields.RxBytes, "TX_Bytes", event.FlowFields.TxBytes)

	if ts := event.GetTimestamp(); ts == nil || !ts.IsValid() {
		return dbEvent, fmt.Errorf("event %s has no valid timestamp", eventUUID)
	}
	eventTime := event.GetTimestamp().AsTime()

	dbEvent = database.EventLog{
		EventTime: eventTime,
		EventId:   eventUUID.String(),
		PublicKey: publicKey,
		FlowFields: &database.FlowFields{
			FlowId:           FlowIDUUID.String(),
			Type:             flowType.String(),
			RuleID:           ruleID,
			SourceResourceID: SrcResID,
			DestResourceID:   DestResID,
			Direction:        direction.String(),
			Protocol:         protocol.String(),
			SourceIp:         net.IP(event.FlowFields.SourceIp).String(),
			DestIp:           net.IP(event.FlowFields.DestIp).String(),
			RxPackets:        event.FlowFields.RxPackets,
			TxPackets:        event.FlowFields.TxPackets,
			RxBytes:          event.FlowFields.RxBytes,
			TxBytes:          event.FlowFields.TxBytes,
		},
	}

	var connectionInfo *database.PortInfo
	var connectionInfoIcmp *database.IcmpInfo

	if event.FlowFields.Protocol == uint32(database.ICMP) || event.FlowFields.Protocol == uint32(database.ICMPv6) {
		// Check if IcmpInfo is present in the event before trying to access it
		if event.FlowFields.GetIcmpInfo() == nil {
			slog.Warn("ICMP protocol specified but IcmpInfo is missing in the event", "EventId", eventUUID.String())
			return dbEvent, nil
		}
		connectionInfoIcmp = &database.IcmpInfo{
			IcmpType: uint32(event.FlowFields.GetIcmpInfo().IcmpType),
			IcmpCode: uint32(event.FlowFields.GetIcmpInfo().IcmpCode),
		}
		dbEvent.FlowFields.ConnectionInfoIcmp = connectionInfoIcmp
	} else {
		connectionInfo = &database.PortInfo{
			SourcePort: event.FlowFields.GetPortInfo().GetSourcePort(),
			DestPort:   event.FlowFields.GetPortInfo().GetDestPort(),
		}
		dbEvent.FlowFields.ConnectionInfo = connectionInfo
	}
	return dbEvent, nil
}
