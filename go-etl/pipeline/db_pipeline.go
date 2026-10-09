package pipeline

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
	"github.com/ValarMarkhulis/traffic-event-logger/go-etl/data"
	"github.com/ValarMarkhulis/traffic-event-logger/go-etl/transformer"
)

// RunFromDBWithCount runs the ETL and returns the session count
func RunFromDBWithCount(db database.Database, threshold float64, window time.Duration) int {
	since := time.Now().Add(-window)
	slog.Info("Running ETL from database", "window", window, "since", since)

	events, err := db.QueryEvents(context.Background(), since)
	if err != nil {
		slog.Error("Failed to query events from database", "err", err)
		return 0
	}
	slog.Info("Fetched events", "count", len(events))

	sessions := transformer.ETLPipelineFromDB(events, threshold)
	slog.Info("Derived sessions", "count", len(sessions))

	transformer.Load(sessions)

	for _, sess := range sessions {
		record := sessionToRecord(sess)
		if err := db.UpsertSession(&record); err != nil {
			slog.Error("Failed to upsert session", "sender_peer", sess.SenderPeer, "err", err)
		}
	}
	slog.Info("ETL complete", "sessions_upserted", len(sessions))
	return len(sessions)
}

func RunFromDB(db database.Database, threshold float64, window time.Duration) {
	since := time.Now().Add(-window)
	slog.Info("Running ETL from database", "window", window, "since", since)

	events, err := db.QueryEvents(context.Background(), since)
	if err != nil {
		slog.Error("Failed to query events from database", "err", err)
		return
	}
	slog.Info("Fetched events", "count", len(events))

	sessions := transformer.ETLPipelineFromDB(events, threshold)
	slog.Info("Derived sessions", "count", len(sessions))

	transformer.Load(sessions)

	for _, sess := range sessions {
		record := sessionToRecord(sess)
		if err := db.UpsertSession(&record); err != nil {
			slog.Error("Failed to upsert session", "sender_peer", sess.SenderPeer, "err", err)
		}
	}
	slog.Info("ETL complete", "sessions_upserted", len(sessions))
}

func sessionToRecord(s data.Session) database.SessionRecord {
	flowIDs := make([]string, 0, len(s.FlowIDs))
	for f := range s.FlowIDs {
		flowIDs = append(flowIDs, f)
	}
	sort.Strings(flowIDs)

	srcPorts := make([]string, 0, len(s.SrcPorts))
	for p := range s.SrcPorts {
		srcPorts = append(srcPorts, p)
	}
	sort.Strings(srcPorts)

	srcIPs := make([]string, 0, len(s.SrcIPs))
	for ip := range s.SrcIPs {
		srcIPs = append(srcIPs, ip)
	}
	sort.Strings(srcIPs)

	destIPs := make([]string, 0, len(s.DestIPs))
	for ip := range s.DestIPs {
		destIPs = append(destIPs, ip)
	}
	sort.Strings(destIPs)

	var startTime time.Time
	if s.Start != nil {
		startTime = *s.Start
	}

	return database.SessionRecord{
		SenderPeer:       s.SenderPeer,
		RoutingPeer:      s.RoutingPeer,
		DestPort:         s.DestPort,
		StartTime:        startTime,
		EndTime:          s.End,
		FlowIDs:          flowIDs,
		SrcPorts:         srcPorts,
		SrcIPs:           srcIPs,
		DestIPs:          destIPs,
		RxPackets:        int64(s.RxPackets),
		TxPackets:        int64(s.TxPackets),
		RxBytes:          int64(s.RxBytes),
		TxBytes:          int64(s.TxBytes),
		RuleID:           s.RuleID,
		SourceResourceID: s.SourceResourceID,
		DestResourceID:   s.DestResourceID,
	}
}
