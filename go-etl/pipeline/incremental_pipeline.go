package pipeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
	"github.com/ValarMarkhulis/traffic-event-logger/go-etl/transformer"
)

const (
	// sessionInactivityTimeout is how long a session can remain open without
	// being updated before it is automatically closed.
	sessionInactivityTimeout = 4 * time.Hour

	// etlLockKey is the Postgres advisory lock key used to prevent concurrent
	// incremental ETL runs from double-counting traffic.
	etlLockKey = 1

	// etlPageSize is the number of event_logs rows fetched per page. Keeping
	// this bounded prevents a single ETL run from loading millions of rows into
	// memory on a first run or after a long gap.
	etlPageSize = 10_000

	// etlMaxEvents is a hard cap on the total events loaded in one run. If
	// more events are pending, the run processes up to this many and the next
	// run advances the watermark further. Prevents OOM on very large backlogs.
	etlMaxEvents = 500_000
)

// RunIncremental runs the ETL over only the events that arrived since the last
// watermark, merging them into existing open sessions. On the very first run
// (no watermark set) it processes all events, equivalent to the full-scan mode.
//
// O(new events) per run instead of O(window), which scales much better when
// the ETL is invoked frequently.
//
// retention controls how long raw event_logs rows are kept after processing.
// Rows older than retention are deleted at the end of each run. Pass 0 to
// disable pruning.
func RunIncremental(db database.Database, threshold float64, retention time.Duration) int {
	acquired, err := db.TryAdvisoryLock(etlLockKey)
	if err != nil {
		slog.Error("Incremental ETL: failed to acquire advisory lock", "err", err)
		return 0
	}
	if !acquired {
		slog.Info("Incremental ETL: another instance is running, skipping this run")
		return 0
	}
	defer func() {
		if err := db.ReleaseAdvisoryLock(etlLockKey); err != nil {
			slog.Warn("Incremental ETL: failed to release advisory lock", "err", err)
		}
	}()

	watermark, err := db.GetWatermark()
	if err != nil {
		slog.Error("Incremental ETL: failed to get watermark", "err", err)
		return 0
	}

	if watermark.IsZero() {
		slog.Info("Incremental ETL: no watermark found, processing all events (first run)")
	} else {
		slog.Info("Incremental ETL: fetching events since watermark", "watermark", watermark)
	}

	// Page through events in ID order to avoid loading millions of rows at once.
	var events []database.EventLog
	var afterID uint
	for {
		page, err := db.QueryEventsPaged(context.Background(), watermark, afterID, etlPageSize)
		if err != nil {
			slog.Error("Incremental ETL: failed to query events", "err", err)
			return 0
		}
		events = append(events, page...)
		if len(page) < etlPageSize {
			break // last page
		}
		afterID = page[len(page)-1].ID
		if len(events) >= etlMaxEvents {
			slog.Warn("Incremental ETL: reached max events cap, will continue on next run",
				"cap", etlMaxEvents)
			break
		}
	}

	if len(events) == 0 {
		slog.Info("Incremental ETL: no new events")
		return 0
	}

	// Compute the new watermark before processing so we don't miss events
	// that arrive during the run.
	newWatermark := watermark
	for _, e := range events {
		if e.CreatedAt.After(newWatermark) {
			newWatermark = e.CreatedAt
		}
	}

	// Load all open sessions in a single query to avoid N+1 per peer group.
	openSessions, err := db.GetAllOpenSessions()
	if err != nil {
		slog.Error("Incremental ETL: failed to load open sessions", "err", err)
		return 0
	}
	sessionMap := make(map[[3]string]*database.SessionRecord, len(openSessions))
	for i := range openSessions {
		s := &openSessions[i]
		sessionMap[[3]string{s.SenderPeer, s.RoutingPeer, s.DestPort}] = s
	}

	sessions := transformer.IncrementalPipeline(events, func(sender, routing, dest string) *database.SessionRecord {
		return sessionMap[[3]string{sender, routing, dest}]
	}, threshold)

	for i := range sessions {
		if err := db.UpsertSession(&sessions[i]); err != nil {
			slog.Error("Incremental ETL: failed to upsert session", "err", err,
				"sender", sessions[i].SenderPeer, "is_open", sessions[i].IsOpen)
		}
	}

	if err := db.SetWatermark(newWatermark); err != nil {
		slog.Error("Incremental ETL: failed to update watermark", "err", err)
	}

	if n, err := db.CloseInactiveSessions(sessionInactivityTimeout); err != nil {
		slog.Error("Incremental ETL: failed to close inactive sessions", "err", err)
	} else if n > 0 {
		slog.Info("Incremental ETL: closed inactive sessions", "count", n)
	}

	if retention > 0 {
		if n, err := db.PruneEventLogs(retention); err != nil {
			slog.Error("Incremental ETL: failed to prune event_logs", "err", err)
		} else if n > 0 {
			slog.Info("Incremental ETL: pruned event_logs", "deleted", n, "retention", retention)
		}
	}

	slog.Info("Incremental ETL: complete",
		"events_processed", len(events),
		"sessions_upserted", len(sessions),
		"new_watermark", newWatermark,
		"elapsed_since", time.Since(newWatermark).Round(time.Second),
	)
	return len(sessions)
}
