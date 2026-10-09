package transformer

import (
	"sort"
	"time"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
)

// IncrementalPipeline runs the ETL pipeline over a batch of new events,
// merging each group into its existing open session (loaded via getOpen) or
// starting a new one. It returns all SessionRecords that need to be upserted:
// closed sessions (IsOpen=false) and the still-open trailing session (IsOpen=true).
func IncrementalPipeline(
	newDBEvents []database.EventLog,
	getOpen func(sender, routing, dest string) *database.SessionRecord,
	threshold float64,
) []database.SessionRecord {
	if len(newDBEvents) == 0 {
		return nil
	}

	extracted := ExtractFromDB(newDBEvents)
	ingress, egress := FilterByProtocol(extracted)
	grouped := GroupByPeers(ingress, egress)

	var results []database.SessionRecord
	for key, groupEvents := range grouped {
		sort.Slice(groupEvents, func(i, j int) bool {
			return groupEvents[i].EventTime.Before(groupEvents[j].EventTime)
		})
		sessions := applyGroupIncremental(getOpen(key[0], key[1], key[2]), groupEvents, key[0], key[1], key[2], threshold)
		results = append(results, sessions...)
	}
	return results
}

// applyGroupIncremental applies a sorted slice of new events onto an existing
// open session (or nil for a brand-new session). It returns all sessions that
// result from the run: any sessions that were split off (IsOpen=false) and the
// final still-open session (IsOpen=true).
func applyGroupIncremental(
	existing *database.SessionRecord,
	events []Event,
	senderPeer, routingPeer, destPort string,
	threshold float64,
) []database.SessionRecord {
	var results []database.SessionRecord
	current := initFromExisting(existing, senderPeer, routingPeer, destPort)

	for idx, ev := range events {
		ts := ev.EventTime

		// Accumulate traffic and metadata into the current session.
		current.RxPackets += int64(ev.RxPackets)
		current.TxPackets += int64(ev.TxPackets)
		current.RxBytes += int64(ev.RxBytes)
		current.TxBytes += int64(ev.TxBytes)
		appendUnique(&current.FlowIDs, ev.FlowID)
		appendUnique(&current.SrcPorts, ev.ConnSourcePort)
		appendUnique(&current.SrcIPs, ev.SourceIP)
		appendUnique(&current.DestIPs, ev.DestIP)
		if current.RuleID == "" && ev.RuleID != "" {
			current.RuleID = ev.RuleID
		}
		if current.SourceResourceID == "" && ev.SourceResourceID != "" {
			current.SourceResourceID = ev.SourceResourceID
		}
		if current.DestResourceID == "" && ev.DestResourceID != "" {
			current.DestResourceID = ev.DestResourceID
		}

		if ev.Type == "TYPE_START" {
			if current.StartTime.IsZero() || ts.Before(current.StartTime) {
				current.StartTime = ts
			}
		}
		if ev.Type == "TYPE_END" {
			if current.EndTime == nil || ts.After(*current.EndTime) {
				current.EndTime = &ts
			}

			if exceedsThreshold(ev, current, threshold) && hasGapAfter(events, idx, ts) {
				// Close the current session and start a fresh open one.
				closed := current
				closed.IsOpen = false
				results = append(results, closed)
				current = newEmptySession(senderPeer, routingPeer, destPort)
			}
		}
	}

	// Fallback: if no TYPE_START was seen, use the earliest event timestamp.
	if current.StartTime.IsZero() && len(events) > 0 {
		current.StartTime = events[0].EventTime
	}

	current.IsOpen = true
	results = append(results, current)
	return results
}

// exceedsThreshold reports whether the TYPE_END event's traffic exceeds
// threshold × the session's cumulative traffic on any metric.
// Traffic has already been added to current before this call.
func exceedsThreshold(ev Event, current database.SessionRecord, threshold float64) bool {
	type m struct{ val, total int64 }
	for _, metric := range []m{
		{int64(ev.RxPackets), current.RxPackets},
		{int64(ev.TxPackets), current.TxPackets},
		{int64(ev.RxBytes), current.RxBytes},
		{int64(ev.TxBytes), current.TxBytes},
	} {
		if metric.total > 0 && float64(metric.val) > threshold*float64(metric.total) {
			return true
		}
	}
	return false
}

// hasGapAfter returns true if there is a TYPE_START event more than 2 seconds
// after ts in the remaining events — the same gap check as the batch pipeline.
func hasGapAfter(events []Event, afterIdx int, ts time.Time) bool {
	for j := afterIdx + 1; j < len(events); j++ {
		if events[j].Type == "TYPE_START" {
			return events[j].EventTime.Sub(ts).Seconds() > 2
		}
	}
	return false
}

func initFromExisting(s *database.SessionRecord, senderPeer, routingPeer, destPort string) database.SessionRecord {
	if s != nil {
		record := *s
		// GORM's autoUpdateTime convention only populates UpdatedAt when it's
		// the zero value. Copying the existing row verbatim carries its
		// UpdatedAt from whenever the session was first opened, which would
		// silently freeze it forever across every subsequent merge (EndTime
		// and the traffic counters would keep advancing, UpdatedAt wouldn't).
		// Zero it out so UpsertSession's Create() refreshes it, same as any
		// other genuinely-modified row. CreatedAt is intentionally left
		// alone — it should keep reflecting when the session first opened.
		record.UpdatedAt = time.Time{}
		return record
	}
	return newEmptySession(senderPeer, routingPeer, destPort)
}

func newEmptySession(senderPeer, routingPeer, destPort string) database.SessionRecord {
	return database.SessionRecord{
		SenderPeer:  senderPeer,
		RoutingPeer: routingPeer,
		DestPort:    destPort,
		IsOpen:      true,
	}
}

func appendUnique(slice *[]string, val string) {
	if val == "" {
		return
	}
	for _, v := range *slice {
		if v == val {
			return
		}
	}
	*slice = append(*slice, val)
}
