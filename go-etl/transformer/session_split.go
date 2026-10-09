package transformer

import (
	"sort"
	"time"

	"github.com/ValarMarkhulis/traffic-event-logger/go-etl/data"
)

func SplitSessions(grouped map[[3]string][]Event, threshold float64) []data.Session {
	// Collect per-group sessions in a map so we can sort groups afterward
	type groupResult struct {
		key      [3]string
		sessions []data.Session
	}
	var groups []groupResult

	for key, groupEvents := range grouped {
		sort.Slice(groupEvents, func(i, j int) bool {
			return groupEvents[i].EventTime.Before(groupEvents[j].EventTime)
		})
		var groupSessions []data.Session
		var current *data.Session
		for idx, ev := range groupEvents {
			ts := ev.EventTime
			if current == nil {
				current = &data.Session{
					FlowIDs:       make(map[string]struct{}),
					SrcPorts:      make(map[string]struct{}),
					SrcIPs:        make(map[string]struct{}),
					DestIPs:       make(map[string]struct{}),
					TypeEndEvents: []data.TypeEndEvent{},
					SenderPeer:    key[0],
					RoutingPeer:   key[1],
					DestPort:      key[2],
				}
			}
			current.FlowIDs[ev.FlowID] = struct{}{}
			current.SrcPorts[ev.ConnSourcePort] = struct{}{}
			current.SrcIPs[ev.SourceIP] = struct{}{}
			current.DestIPs[ev.DestIP] = struct{}{}
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
				if current.Start == nil || ts.Before(*current.Start) {
					current.Start = &ts
				}
			}
			if ev.Type == "TYPE_END" {
				if current.End == nil || ts.After(*current.End) {
					current.End = &ts
				}
				current.TypeEndEvents = append(current.TypeEndEvents, data.TypeEndEvent{
					RxPackets: ev.RxPackets,
					TxPackets: ev.TxPackets,
					RxBytes:   ev.RxBytes,
					TxBytes:   ev.TxBytes,
					Timestamp: ts,
				})
				current.LastEndTs = &ts
			}
			current.RxPackets += ev.RxPackets
			current.TxPackets += ev.TxPackets
			current.RxBytes += ev.RxBytes
			current.TxBytes += ev.TxBytes
			// Split logic
			split := false
			if ev.Type == "TYPE_END" {
				totalRxPackets := current.RxPackets
				totalTxPackets := current.TxPackets
				totalRxBytes := current.RxBytes
				totalTxBytes := current.TxBytes
				sumEndRxPackets := 0
				sumEndTxPackets := 0
				sumEndRxBytes := 0
				sumEndTxBytes := 0
				for _, e := range current.TypeEndEvents {
					sumEndRxPackets += e.RxPackets
					sumEndTxPackets += e.TxPackets
					sumEndRxBytes += e.RxBytes
					sumEndTxBytes += e.TxBytes
				}
				thisEnd := current.TypeEndEvents[len(current.TypeEndEvents)-1]
				metrics := []struct {
					val    int
					total  int
					sumEnd int
				}{
					{thisEnd.RxPackets, totalRxPackets, sumEndRxPackets},
					{thisEnd.TxPackets, totalTxPackets, sumEndTxPackets},
					{thisEnd.RxBytes, totalRxBytes, sumEndRxBytes},
					{thisEnd.TxBytes, totalTxBytes, sumEndTxBytes},
				}
				for _, m := range metrics {
					if (m.total > 0 && float64(m.val) > threshold*float64(m.total)) ||
						(m.sumEnd > 0 && float64(m.val) > threshold*float64(m.sumEnd)) {
						var nextStartTs *time.Time
						for j := idx + 1; j < len(groupEvents); j++ {
							if groupEvents[j].Type == "TYPE_START" {
								nextStartTs = &groupEvents[j].EventTime
								break
							}
						}
						timeGap := 0.0
						if nextStartTs != nil {
							timeGap = nextStartTs.Sub(ts).Seconds()
						}
						if nextStartTs != nil && timeGap > 2 {
							split = true
						}
						break
					}
				}
			}
			if split {
				groupSessions = append(groupSessions, *current)
				current = nil
			}
		}
		if current != nil && len(current.FlowIDs) > 0 {
			// Ensure Start/End are set to min/max CreatedAt if not set
			minTs := time.Time{}
			maxTs := time.Time{}
			for _, ev := range groupEvents {
				if minTs.IsZero() || ev.EventTime.Before(minTs) {
					minTs = ev.EventTime
				}
				if maxTs.IsZero() || ev.EventTime.After(maxTs) {
					maxTs = ev.EventTime
				}
			}
			if current.Start == nil || current.Start.IsZero() {
				current.Start = &minTs
			}
			groupSessions = append(groupSessions, *current)
		}
		// Merge adjacent sessions
		merged := mergeSessions(groupSessions)
		// After merging, ensure Start/End are set for each session
		for i := range merged {
			sess := &merged[i]
			minTs := time.Time{}
			maxTs := time.Time{}
			for _, ev := range groupEvents {
				if minTs.IsZero() || ev.EventTime.Before(minTs) {
					minTs = ev.EventTime
				}
				if maxTs.IsZero() || ev.EventTime.After(maxTs) {
					maxTs = ev.EventTime
				}
			}
			if sess.Start == nil || sess.Start.IsZero() {
				sess.Start = &minTs
			}
		}
		groups = append(groups, groupResult{key: key, sessions: merged})
	}

	// Sort groups by their earliest session start time for deterministic output
	sort.Slice(groups, func(i, j int) bool {
		si := groups[i].sessions
		sj := groups[j].sessions
		if len(si) == 0 {
			return false
		}
		if len(sj) == 0 {
			return true
		}
		ti := si[0].Start
		tj := sj[0].Start
		if ti == nil {
			return false
		}
		if tj == nil {
			return true
		}
		return ti.Before(*tj)
	})

	var sessions []data.Session
	for _, g := range groups {
		sessions = append(sessions, g.sessions...)
	}
	return sessions
}

func mergeSessions(groupSessions []data.Session) []data.Session {
	var merged []data.Session
	for i := 0; i < len(groupSessions); {
		sess := groupSessions[i]
		if sess.End == nil && sess.RxPackets == 0 && sess.TxPackets == 0 && sess.RxBytes == 0 && sess.TxBytes == 0 {
			i++
			continue
		}
		j := i + 1
		for j < len(groupSessions) {
			nextSess := groupSessions[j]
			sharedPort := false
			for p := range sess.SrcPorts {
				if _, ok := nextSess.SrcPorts[p]; ok {
					sharedPort = true
					break
				}
			}
			sharedFlow := false
			for f := range sess.FlowIDs {
				if _, ok := nextSess.FlowIDs[f]; ok {
					sharedFlow = true
					break
				}
			}
			if sharedPort || sharedFlow {
				for f := range nextSess.FlowIDs {
					sess.FlowIDs[f] = struct{}{}
				}
				for p := range nextSess.SrcPorts {
					sess.SrcPorts[p] = struct{}{}
				}
				for ip := range nextSess.SrcIPs {
					sess.SrcIPs[ip] = struct{}{}
				}
				for ip := range nextSess.DestIPs {
					sess.DestIPs[ip] = struct{}{}
				}
				sess.RxPackets += nextSess.RxPackets
				sess.TxPackets += nextSess.TxPackets
				sess.RxBytes += nextSess.RxBytes
				sess.TxBytes += nextSess.TxBytes
				if sess.Start == nil || (nextSess.Start != nil && nextSess.Start.Before(*sess.Start)) {
					sess.Start = nextSess.Start
				}
				if sess.End == nil || (nextSess.End != nil && nextSess.End.After(*sess.End)) {
					sess.End = nextSess.End
				}
				if sess.RuleID == "" && nextSess.RuleID != "" {
					sess.RuleID = nextSess.RuleID
				}
				if sess.SourceResourceID == "" && nextSess.SourceResourceID != "" {
					sess.SourceResourceID = nextSess.SourceResourceID
				}
				if sess.DestResourceID == "" && nextSess.DestResourceID != "" {
					sess.DestResourceID = nextSess.DestResourceID
				}
				groupSessions = append(groupSessions[:j], groupSessions[j+1:]...)
			} else {
				j++
			}
		}
		merged = append(merged, sess)
		i++
	}
	return merged
}
