package transformer

import (
	"strconv"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
)

// ExtractFromDB converts database EventLog records into the same Event slice
// that Extract() produces from CSV, allowing the rest of the pipeline to be
// reused unchanged.
func ExtractFromDB(dbEvents []database.EventLog) []Event {
	events := make([]Event, 0, len(dbEvents))
	for _, e := range dbEvents {
		if e.FlowFields == nil {
			continue
		}
		ff := e.FlowFields

		var connSourcePort, connDestPort string
		if ff.ConnectionInfo != nil {
			if ff.ConnectionInfo.SourcePort != 0 {
				connSourcePort = strconv.FormatUint(uint64(ff.ConnectionInfo.SourcePort), 10)
			}
			if ff.ConnectionInfo.DestPort != 0 {
				connDestPort = strconv.FormatUint(uint64(ff.ConnectionInfo.DestPort), 10)
			}
		}

		var icmpType, icmpCode string
		if ff.ConnectionInfoIcmp != nil {
			icmpType = strconv.FormatUint(uint64(ff.ConnectionInfoIcmp.IcmpType), 10)
			icmpCode = strconv.FormatUint(uint64(ff.ConnectionInfoIcmp.IcmpCode), 10)
		}

		events = append(events, Event{
			Protocol:         ff.Protocol,
			ConnDestPort:     connDestPort,
			ConnSourcePort:   connSourcePort,
			Direction:        ff.Direction,
			PublicKey:        e.PublicKey,
			FlowID:           ff.FlowId,
			SourceIP:         ff.SourceIp,
			DestIP:           ff.DestIp,
			Type:             ff.Type,
			EventTime:        e.EventTime,
			RxPackets:        int(ff.RxPackets),
			TxPackets:        int(ff.TxPackets),
			RxBytes:          int(ff.RxBytes),
			TxBytes:          int(ff.TxBytes),
			IcmpType:         icmpType,
			IcmpCode:         icmpCode,
			RuleID:           ff.RuleID,
			SourceResourceID: ff.SourceResourceID,
			DestResourceID:   ff.DestResourceID,
		})
	}
	return events
}
