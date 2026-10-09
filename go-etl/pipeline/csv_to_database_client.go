package pipeline

import (
	"context"
	"fmt"
	"strconv"

	"gorm.io/gorm"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
	"github.com/ValarMarkhulis/traffic-event-logger/go-etl/transformer"
)

// SeedFromCSV reads a CSV file and inserts each event into the database,
// preserving the original timestamps from the CSV rather than using time.Now().
// Returns the number of events inserted.
func SeedFromCSV(csvFile string, db database.Database) int {
	events := transformer.Extract(csvFile)
	count := 0
	for _, e := range events {
		record := eventToEventLog(e)
		if err := db.CreateEventLog(context.Background(), &record); err != nil {
			fmt.Printf("WARN: failed to insert event flow_id=%s: %v\n", e.FlowID, err)
			continue
		}
		count++
	}
	fmt.Printf("Seeded %d events from %s\n", count, csvFile)
	return count
}

// eventToEventLog converts a transformer.Event (from CSV) into a database.EventLog.
// The gorm.Model.CreatedAt is pre-set to the CSV timestamp so GORM preserves it.
func eventToEventLog(e transformer.Event) database.EventLog {
	ff := &database.FlowFields{
		FlowId:           e.FlowID,
		Type:             e.Type,
		RuleID:           e.RuleID,
		SourceResourceID: e.SourceResourceID,
		DestResourceID:   e.DestResourceID,
		Direction:        e.Direction,
		Protocol:         e.Protocol,
		SourceIp:         e.SourceIP,
		DestIp:           e.DestIP,
		RxPackets:        uint64(e.RxPackets),
		TxPackets:        uint64(e.TxPackets),
		RxBytes:          uint64(e.RxBytes),
		TxBytes:          uint64(e.TxBytes),
	}

	if e.ConnSourcePort != "" || e.ConnDestPort != "" {
		ff.ConnectionInfo = &database.PortInfo{
			SourcePort: parsePort(e.ConnSourcePort),
			DestPort:   parsePort(e.ConnDestPort),
		}
	}

	if e.IcmpType != "" || e.IcmpCode != "" {
		ff.ConnectionInfoIcmp = &database.IcmpInfo{
			IcmpType: parseUint32(e.IcmpType),
			IcmpCode: parseUint32(e.IcmpCode),
		}
	}

	return database.EventLog{
		Model: gorm.Model{
			CreatedAt: e.CreatedAt,
		},
		EventId:    e.FlowID,
		PublicKey:  e.PublicKey,
		FlowFields: ff,
	}
}

func parsePort(s string) uint32 {
	return parseUint32(s)
}

func parseUint32(s string) uint32 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(v)
}
