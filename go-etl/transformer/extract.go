package transformer

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"
	"time"
)

type Event struct {
	Protocol         string
	ConnDestPort     string
	ConnSourcePort   string
	Direction        string
	PublicKey        string
	FlowID           string
	SourceIP         string
	DestIP           string
	Type             string
	EventTime        time.Time
	RxPackets        int
	TxPackets        int
	RxBytes          int
	TxBytes          int
	IcmpType         string
	IcmpCode         string
	RuleID           string
	SourceResourceID string
	DestResourceID   string
}

func safeInt(val string) int {
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0
	}
	return i
}

func parseTimestamp(ts string) time.Time {
	// Strip timezone suffixes produced by PostgreSQL (+00 or +00:00)
	if strings.HasSuffix(ts, "+00:00") {
		ts = ts[:len(ts)-6]
	} else if strings.HasSuffix(ts, "+00") {
		ts = ts[:len(ts)-3]
	}
	// Try space-separated date/time with microseconds (PostgreSQL default)
	t, err := time.Parse("2006-01-02 15:04:05.999999", ts)
	if err == nil {
		return t
	}
	// Try space-separated without fractional seconds
	t, err = time.Parse("2006-01-02 15:04:05", ts)
	if err == nil {
		return t
	}
	// Try ISO 8601 with T separator and microseconds
	t, err = time.Parse("2006-01-02T15:04:05.999999", ts)
	if err == nil {
		return t
	}
	// Try ISO 8601 without fractional seconds
	t, err = time.Parse("2006-01-02T15:04:05", ts)
	if err == nil {
		return t
	}
	return time.Time{}
}

func Extract(csvFile string) []Event {
	file, err := os.Open(csvFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		panic(err)
	}
	var events []Event
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		m := make(map[string]string)
		for i, h := range headers {
			m[h] = row[i]
		}
		eventTime := parseTimestamp(m["event_time"])
		events = append(events, Event{
			Protocol:         m["protocol"],
			ConnDestPort:     m["conn_dest_port"],
			ConnSourcePort:   m["conn_source_port"],
			Direction:        m["direction"],
			PublicKey:        m["public_key"],
			FlowID:           m["flow_id"],
			SourceIP:         m["source_ip"],
			DestIP:           m["dest_ip"],
			Type:             m["type"],
			EventTime:        eventTime,
			RxPackets:        safeInt(m["rx_packets"]),
			TxPackets:        safeInt(m["tx_packets"]),
			RxBytes:          safeInt(m["rx_bytes"]),
			TxBytes:          safeInt(m["tx_bytes"]),
			IcmpType:         m["icmp_icmp_type"],
			IcmpCode:         m["icmp_icmp_code"],
			RuleID:           m["rule_id"],
			SourceResourceID: m["source_resource_id"],
			DestResourceID:   m["dest_resource_id"],
		})
	}
	return events
}
