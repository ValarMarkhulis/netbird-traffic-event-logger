package transformer

import (
	"github.com/ValarMarkhulis/traffic-event-logger/database"
	"github.com/ValarMarkhulis/traffic-event-logger/go-etl/data"
)

func ETLPipeline(csvFile string, threshold float64) []data.Session {
	// Extraction
	events := Extract(csvFile)
	// Filtering
	ingress, egress := FilterByProtocol(events)
	// Grouping
	grouped := GroupByPeers(ingress, egress)
	// Session splitting
	sessions := SplitSessions(grouped, threshold)
	// Enrichment (optional, can be extended)
	sessions = Enrich(sessions)
	return sessions
}

func ETLPipelineFromDB(dbEvents []database.EventLog, threshold float64) []data.Session {
	// Extraction
	events := ExtractFromDB(dbEvents)
	// Filtering
	ingress, egress := FilterByProtocol(events)
	// Grouping
	grouped := GroupByPeers(ingress, egress)
	// Session splitting
	sessions := SplitSessions(grouped, threshold)
	// Enrichment (optional, can be extended)
	sessions = Enrich(sessions)
	return sessions
}
