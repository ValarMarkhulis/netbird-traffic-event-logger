package transformer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
)

// TestApplyGroupIncremental_RefreshesUpdatedAtOnMerge is a regression test for
// a bug where merging new events into an existing open session left
// UpdatedAt frozen at whatever it was on the session's very first insert,
// even though EndTime and the traffic counters correctly advanced. GORM only
// auto-populates UpdatedAt when it's the zero value, so copying the existing
// row's non-zero UpdatedAt verbatim silently suppressed every later refresh.
func TestApplyGroupIncremental_RefreshesUpdatedAtOnMerge(t *testing.T) {
	firstOpenedAt := time.Date(2026, 8, 5, 19, 27, 24, 0, time.UTC)
	existing := &database.SessionRecord{
		Model:       gorm.Model{CreatedAt: firstOpenedAt, UpdatedAt: firstOpenedAt},
		SenderPeer:  "sender",
		RoutingPeer: "router",
		DestPort:    "3389/UDP",
		StartTime:   firstOpenedAt,
		IsOpen:      true,
	}

	newEvent := []Event{
		{
			Type:           "TYPE_START",
			ConnSourcePort: "59903",
			CreatedAt:      firstOpenedAt.Add(time.Hour),
			RxBytes:        1024,
		},
	}

	results := applyGroupIncremental(existing, newEvent, "sender", "router", "3389/UDP", 0.5)

	assert.Len(t, results, 1)
	assert.True(t, results[0].UpdatedAt.IsZero(),
		"UpdatedAt should be reset to zero so UpsertSession's Create() refreshes it — "+
			"a non-zero value here means it will stay frozen at the session's first-open time forever")
	// CreatedAt should NOT be touched — it's meant to keep reflecting when
	// the session was first opened.
	assert.Equal(t, firstOpenedAt, results[0].CreatedAt)
}
