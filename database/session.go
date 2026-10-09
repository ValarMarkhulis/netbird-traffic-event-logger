package database

import (
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SessionRecord struct {
	gorm.Model
	SenderPeer       string    `gorm:"not null;uniqueIndex:idx_session_key"`
	RoutingPeer      string    `gorm:"not null;uniqueIndex:idx_session_key"`
	DestPort         string    `gorm:"not null;uniqueIndex:idx_session_key"`
	StartTime        time.Time `gorm:"uniqueIndex:idx_session_key"`
	EndTime          *time.Time
	IsOpen           bool
	FlowIDs          []string `gorm:"column:flow_ids;serializer:json"`
	SrcPorts         []string `gorm:"column:src_ports;serializer:json"`
	SrcIPs           []string `gorm:"column:src_ips;serializer:json"`
	DestIPs          []string `gorm:"column:dest_ips;serializer:json"`
	RxPackets        int64
	TxPackets        int64
	RxBytes          int64
	TxBytes          int64
	RuleID           string
	SourceResourceID string
	DestResourceID   string
}

// ETLWatermark holds a single row tracking the last event timestamp processed
// by the incremental ETL run. ID is always 1.
type ETLWatermark struct {
	ID          uint `gorm:"primaryKey"`
	ProcessedAt time.Time
}

func (db *DB) UpsertSession(s *SessionRecord) error {
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "sender_peer"},
			{Name: "routing_peer"},
			{Name: "dest_port"},
			{Name: "start_time"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"end_time", "is_open", "flow_ids", "src_ports", "src_ips", "dest_ips",
			"rx_packets", "tx_packets", "rx_bytes", "tx_bytes",
			"rule_id", "source_resource_id", "dest_resource_id", "updated_at",
		}),
	}).Create(s).Error
}

// GetOpenSession returns the open session for a given peer group, or nil if none exists.
func (db *DB) GetOpenSession(senderPeer, routingPeer, destPort string) (*SessionRecord, error) {
	var s SessionRecord
	err := db.Where(
		"sender_peer = ? AND routing_peer = ? AND dest_port = ? AND is_open = ?",
		senderPeer, routingPeer, destPort, true,
	).First(&s).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &s, err
}

// GetWatermark returns the last successfully processed event timestamp.
// Returns a zero time if no watermark has been set yet (first run).
func (db *DB) GetWatermark() (time.Time, error) {
	var w ETLWatermark
	err := db.First(&w).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return time.Time{}, nil
	}
	return w.ProcessedAt, err
}

// SetWatermark updates the single watermark row to t.
func (db *DB) SetWatermark(t time.Time) error {
	return db.Save(&ETLWatermark{ID: 1, ProcessedAt: t}).Error
}

// CloseInactiveSessions marks open sessions as closed if they have not been
// updated within olderThan. Returns the number of rows affected.
func (db *DB) CloseInactiveSessions(olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	result := db.Model(&SessionRecord{}).
		Where("is_open = ? AND updated_at < ?", true, cutoff).
		Update("is_open", false)
	return result.RowsAffected, result.Error
}

// GetAllOpenSessions returns all session records with is_open = true.
// Used by the incremental ETL to preload all open sessions in one query
// instead of issuing one SELECT per peer group (N+1).
func (db *DB) GetAllOpenSessions() ([]SessionRecord, error) {
	var sessions []SessionRecord
	err := db.Where("is_open = ?", true).Find(&sessions).Error
	return sessions, err
}

// TryAdvisoryLock attempts to acquire a Postgres session-level advisory lock.
// Returns true if the lock was acquired, false if another session holds it.
func (db *DB) TryAdvisoryLock(key int64) (bool, error) {
	var acquired bool
	err := db.Raw("SELECT pg_try_advisory_lock(?)", key).Scan(&acquired).Error
	return acquired, err
}

// ReleaseAdvisoryLock releases a previously acquired Postgres advisory lock.
func (db *DB) ReleaseAdvisoryLock(key int64) error {
	return db.Exec("SELECT pg_advisory_unlock(?)", key).Error
}
