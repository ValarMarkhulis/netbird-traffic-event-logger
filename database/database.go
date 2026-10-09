package database

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type DBConfig struct {
	User     string
	Password string
	Host     string
	Port     string
	DBName   string
	SSLMode  string
}

// Make sure that the password field in DBConfig struct is not logged if SSL is enabled
func (c DBConfig) String() string {
	if c.SSLMode == "require" {
		return fmt.Sprintf("User:' %v',  Password: <redacted>, Host: '%v', Port: '%v', DBName: '%v', SSLMode: '%v'", c.User, c.Host, c.Port, c.DBName, c.SSLMode)
	} else {
		return fmt.Sprintf("User: '%v', Password: '%v', Host: '%v', Port: '%v', DBName: '%v', SSLMode: '%v'", c.User, c.Password, c.Host, c.Port, c.DBName, c.SSLMode)
	}

}

// DSN returns the data source name for the database connection
func (c DBConfig) DSN() string {
	return fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=%v",
		c.User,
		c.Password,
		c.Host,
		c.Port,
		c.DBName,
		c.SSLMode)
}

// Database defines the interface for database operations
type Database interface {
	AutoMigrateDB() error
	CreateEventLog(ctx context.Context, event *EventLog) error
	CreateEventLogBatch(ctx context.Context, events []EventLog) error
	QueryEvents(ctx context.Context, since time.Time) ([]EventLog, error)
	QueryEventsPaged(ctx context.Context, since time.Time, afterID uint, limit int) ([]EventLog, error)
	UpsertSession(s *SessionRecord) error
	GetOpenSession(senderPeer, routingPeer, destPort string) (*SessionRecord, error)
	GetAllOpenSessions() ([]SessionRecord, error)
	GetWatermark() (time.Time, error)
	SetWatermark(t time.Time) error
	CloseInactiveSessions(olderThan time.Duration) (int64, error)
	TryAdvisoryLock(key int64) (bool, error)
	ReleaseAdvisoryLock(key int64) error
	PruneEventLogs(olderThan time.Duration) (int64, error)
}

// DB implements the Database interface with an embedded gorm.DB
type DB struct {
	*gorm.DB
}

// Ensure DB conforms to the Database interface
var _ Database = &DB{}

var UADB Database = nil

func connectToDatabase() (db *gorm.DB, err error) {
	dbUser := os.Getenv("POSTGRES_USER")
	dbPass := os.Getenv("POSTGRES_PASSWORD")
	dbHost := os.Getenv("DATABASE_ADDR")
	dbName := os.Getenv("POSTGRES_DB")
	dbPort := os.Getenv("POSTGRES_PORT")
	dbSSL := os.Getenv("POSTGRES_SSL")
	// set log moded based on dbLogMode
	var logMode logger.LogLevel
	switch os.Getenv("DB_LOG_MODE") {
	case "silent":
		logMode = logger.Silent
	case "error":
		logMode = logger.Error
	case "warn":
		logMode = logger.Warn
	case "info":
		logMode = logger.Info
	default:
		logMode = logger.Silent
	}

	config := DBConfig{
		User:     dbUser,
		Password: dbPass,
		Host:     dbHost,
		Port:     dbPort,
		DBName:   dbName,
		SSLMode:  dbSSL,
	}

	// Log the database configuration
	slog.Info("Database configuration", slog.String("config", config.String()))

	db, err = gorm.Open(postgres.Open(config.DSN()), &gorm.Config{Logger: logger.Default.LogMode(logMode)})
	if err != nil {
		return
	}

	sqlDB, sqlErr := db.DB()
	if sqlErr != nil {
		err = fmt.Errorf("failed to configure connection pool: %w", sqlErr)
		return
	}
	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)

	return
}

func getDatabase() (Database, error) {
	var err error
	if UADB == nil {
		var gormDB *gorm.DB
		if gormDB, err = connectToDatabase(); err != nil {
			return nil, err
		}
		UADB = &DB{gormDB}
	}
	return UADB, nil
}

func DBInit() (db Database, err error) {
	for attempts := 0; attempts < 10; attempts++ {
		db, err = getDatabase()
		if err == nil {
			break
		}
		time.Sleep(5000 * time.Millisecond)
	}

	if err != nil {
		slog.Error("Failed to connect to database", "err", err)
		return nil, err
	}

	slog.Info("Migrating DB on start")
	err = db.AutoMigrateDB()
	if err != nil {
		slog.Error("Error migrating DB", "err", err)
		os.Exit(0)
	}

	return db, nil
}

// DBConnect connects to the database without running migrations.
func DBConnect() (db Database, err error) {
	for attempts := 0; attempts < 10; attempts++ {
		db, err = getDatabase()
		if err == nil {
			break
		}
		time.Sleep(5000 * time.Millisecond)
	}

	if err != nil {
		slog.Error("Failed to connect to database", "err", err)
		return nil, err
	}

	return db, nil
}

//// Database models

type EventLog struct {
	gorm.Model
	EventId    string `gorm:"uniqueIndex"`
	PublicKey  string
	FlowFields *FlowFields `gorm:"embedded"`
}

type FlowFields struct {
	FlowId             string
	Type               string
	RuleID             string
	SourceResourceID   string
	DestResourceID     string
	Direction          string
	Protocol           string
	SourceIp           string
	DestIp             string
	ConnectionInfo     *PortInfo `gorm:"embedded;embeddedPrefix:conn_"`
	ConnectionInfoIcmp *IcmpInfo `gorm:"embedded;embeddedPrefix:icmp_"`
	RxPackets          uint64
	TxPackets          uint64
	RxBytes            uint64
	TxBytes            uint64
}

type PortInfo struct {
	SourcePort uint32
	DestPort   uint32
}

type IcmpInfo struct {
	IcmpType uint32
	IcmpCode uint32
}

// Implementation of the Database interface methods

// CreateEventLog creates a new event log in the database, ignoring duplicates by EventId.
func (db *DB) CreateEventLog(ctx context.Context, event *EventLog) error {
	return db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(event).Error
}

// CreateEventLogBatch inserts multiple event logs in a single DB round-trip,
// ignoring rows whose EventId already exists.
func (db *DB) CreateEventLogBatch(ctx context.Context, events []EventLog) error {
	return db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&events).Error
}

// QueryEvents returns all event logs created at or after since.
// Use QueryEventsPaged for incremental ETL runs to avoid loading millions of
// rows into memory in a single query.
func (db *DB) QueryEvents(ctx context.Context, since time.Time) ([]EventLog, error) {
	var events []EventLog
	err := db.WithContext(ctx).Where("created_at >= ?", since).Find(&events).Error
	return events, err
}

// QueryEventsPaged returns up to limit event logs with created_at >= since
// and id > afterID, ordered by id ascending. Pass afterID=0 for the first page.
// Callers should repeat with the last returned row's ID until fewer than limit
// rows are returned.
func (db *DB) QueryEventsPaged(ctx context.Context, since time.Time, afterID uint, limit int) ([]EventLog, error) {
	var events []EventLog
	err := db.WithContext(ctx).
		Where("created_at >= ? AND id > ?", since, afterID).
		Order("id ASC").
		Limit(limit).
		Find(&events).Error
	return events, err
}

// PruneEventLogs deletes event_logs rows with created_at older than now-olderThan.
// Call this after advancing the watermark; any event before the watermark has
// already been aggregated into session_records and is safe to discard.
func (db *DB) PruneEventLogs(olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	result := db.Unscoped().Where("created_at < ?", cutoff).Delete(&EventLog{})
	return result.RowsAffected, result.Error
}

// AutoMigrateDB

func (db DB) AutoMigrateDB() (err error) {
	slog.Info("----------------------------------")
	slog.Info(os.Getenv("DATABASE_ADDR"))
	slog.Info(os.Getenv("POSTGRES_DB"))
	slog.Info(os.Getenv("POSTGRES_PORT"))
	slog.Info("----------------------------------")
	slog.Info("Migrating DB...")

	err = db.AutoMigrate(
		&EventLog{},
		&SessionRecord{},
		&ETLWatermark{},
	)

	if err != nil {
		slog.Error("Error migrating DB", "err", err)
		return err
	}

	slog.Info("Successfully migrated DB")

	return err

}
