package transformer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTimestamp_PostgresSpaceWithPlusZero(t *testing.T) {
	ts := "2025-12-06 12:32:04.783443+00"
	result := parseTimestamp(ts)
	assert.False(t, result.IsZero())
	assert.Equal(t, 2025, result.Year())
	assert.Equal(t, 12, int(result.Month()))
	assert.Equal(t, 6, result.Day())
	assert.Equal(t, 12, result.Hour())
	assert.Equal(t, 32, result.Minute())
	assert.Equal(t, 4, result.Second())
}

func TestParseTimestamp_PostgresSpaceWithPlusZeroColon(t *testing.T) {
	ts := "2025-12-06 12:32:04.783443+00:00"
	result := parseTimestamp(ts)
	assert.False(t, result.IsZero())
	assert.Equal(t, 12, result.Hour())
}

func TestParseTimestamp_PostgresSpaceNoFractional(t *testing.T) {
	ts := "2025-12-06 12:32:04"
	result := parseTimestamp(ts)
	assert.False(t, result.IsZero())
	assert.Equal(t, 4, result.Second())
}

func TestParseTimestamp_ISO8601WithFractional(t *testing.T) {
	ts := "2025-12-06T12:32:04.783443"
	result := parseTimestamp(ts)
	assert.False(t, result.IsZero())
	assert.Equal(t, 12, result.Hour())
}

func TestParseTimestamp_ISO8601NoFractional(t *testing.T) {
	ts := "2025-12-06T12:32:04"
	result := parseTimestamp(ts)
	assert.False(t, result.IsZero())
}

func TestParseTimestamp_Invalid(t *testing.T) {
	result := parseTimestamp("not-a-timestamp")
	assert.True(t, result.IsZero())
}

func TestParseTimestamp_Empty(t *testing.T) {
	result := parseTimestamp("")
	assert.True(t, result.IsZero())
}

func TestSafeInt_ValidNumber(t *testing.T) {
	assert.Equal(t, 42, safeInt("42"))
}

func TestSafeInt_Zero(t *testing.T) {
	assert.Equal(t, 0, safeInt("0"))
}

func TestSafeInt_InvalidReturnsZero(t *testing.T) {
	assert.Equal(t, 0, safeInt("abc"))
}

func TestSafeInt_EmptyReturnsZero(t *testing.T) {
	assert.Equal(t, 0, safeInt(""))
}
