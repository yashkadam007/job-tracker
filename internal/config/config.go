// Package config centralises the env-read + dev-default logic that
// every cmd/ entry point needs to discover its Kafka brokers and
// Postgres DSN (ADR 0008).
//
// Callers pass a prefix to honour the JOB_TRACKER_* namespacing
// boundary from ADR 0004: services pass "", the desktop TUI binary
// passes "JOB_TRACKER_". The trailing underscore is the caller's
// responsibility.
package config

import (
	"os"
	"strings"
)

const (
	defaultBrokers = "localhost:9092"
	defaultDSN     = "postgres://jobtracker:jobtracker@localhost:5432/jobtracker?sslmode=disable"
)

// Brokers reads <prefix>KAFKA_BOOTSTRAP (comma-separated) and falls
// back to the host-side listener exposed by compose.yml. Splits on
// "," with no trimming, matching every existing cmd/ copy.
func Brokers(prefix string) []string {
	b := os.Getenv(prefix + "KAFKA_BOOTSTRAP")
	if b == "" {
		b = defaultBrokers
	}
	return strings.Split(b, ",")
}

// DSN reads <prefix>DATABASE_URL and falls back to the local Postgres
// connection string used by compose.yml.
func DSN(prefix string) string {
	d := os.Getenv(prefix + "DATABASE_URL")
	if d == "" {
		d = defaultDSN
	}
	return d
}
