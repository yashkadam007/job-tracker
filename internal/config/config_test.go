package config

import (
	"reflect"
	"testing"
)

func TestBrokers(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		env    map[string]string
		want   []string
	}{
		{
			name: "default when unset",
			want: []string{"localhost:9092"},
		},
		{
			name: "single host",
			env:  map[string]string{"KAFKA_BOOTSTRAP": "broker:9092"},
			want: []string{"broker:9092"},
		},
		{
			name: "comma-split",
			env:  map[string]string{"KAFKA_BOOTSTRAP": "a:9092,b:9092"},
			want: []string{"a:9092", "b:9092"},
		},
		{
			name:   "namespaced prefix reads namespaced var",
			prefix: "JOB_TRACKER_",
			env:    map[string]string{"JOB_TRACKER_KAFKA_BOOTSTRAP": "tail:9092"},
			want:   []string{"tail:9092"},
		},
		{
			name:   "namespaced prefix ignores non-namespaced var",
			prefix: "JOB_TRACKER_",
			env:    map[string]string{"KAFKA_BOOTSTRAP": "should-be-ignored:9092"},
			want:   []string{"localhost:9092"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("KAFKA_BOOTSTRAP", "")
			t.Setenv("JOB_TRACKER_KAFKA_BOOTSTRAP", "")
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			got := Brokers(c.prefix)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("Brokers(%q) = %v, want %v", c.prefix, got, c.want)
			}
		})
	}
}

func TestDSN(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		env    map[string]string
		want   string
	}{
		{
			name: "default when unset",
			want: "postgres://jobtracker:jobtracker@localhost:5432/jobtracker?sslmode=disable",
		},
		{
			name: "override",
			env:  map[string]string{"DATABASE_URL": "postgres://x/y"},
			want: "postgres://x/y",
		},
		{
			name:   "namespaced prefix reads namespaced var",
			prefix: "JOB_TRACKER_",
			env:    map[string]string{"JOB_TRACKER_DATABASE_URL": "postgres://tail/y"},
			want:   "postgres://tail/y",
		},
		{
			name:   "namespaced prefix ignores non-namespaced var",
			prefix: "JOB_TRACKER_",
			env:    map[string]string{"DATABASE_URL": "should-be-ignored"},
			want:   "postgres://jobtracker:jobtracker@localhost:5432/jobtracker?sslmode=disable",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "")
			t.Setenv("JOB_TRACKER_DATABASE_URL", "")
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			got := DSN(c.prefix)
			if got != c.want {
				t.Fatalf("DSN(%q) = %q, want %q", c.prefix, got, c.want)
			}
		})
	}
}
