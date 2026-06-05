package tui

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"job-tracker/internal/events"
	"job-tracker/internal/jobclient"
)

var ansiRE = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func stripANSI(s string) string {
	return ansiRE.ReplaceAllString(s, "")
}

func TestStatsBarRendersZeroCountsInFunnelOrder(t *testing.T) {
	m := Model{
		width:       120,
		statsCounts: normalizeStats(nil),
	}

	got := stripANSI(m.statsBar())
	wantSegments := []string{
		"saved 0",
		"applied 0",
		"assessment 0",
		"interview 0",
		"offer 0",
		"rejected 0",
	}
	last := -1
	for _, segment := range wantSegments {
		idx := strings.Index(got, segment)
		if idx < 0 {
			t.Fatalf("stats bar missing %q in %q", segment, got)
		}
		if idx <= last {
			t.Fatalf("stats bar segment %q rendered out of order in %q", segment, got)
		}
		last = idx
	}
}

func TestStatsBarUsesGlobalCountsNotVisibleRows(t *testing.T) {
	m := New(Config{})
	m.width = 120
	m.jobs = []jobclient.Job{
		testJob("1", "Alpha Backend", events.StatusSaved),
		testJob("2", "Beta Backend", events.StatusApplied),
	}
	m.searchTerm = "alpha"
	m.statsCounts = normalizeStats(map[events.JobStatus]int{
		events.StatusSaved:      10,
		events.StatusApplied:    20,
		events.StatusAssessment: 30,
		events.StatusInterview:  40,
		events.StatusOffer:      50,
		events.StatusRejected:   60,
	})

	m.applyFilter()

	if len(m.view) != 1 {
		t.Fatalf("expected one visible row after search, got %d", len(m.view))
	}
	got := stripANSI(m.statsBar())
	for _, segment := range []string{"saved 10", "applied 20", "assessment 30", "interview 40", "offer 50", "rejected 60"} {
		if !strings.Contains(got, segment) {
			t.Fatalf("stats bar missing global count %q in %q", segment, got)
		}
	}
}

func TestApplyStatusUpdatesStatsOptimistically(t *testing.T) {
	m := New(Config{})
	m.jobs = []jobclient.Job{testJob("1", "Alpha Backend", events.StatusSaved)}
	m.statsCounts = normalizeStats(map[events.JobStatus]int{
		events.StatusSaved: 1,
	})
	m.applyFilter()

	nextModel, _ := m.applyStatus(events.StatusApplied)
	got := nextModel.(Model)
	if got.statsCounts[events.StatusSaved] != 0 {
		t.Fatalf("saved count = %d, want 0", got.statsCounts[events.StatusSaved])
	}
	if got.statsCounts[events.StatusApplied] != 1 {
		t.Fatalf("applied count = %d, want 1", got.statsCounts[events.StatusApplied])
	}

	sameModel, _ := got.applyStatus(events.StatusApplied)
	same := sameModel.(Model)
	if same.statsCounts[events.StatusSaved] != 0 || same.statsCounts[events.StatusApplied] != 1 {
		t.Fatalf("same-status no-op changed counts: saved=%d applied=%d",
			same.statsCounts[events.StatusSaved], same.statsCounts[events.StatusApplied])
	}
}

func TestApplyStatusWithUnloadedStatsDoesNotPanic(t *testing.T) {
	m := New(Config{})
	m.jobs = []jobclient.Job{testJob("1", "Alpha Backend", events.StatusSaved)}
	m.applyFilter()

	nextModel, _ := m.applyStatus(events.StatusApplied)
	got := nextModel.(Model)
	if got.statsCounts != nil {
		t.Fatalf("statsCounts = %#v, want nil", got.statsCounts)
	}
}

func testJob(id, title string, status events.JobStatus) jobclient.Job {
	now := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	return jobclient.Job{
		JobID:       id,
		URL:         "https://example.com/" + id,
		Title:       title,
		Company:     "Acme",
		Status:      status,
		FirstSeenAt: now,
		LastEventAt: now,
	}
}
