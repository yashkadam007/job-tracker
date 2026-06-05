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

func TestFmtRelativeWhen(t *testing.T) {
	loc := time.Local
	now := time.Date(2026, 6, 5, 12, 0, 0, 0, loc)

	tests := []struct {
		name string
		when time.Time
		want string
	}{
		{
			name: "same local date",
			when: time.Date(2026, 6, 5, 1, 0, 0, 0, loc),
			want: "today",
		},
		{
			name: "one day",
			when: now.AddDate(0, 0, -1),
			want: "1d ago",
		},
		{
			name: "four days",
			when: now.AddDate(0, 0, -4),
			want: "4d ago",
		},
		{
			name: "thirty days",
			when: now.AddDate(0, 0, -30),
			want: "30d ago",
		},
		{
			name: "thirty one days rounds to one month",
			when: now.AddDate(0, 0, -31),
			want: "1m ago",
		},
		{
			name: "forty five days rounds to two months",
			when: now.AddDate(0, 0, -45),
			want: "2m ago",
		},
		{
			name: "twelve rounded months renders as one year",
			when: now.AddDate(0, 0, -360),
			want: "1y ago",
		},
		{
			name: "future different day falls back to full datetime",
			when: now.AddDate(0, 0, 1),
			want: now.AddDate(0, 0, 1).Local().Format("2006-01-02 15:04"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fmtRelativeWhen(tt.when, now); got != tt.want {
				t.Fatalf("fmtRelativeWhen() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestApplyFilterUsesRelativeLastEventInTableRows(t *testing.T) {
	loc := time.Local
	now := time.Date(2026, 6, 5, 12, 0, 0, 0, loc)
	oldNowForLastEvent := nowForLastEvent
	nowForLastEvent = func() time.Time { return now }
	defer func() { nowForLastEvent = oldNowForLastEvent }()

	m := New(Config{})
	job := testJob("1", "Alpha Backend", events.StatusSaved)
	job.LastEventAt = now.AddDate(0, 0, -4)
	m.jobs = []jobclient.Job{job}

	m.applyFilter()

	rows := m.tbl.Rows()
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if got := rows[0][3]; got != "4d ago" {
		t.Fatalf("last event cell = %q, want %q", got, "4d ago")
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
