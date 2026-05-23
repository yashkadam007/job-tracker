package jobclient

import (
	"regexp"
	"sort"
	"strings"
	"testing"

	"job-tracker/internal/db"
	"job-tracker/internal/events"
)

// TestAllowedValuesMatchSchema parses every CHECK (... IN (...))
// constraint in schema.sql and asserts the matching in-process slice
// in internal/events/allowed_values.go contains the same values.
//
// This is the drift detector promised by ADR 0005: schema and Go
// stay in lockstep, or this test fails on the first build after the
// schema is edited without updating the slice.
func TestAllowedValuesMatchSchema(t *testing.T) {
	schema := db.SchemaSQL

	// CHECK (col IN ('a','b',…)) and the IS-NULL-OR variant both match
	// this pattern — we only care about the column name and the value
	// list, not the surrounding clause.
	re := regexp.MustCompile(`(?i)(\w+)\s+IN\s*\(([^)]+)\)`)
	matches := re.FindAllStringSubmatch(schema, -1)
	if len(matches) == 0 {
		t.Fatalf("no CHECK IN(...) constraints found in schema.sql")
	}

	// First match wins per column. Nullable columns produce two
	// references (column name appears twice in "col IS NULL OR col
	// IN (...)") but the regex anchors on the IN-clause, so we still
	// land on the values list.
	schemaSets := map[string][]string{}
	for _, m := range matches {
		col := strings.ToLower(m[1])
		if _, exists := schemaSets[col]; exists {
			continue
		}
		schemaSets[col] = parseSQLStringList(m[2])
	}

	checks := []struct {
		col    string
		inProc []string
	}{
		{"status", asStrings(events.AllowedStatuses)},
		{"work_mode", asStrings(events.AllowedWorkModes)},
		{"seniority", asStrings(events.AllowedSeniorities)},
		{"source", asStrings(events.AllowedSources)},
		{"round", asStrings(events.AllowedInterviewRounds)},
	}

	for _, c := range checks {
		got, ok := schemaSets[c.col]
		if !ok {
			t.Errorf("schema.sql has no CHECK IN(...) for column %q", c.col)
			continue
		}
		if !equalSets(got, c.inProc) {
			t.Errorf("drift on %s:\n  schema:  %v\n  events:  %v",
				c.col, sortedCopy(got), sortedCopy(c.inProc))
		}
	}
}

// parseSQLStringList splits an `'a','b','c'` payload into its raw
// values. SQL allows whitespace and inline comments inside the
// parenthesis; we keep it simple — schema.sql doesn't use either.
func parseSQLStringList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "'")
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func equalSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := sortedCopy(a)
	bb := sortedCopy(b)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func sortedCopy(xs []string) []string {
	out := append([]string(nil), xs...)
	sort.Strings(out)
	return out
}

// Tiny per-type adapters keep the call site flat without pulling in
// generics or reflect.
func asStrings[T ~string](xs []T) []string {
	out := make([]string, len(xs))
	for i, x := range xs {
		out[i] = string(x)
	}
	return out
}
