// Package consumeradmin provides the cross-cutting bits each consumer
// (Store, Scheduler) needs to satisfy ADR 0006: a small in-process
// skip counter, a localhost admin HTTP server that exposes it, and a
// capped-exponential-backoff retry loop for infra errors.
//
// Domain-specific error classification (sentinels, Classify) stays in
// the consumer's own package — this one only knows about class names
// as opaque strings.
package consumeradmin

import (
	"maps"
	"sort"
	"sync"
)

// SkipCounter tracks how many records the current process has skipped,
// in total and broken down by error class. In-memory only; resets on
// restart (the TUI reads the current session's count — see ADR 0006
// Notes / "Counter persistence").
type SkipCounter struct {
	mu      sync.Mutex
	total   int
	byClass map[string]int
}

func NewSkipCounter() *SkipCounter {
	return &SkipCounter{byClass: map[string]int{}}
}

// Inc records one skip under the named class.
func (c *SkipCounter) Inc(class string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.total++
	c.byClass[class]++
}

// Snapshot returns a point-in-time copy safe for serialisation.
func (c *SkipCounter) Snapshot() (total int, byClass map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.byClass))
	maps.Copy(out, c.byClass)
	return c.total, out
}

// classes returns the registered class keys in sorted order. Used by
// callers that want a deterministic JSON shape even when a class has
// zero hits.
func classes(known []string, observed map[string]int) []string {
	set := map[string]struct{}{}
	for _, k := range known {
		set[k] = struct{}{}
	}
	for k := range observed {
		set[k] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
