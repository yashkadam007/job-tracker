package events

// AllowedTransitions encodes the legal job-status state graph (ADR
// 0013). Keys are the "from" status; values are the set of legal
// "to" statuses for that source. A status that maps to an empty set
// is terminal.
//
// Same-status transitions are NOT listed here — they are treated
// separately as idempotent no-ops in CanTransition. The lone
// exception is `interview → interview`, which is listed because
// multi-round interview loops re-emit the status at a later
// timestamp; the Store treats it as a real history row when the
// incoming changed_at is strictly newer (see ADR 0013 §Notes).
var AllowedTransitions = map[JobStatus]map[JobStatus]struct{}{
	StatusSaved: {
		StatusApplied:   {},
		StatusWithdrawn: {},
	},
	StatusApplied: {
		StatusAssessment: {},
		StatusInterview:  {},
		StatusRejected:   {},
		StatusWithdrawn:  {},
	},
	StatusAssessment: {
		StatusInterview: {},
		StatusRejected:  {},
		StatusWithdrawn: {},
	},
	StatusInterview: {
		StatusInterview:  {}, // multi-round — see Notes in ADR 0013
		StatusAssessment: {}, // post-screen take-home — see Notes
		StatusOffer:      {},
		StatusRejected:   {},
		StatusWithdrawn:  {},
	},
	StatusOffer: {
		StatusDeclined:  {},
		StatusWithdrawn: {},
	},
	StatusRejected:  {}, // terminal — company rejected
	StatusDeclined:  {}, // terminal — candidate declined offer
	StatusWithdrawn: {}, // terminal — candidate withdrew pre-offer
}

// CanTransition reports whether (from -> to) is legal. Same-status
// transitions return true (idempotent no-op). Unknown statuses
// return false — the allowed-set check at the Publisher / schema is
// the upstream guard.
func CanTransition(from, to JobStatus) bool {
	if from == to {
		return true
	}
	nexts, ok := AllowedTransitions[from]
	if !ok {
		return false
	}
	_, allowed := nexts[to]
	return allowed
}

// AllowedNext returns the legal "to" statuses for from, in a stable
// order (the iteration order of AllowedStatuses). UI callers use
// this to enable / disable status-change keys against the
// currently-selected row. The same-status case is omitted — callers
// asking "what can I transition to next?" don't want the current
// status in the list.
func AllowedNext(from JobStatus) []JobStatus {
	nexts := AllowedTransitions[from]
	out := make([]JobStatus, 0, len(nexts))
	for _, s := range AllowedStatuses {
		if s == from {
			continue
		}
		if _, ok := nexts[s]; ok {
			out = append(out, s)
		}
	}
	return out
}
