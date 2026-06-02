package events

import "testing"

// TestCanTransition is the table-driven enumeration of every (from,
// to) pair the v1 state graph admits (ADR 0013). A future graph edit
// shows up as a one-line diff in this table; the test names every
// pair explicitly rather than re-deriving from the map, so a
// regression that *both* the map and the test miss can't sneak in.
func TestCanTransition(t *testing.T) {
	cases := []struct {
		from, to JobStatus
		want     bool
	}{
		// from saved
		{StatusSaved, StatusSaved, true}, // same-status no-op
		{StatusSaved, StatusApplied, true},
		{StatusSaved, StatusAssessment, false},
		{StatusSaved, StatusInterview, false},
		{StatusSaved, StatusOffer, false},
		{StatusSaved, StatusRejected, false},
		{StatusSaved, StatusDeclined, false},
		{StatusSaved, StatusWithdrawn, true},

		// from applied
		{StatusApplied, StatusSaved, false},
		{StatusApplied, StatusApplied, true},
		{StatusApplied, StatusAssessment, true},
		{StatusApplied, StatusInterview, true},
		{StatusApplied, StatusOffer, false},
		{StatusApplied, StatusRejected, true},
		{StatusApplied, StatusDeclined, false},
		{StatusApplied, StatusWithdrawn, true},

		// from assessment
		{StatusAssessment, StatusSaved, false},
		{StatusAssessment, StatusApplied, false},
		{StatusAssessment, StatusAssessment, true},
		{StatusAssessment, StatusInterview, true},
		{StatusAssessment, StatusOffer, false},
		{StatusAssessment, StatusRejected, true},
		{StatusAssessment, StatusDeclined, false},
		{StatusAssessment, StatusWithdrawn, true},

		// from interview
		{StatusInterview, StatusSaved, false},
		{StatusInterview, StatusApplied, false},
		{StatusInterview, StatusAssessment, true},
		{StatusInterview, StatusInterview, true}, // multi-round
		{StatusInterview, StatusOffer, true},
		{StatusInterview, StatusRejected, true},
		{StatusInterview, StatusDeclined, false},
		{StatusInterview, StatusWithdrawn, true},

		// from offer
		{StatusOffer, StatusSaved, false},
		{StatusOffer, StatusApplied, false},
		{StatusOffer, StatusAssessment, false},
		{StatusOffer, StatusInterview, false},
		{StatusOffer, StatusOffer, true},
		{StatusOffer, StatusRejected, false}, // no offer-revocation edge in v1
		{StatusOffer, StatusDeclined, true},
		{StatusOffer, StatusWithdrawn, true},

		// terminal: rejected
		{StatusRejected, StatusSaved, false},
		{StatusRejected, StatusApplied, false},
		{StatusRejected, StatusAssessment, false},
		{StatusRejected, StatusInterview, false},
		{StatusRejected, StatusOffer, false},
		{StatusRejected, StatusRejected, true},
		{StatusRejected, StatusDeclined, false},
		{StatusRejected, StatusWithdrawn, false},

		// terminal: declined
		{StatusDeclined, StatusSaved, false},
		{StatusDeclined, StatusApplied, false},
		{StatusDeclined, StatusInterview, false},
		{StatusDeclined, StatusOffer, false},
		{StatusDeclined, StatusRejected, false},
		{StatusDeclined, StatusDeclined, true},
		{StatusDeclined, StatusWithdrawn, false},

		// terminal: withdrawn
		{StatusWithdrawn, StatusSaved, false},
		{StatusWithdrawn, StatusApplied, false},
		{StatusWithdrawn, StatusInterview, false},
		{StatusWithdrawn, StatusOffer, false},
		{StatusWithdrawn, StatusRejected, false},
		{StatusWithdrawn, StatusDeclined, false},
		{StatusWithdrawn, StatusWithdrawn, true},

		// unknown source — falls through to the !ok branch.
		{JobStatus("bogus"), StatusApplied, false},
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Errorf("CanTransition(%q, %q) = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

// TestAllowedNext spot-checks the stable-order contract that the TUI
// keybind layer relies on: AllowedNext emits in AllowedStatuses
// order and never includes the source status itself.
func TestAllowedNext(t *testing.T) {
	tests := []struct {
		from JobStatus
		want []JobStatus
	}{
		{StatusSaved, []JobStatus{StatusApplied, StatusWithdrawn}},
		{StatusApplied, []JobStatus{
			StatusAssessment, StatusInterview, StatusRejected, StatusWithdrawn,
		}},
		{StatusInterview, []JobStatus{
			StatusAssessment, StatusOffer, StatusRejected, StatusWithdrawn,
		}},
		{StatusOffer, []JobStatus{StatusDeclined, StatusWithdrawn}},
		{StatusRejected, nil},
		{StatusDeclined, nil},
		{StatusWithdrawn, nil},
	}
	for _, tc := range tests {
		got := AllowedNext(tc.from)
		if len(got) != len(tc.want) {
			t.Errorf("AllowedNext(%q) len = %d (%v), want %d (%v)",
				tc.from, len(got), got, len(tc.want), tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("AllowedNext(%q)[%d] = %q, want %q",
					tc.from, i, got[i], tc.want[i])
			}
		}
	}
}
