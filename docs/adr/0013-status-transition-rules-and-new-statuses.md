# ADR 0013 — Status transition rules, plus `assessment` and `declined` statuses

## Issue

`jobs.status` is one of six values today: `saved`, `applied`,
`interview`, `rejected`, `offer`, `withdrawn` (ADR 0001 schema; ADR
0005 allow-list mirror). Any of the six can be set from any other:
the schema's `CHECK (status IN (...))` and the producer-side
`validateStatusChanged` (`internal/jobclient/validate.go:68`) both
check membership in the allowed set and nothing else. There is no
"you can't go from `rejected` back to `saved`" rule, no "you can't
skip `applied` and land directly on `offer`" rule, and the
`job_status_history` table happily records whatever sequence the
operator presses keys for.

Two distinct gaps surface from real use:

1. **A missing state between `applied` and `interview`.** Companies
   increasingly hand out a take-home assignment / coding assessment
   after the resume screen but before any synchronous interview.
   Today the operator either parks these rows in `applied` (and
   loses "did I submit the assessment yet?" as a queryable fact)
   or promotes them to `interview` (which is a lie — there is no
   interview scheduled, only an assessment to deliver). Neither
   reflects reality, and both distort the analytics ADR 0001
   §`job_status_history` was built for ("median time from applied
   to first interview" picks up assessment work as interview time).

2. **`rejected` conflates two distinct terminal outcomes.** Today
   it means "the company rejected the candidate" *and* "the
   candidate rejected an offer". The two are the operator's
   opposite ends of the same conversation: the first is a signal
   to revise resume / sourcing / targeting; the second is a signal
   that the offer wasn't competitive. A funnel chart that lumps
   them together hides both.

   `withdrawn` does not solve the second case. `withdrawn` is "the
   candidate pulled out of the process *before* an offer landed"
   (lost interest, accepted elsewhere, scheduling collapsed).
   "Candidate received an offer and turned it down" is a different
   terminal node with different implications for follow-up
   ("would you reconsider at a higher band?", "stay in touch for
   next year").

The second gap also exposes the first: today the operator has *no
way* to make any of these transitions wrong, because every
transition is allowed. Adding two new statuses without rules would
make the graph denser, not clearer — the operator could go from
`rejected` to `assessment`, which is meaningless.

Status transition legality was explicitly carved out of producer-
side validation by ADR 0005 ("Anything that asks 'does this make
sense given other data?' (status transition legality, e.g.) does
not belong here"). That carve-out was correct: the Publisher is
stateless and doesn't read Postgres on the write path (ADR 0001).
But it left the door open for *someone* to enforce the rules
authoritatively. That someone is the Store consumer, which already
holds the current row in the same transaction as the UPDATE.

## Decision

Three coordinated changes — one schema, one shared transition
table, one Store consumer guard.

**1. Two new `JobStatus` values.**

```
assessment   // take-home / coding assessment outstanding or in flight
declined     // candidate declined an offer the company extended
```

Schema additions land via one `golang-migrate` pair (ADR 0009)
that updates the `jobs.status` CHECK constraint and the
`job_status_history.status` CHECK constraint to the new
eight-value allowed set. Both columns are `text NOT NULL` with a
`CHECK (status IN (...))` (ADR 0001); the migration drops and
re-adds each constraint with the wider list. No backfill — existing
rows keep their current status; no row becomes invalid.

`internal/events/events.go` gains:

```go
const (
    StatusAssessment JobStatus = "assessment"
    StatusDeclined   JobStatus = "declined"
)
```

`internal/events/allowed_values.go` adds both to
`AllowedStatuses`. The drift-detection test (ADR 0005) catches
forgetfulness on either side.

**2. New file `internal/events/transitions.go` — adjacency map.**

```go
// AllowedTransitions encodes the legal state graph. Keys are the
// "from" status; values are the set of legal "to" statuses for
// that source. A status that maps to an empty set is terminal.
// Same-status transitions are NOT listed here — they are treated
// separately as idempotent no-ops (see Notes).
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
        StatusInterview:  {}, // multi-round — see Notes
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
// return false — the allowed-set check at the Publisher / schema
// is the upstream guard.
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

// AllowedNext returns the legal "to" statuses for from, in a
// stable order (the iteration order of AllowedStatuses). UI
// callers use this to enable / disable status-change keys
// against the currently-selected row.
func AllowedNext(from JobStatus) []JobStatus {
    nexts := AllowedTransitions[from]
    out := make([]JobStatus, 0, len(nexts))
    for _, s := range AllowedStatuses {
        if _, ok := nexts[s]; ok {
            out = append(out, s)
        }
    }
    return out
}
```

The same-status case is *not* an entry in the map — it's handled
in `CanTransition`. This keeps the map readable as a state graph
("from `applied`, you can go to one of these other states") and
avoids restating "X→X" on every line.

**3. Store consumer enforces — Publisher does not.**

`internal/store/store.go:156` `ApplyStatusChanged` is the
authoritative enforcement point. It already loads the row inside
the transaction; the change is to look up the current status,
compare against the incoming one through
`events.CanTransition`, and skip the UPDATE + history insert on
an illegal transition. The event is still marked processed via
`db.ClaimEvent` (no infinite retry loop); a counter / log line
records the skip so the operator can spot drift between their
mental model and the table.

Concrete shape:

```go
var current events.JobStatus
err := tx.QueryRow(ctx,
    `SELECT status FROM jobs WHERE job_id = $1`, ev.JobID,
).Scan(&current)
if errors.Is(err, pgx.ErrNoRows) {
    return true, true, wrapDBError(tx.Commit(ctx)) // missing=true, unchanged path
}
if err != nil {
    return false, false, wrapDBError(err)
}
if !events.CanTransition(current, ev.Status) {
    // Event already claimed; commit the no-op so Kafka moves on.
    return true, false, wrapDBError(tx.Commit(ctx))
    // Caller logs at WARN; see ApplyStatusChanged return-shape note.
}
if current == ev.Status {
    return true, false, wrapDBError(tx.Commit(ctx)) // idempotent no-op
}
// ... existing UPDATE + INSERT INTO job_status_history ...
```

The `ApplyStatusChanged` return shape grows one boolean —
`illegal bool` — so callers (`cmd/store/main.go:185`) can log the
skip distinctly from the existing "missing" case. The cost is
one extra return value at one call site; no public type churn.

**Publisher behaviour is unchanged.** `jobclient.Publisher.ChangeStatus`
still validates allowed-set membership only (per ADR 0005
constraint). It does *not* take a `currentStatus` parameter, does
not read Postgres, and does not consult `CanTransition`. The
transition rules are imported into the TUI (next paragraph) and
the Store consumer; the Publisher stays a thin pipe.

**TUI surfaces the rules at the keybind layer.** The status-change
keybinds (`a`, `i`, `o`, `r`, `w`, `S`, plus new `A` and `D`)
consult `events.CanTransition(m.selectedJob().Status, target)`
before publishing. An illegal keypress is a no-op with a small
status-line message ("can't go from rejected to offer"), not a
Kafka publish that the Store will silently skip. This is *UI
ergonomics*, not validation — a misbehaving frontend could still
publish an illegal change; the Store is the line that holds.

## Status

Implemented.

## Group

Schema / Domain semantics.

## Assumptions

- The operator wants `assessment` as a first-class status, not a
  tag or a note. It maps to a queryable funnel stage ("how many
  assessments outstanding", "median time-in-assessment") that the
  history table already supports for the other statuses.
- The operator distinguishes "company said no" from "I said no to
  an offer". The two terminal nodes are useful to the operator's
  retrospective work (resume vs. comp).
- Same-status transitions (`applied → applied`) are idempotent
  no-ops. The operator does not press `a` on an already-applied
  row expecting anything to change; replays through Kafka must
  not break. Both cases collapse to "current == target, do
  nothing".
- The Store consumer is the only writer to `jobs.status` / 
  `job_status_history` (ADR 0001 "no service writes to another's
  tables"). Authoritative enforcement at one place is enough.
- The state graph in §Decision §2 is correct for v1. Edge cases
  the operator may want later (re-opening a rejection as a fresh
  `saved` row, `offer → offer` for renegotiation) are deferred —
  the map is one PR away from a future tweak.
- ADR 0005's carve-out ("no transition legality at the Publisher")
  is honoured. The Publisher does not gain a `currentStatus`
  parameter; the rules live in `internal/events/` and are
  enforced at the Store.

## Constraints

- Schema change through `golang-migrate` (ADR 0009). One up/down
  pair. Up: drop and re-add the two `CHECK` constraints with the
  wider allowed list. Down: drop and re-add with the original
  six values; aborts if any row is in `assessment` or `declined`
  state (the constraint itself enforces this — no preflight
  needed).
- The Publisher signature does not change. ADR 0005 §Constraints
  explicitly excludes transition checks from producer-side
  validators; this ADR does not amend that. The
  `validateStatusChanged` body stays as it is — allowed-set check
  only.
- Field names match Postgres column names (ADR 0001 rule). The
  two new enum values are lower-case strings (`"assessment"`,
  `"declined"`) wire-side and database-side.
- The Store's transition check runs inside the existing
  transaction in `ApplyStatusChanged` — no second round-trip,
  no second connection. The `SELECT status` is one row by primary
  key.
- `processed_events` is updated for every consumed event,
  *including* events that the Store skipped as illegal. This
  avoids a retry loop on a permanently-bad message and matches
  the existing dedup pattern (ADR 0007).
- `job_status_history` is *not* written on a skipped transition.
  History reflects state changes that happened, not ones that
  were rejected.
- The scheduler's `dueForStatus` (`internal/scheduler/scheduler.go:112`)
  currently fires reminders for `saved` and `applied` only. The
  two new statuses do not implicitly become reminder-bearing
  states in this ADR; see Implications.

## Positions

Alternatives considered:

1. **Adjacency map in `internal/events/transitions.go`, Store
   enforces authoritatively, TUI consults for keybind greying,
   Publisher unchanged** (this decision).
2. **Producer-side enforcement: extend `ChangeStatus` to take
   `currentStatus` and reject illegal transitions in
   `validateStatusChanged`.** Rejected — directly contradicts
   ADR 0005's "no business logic in validators" constraint. The
   Publisher is stateless on the write path; making it
   stateful (or making the caller pass the state) leaks the
   producer abstraction. The TUI / bot / CLI would each have to
   either read Postgres before publish or pass a snapshot they
   may not have.
3. **Postgres trigger / function on `jobs` that checks
   `(OLD.status, NEW.status)` against a table-valued allowed
   set.** Rejected — splits the transition graph across Go and
   SQL, with no drift detection like the one ADR 0005 set up for
   enum lists. Triggers also fire on direct `UPDATE`s from
   ad-hoc `psql` sessions, which is a feature for some teams and
   a debugging burden for a single-operator tool. The Store
   already runs the UPDATE in a single place; the check belongs
   there.
4. **FSM library (`looplab/fsm`, `qmuntal/stateless`) instead of
   an adjacency map.** Rejected — six-states-soon-to-be-eight is
   below the threshold where a library pays for itself. The
   adjacency map is ~40 lines, zero dependencies, and produces a
   reviewable diff when the graph changes. The library's
   per-state entry / exit hooks would compete with the existing
   scheduler reactions (`HandleStatusChanged` already does
   "cancel pending reminders on status change") for the same
   responsibility, muddying which code owns the side effect.
5. **One generic "transition" check imported into the
   Publisher via a callback the caller supplies (`Publisher.Edit`
   with a `func() (JobStatus, error)` current-status fetcher).**
   Rejected — pushes Postgres reads into the producer in a way
   the typed signature can't make obvious. Two layers
   (Publisher + caller-supplied fetcher) sharing one rule is
   worse than one layer (Store) owning it outright.
6. **Make `assessment` a sub-state of `applied` (a boolean
   `applied.has_assessment` column).** Rejected — defeats the
   history table. The point of `job_status_history` is to
   produce funnel queries by status; a boolean side-car makes
   "time in assessment" a join against a column rather than a
   row, which doesn't compose with the rest of the analytics.
7. **Reuse `withdrawn` for "candidate declined offer".**
   Rejected — already covered in §Issue. `withdrawn` is
   "pulled out before an offer", `declined` is "rejected an
   offer that landed". Two terminal nodes, two analytics
   stories.
8. **Add `accepted` alongside `declined` in this ADR so `offer`
   has a positive terminal node.** Rejected for v1 — out of
   scope and surfaces a larger question (does `accepted` mean
   "signed", "started", "received written offer"?) the operator
   hasn't asked. The current implicit semantic ("offer that
   stays on the board with no further transition is the
   accepted one") is enough until the operator names a use
   case. Trivially additive later — one enum value, one row in
   the adjacency map.
9. **Same-status transitions reject as illegal.** Rejected —
   replay through Kafka would surface them as false positives
   in the skip counter, and the TUI's "press `a` on an applied
   row" becomes an annoying ergonomic blocker. Idempotent
   no-op is the right v1 default.
10. **Skip the TUI keybind-greying piece — let the Store be the
    only line.** Rejected — the operator gets a worse signal
    ("nothing happened, why?") than "key disabled / inline
    'can't go from X to Y' message". The greying piece is small
    and reuses `events.AllowedNext`; no reason to leave it out.

## Argument

- **Two real gaps, one ADR.** Adding the statuses without rules
  makes the graph denser; adding the rules without the statuses
  leaves the operator's two distinct outcomes still conflated.
  Shipping them together also means one schema migration, one
  set of allowed-list edits, and one round of TUI keybind
  updates instead of two.
- **The Store is the right enforcement point.** It already holds
  the row in a transaction; the check is a `SELECT status` away.
  The Publisher cannot enforce without becoming stateful, which
  ADR 0005 explicitly rejected. Pushing the rule to the
  consumer also means a buggy or out-of-date producer cannot
  corrupt the funnel.
- **The adjacency map is the smallest thing that works.** Six
  → eight states, one map literal, one helper. No dependency,
  no per-state types, no FSM library's lifecycle hooks
  competing with the scheduler's existing reactions.
- **TUI greying is UX, not validation.** Pressing `o` on a
  `rejected` row should fail at the keypress, not by publishing
  a doomed event. `events.AllowedNext(current)` makes this a
  three-line check at the keymap layer; the rule itself stays
  in `internal/events/`.
- **Same-status idempotency keeps replays honest.** Kafka
  replays a partition from offset N regularly enough (consumer
  restart, lag recovery) that "same status → no-op" must be a
  silent success, not a logged skip. Pulling that case out of
  the adjacency map and into the helper keeps both pieces
  readable.
- **No new infrastructure.** No DLQ, no transition-rules
  service, no Postgres trigger. The change is one migration,
  one new Go file in `internal/events/`, three modified files
  (`store.go`, `tui/tui.go`, `cmd/store/main.go`), and an
  expanded allowed-list test.

## Implications

- **Migration.** One up/down pair under
  `internal/db/migrations/<ts>_assessment_declined.up.sql`:
  ```sql
  ALTER TABLE jobs              DROP CONSTRAINT jobs_status_check;
  ALTER TABLE jobs              ADD  CONSTRAINT jobs_status_check
      CHECK (status IN ('saved','applied','assessment',
                        'interview','offer','rejected',
                        'declined','withdrawn'));
  ALTER TABLE job_status_history DROP CONSTRAINT job_status_history_status_check;
  ALTER TABLE job_status_history ADD  CONSTRAINT job_status_history_status_check
      CHECK (status IN ('saved','applied','assessment',
                        'interview','offer','rejected',
                        'declined','withdrawn'));
  ```
  Down reverts to the six-value list; will fail (correctly) if
  any row uses one of the new values.
- **`internal/events/events.go`.** Two new `JobStatus` constants
  (`StatusAssessment`, `StatusDeclined`).
- **`internal/events/allowed_values.go`.** Both new constants
  added to `AllowedStatuses`. The order in the slice determines
  the order `AllowedNext` returns; keep it stable.
- **`internal/events/transitions.go`** (new). `AllowedTransitions`
  map, `CanTransition(from, to)`, `AllowedNext(from)`. No tests
  on the helpers themselves beyond a table-driven test
  enumerating every `(from, to)` pair against the expected
  legal/illegal verdict — the table makes future graph edits a
  visible diff.
- **`internal/jobclient/validate.go`.** No change. ADR 0005's
  carve-out stands; `validateStatusChanged` continues to check
  allowed-set membership only.
- **`internal/jobclient/publisher.go`.** No change to
  `ChangeStatus`. Signature stays `(ctx, events.JobStatusChanged)
  error`.
- **`internal/store/store.go:156` `ApplyStatusChanged`.** Read
  current status inside the existing transaction; consult
  `events.CanTransition`; skip UPDATE + history insert on
  illegal; commit the claim either way. Return shape grows one
  boolean: `(applied, missing, illegal bool, err error)`.
- **`cmd/store/main.go:185`.** Match the new return value; log
  illegal transitions at WARN with the `(from, to, job_id)`
  triple so the operator can spot a buggy frontend.
- **`internal/store/errors.go`.** New `ErrIllegalTransition`
  sentinel only if the caller needs to branch on it; the bool
  return may be enough. Add lazily.
- **`internal/tui/tui.go` / `internal/tui/cmds.go`.** Two new
  keybinds — `A` for `assessment`, `D` for `declined` — on the
  list view. Each status-change keymap entry calls
  `events.CanTransition(m.selectedJob().Status, target)` first;
  on `false`, render a status-line message
  (`fmt.Sprintf("can't change %s → %s", current, target)`) and
  skip the publish. No change to the publish path itself.
- **`internal/bot/`.** The bot's status-change buttons today are
  Applied / Rejected (ADR 0003); their callback handlers need
  the same `CanTransition` gate the TUI gets. The bot does not
  expose `assessment` / `declined` buttons in v1 — the operator
  named only the two new statuses, not new bot affordances;
  surfacing them in chat is a follow-up.
- **`cmd/cli/main.go`.** The `status` subcommand accepts the two
  new values via the existing allowed-list flow (the
  `AllowedStatuses` slice). The CLI does *not* gate on
  `CanTransition` — it cannot cheaply read the current status,
  and the Store will skip an illegal publish anyway. The CLI is
  a power-user surface; an operator using it can read the
  consequences in logs.
- **`internal/scheduler/scheduler.go:112` `dueForStatus`.** Not
  changed in this ADR. `assessment` is a real candidate for a
  follow-up reminder ("did you submit the assessment?") but
  the operator has not specified the cadence. `declined` is
  terminal — no reminder. Leaving both unhandled in
  `dueForStatus` falls into the `default:` branch (no
  reminder), which is the safe v1 default. Adding an
  `AssessmentFollowup` reminder is a scheduler-only change
  later.
- **Drift-detection test** (per ADR 0005). `AllowedStatuses` is
  parsed against the `jobs.status` `CHECK` constraint; the
  existing test catches a missing-or-extra value automatically.
  No change to the test itself.
- **Consumer-side replay.** A status-change event whose new
  status is illegal from the row's current state is committed
  as processed and logged. A second copy of the same event
  hits the `processed_events` dedup (ADR 0007) and no-ops. No
  retry loop.

## Related decisions

- **ADR 0001** — Richer schema and event contracts. This ADR
  adds two values to the `status` enum and updates the same
  `CHECK` constraints. The history-table-driven analytics rules
  are reused verbatim.
- **ADR 0005** — Producer-side input validation. Explicitly
  excludes transition legality from the producer; this ADR
  honours that and pushes enforcement to the Store. The
  `AllowedStatuses` allow-list pattern is extended, not
  replaced.
- **ADR 0006** — Consumer-side error classification. Illegal
  transitions are a "permanently-bad message" class — commit
  the claim, log at WARN, move on. The pattern matches.
- **ADR 0007** — Notifier dedup pattern. `processed_events`
  carries the skipped-illegal events without change.
- **ADR 0009** — Schema migrations via `golang-migrate`. The
  status-CHECK widening lands as the next migration.
- **ADR 0011** — Editing job fields via `JobEdited`. The status
  field is *not* part of `JobEdited`; it stays on the
  `job.status.changed` topic. This ADR does not change that
  separation.
- **ADR 0012** — Push-based TUI reconciliation. The TUI keybind
  greying reads the current status from the in-memory snapshot
  the LISTEN/NOTIFY path keeps fresh; no extra read needed.

## Related requirements

- The operator can record "assessment outstanding" as a first-
  class state distinct from `applied` and `interview`.
- The operator can distinguish "company rejected me" from "I
  declined the offer" in both the live view and the history-
  table funnel.
- Illegal transitions never silently change a row. The Store is
  the line that holds; the TUI surfaces the rule before the
  publish.
- The Publisher remains stateless on the write path (ADR 0005).
  No producer-side `currentStatus` parameter.
- Replays of legal status changes remain idempotent.

## Related artifacts

- `internal/db/migrations/<ts>_assessment_declined.up.sql` (new)
- `internal/db/migrations/<ts>_assessment_declined.down.sql` (new)
- `internal/events/events.go` — `StatusAssessment`,
  `StatusDeclined` constants.
- `internal/events/allowed_values.go` — extended
  `AllowedStatuses`.
- `internal/events/transitions.go` (new) —
  `AllowedTransitions`, `CanTransition`, `AllowedNext`.
- `internal/events/transitions_test.go` (new) — table-driven
  `(from, to) → bool` enumeration.
- `internal/store/store.go` — `ApplyStatusChanged` gains the
  current-status read and the `CanTransition` guard; return
  shape gains `illegal bool`.
- `cmd/store/main.go` — log illegal skips at WARN.
- `internal/tui/tui.go`, `internal/tui/cmds.go` — `A`/`D`
  keybinds, per-keybind `CanTransition` check, status-line
  rejection message.
- `internal/bot/callbacks.go` — `CanTransition` gate on the
  existing Applied / Rejected buttons.

## Related principles

- **Schema as canonical truth.** Enum widening goes through
  `CHECK` constraints first; the Go side mirrors.
- **No business logic in events.** The transition rules live in
  `internal/events/transitions.go` as data (a map), not as
  methods on event structs. The events package stays a schema
  layer.
- **Producers don't read on the write path.** The Publisher does
  not gain a `currentStatus` parameter. The Store reads its own
  row.
- **One event one append.** Status changes stay on
  `job.status.changed`; nothing folds into `JobEdited`.
- **Authoritative enforcement at the consumer.** Frontends
  advise; the Store decides.

## Notes

- **Same-status transitions are idempotent no-ops, not
  illegal.** Replays must succeed silently; the operator
  pressing `a` on an already-applied row must not log a "you
  can't do that" warning. The `CanTransition(x, x) == true`
  shortcut handles both cases. The Store's UPDATE on a
  same-status row is also a no-op; the history-table insert is
  skipped on this path to avoid duplicate history rows for the
  same status at the same time.
- **`interview → interview` is legal.** Multi-round loops
  (phone screen → technical → behavioural) progress within the
  `interview` status, with the round detail captured by
  `JobInterviewRecorded` (ADR 0001). The transition is a
  re-emission of `interview` at a later timestamp; the
  history-table records each one, which the funnel-by-round
  analytics consume. (Note that "same-status idempotent
  no-op" above would normally skip the history insert — for
  `interview → interview`, the timestamp difference is the
  point, so the Store *does* write the history row when the
  incoming `changed_at` is strictly later than the most recent
  `job_status_history.changed_at` for the row. Other same-
  status pairs do not get this carve-out.) This is the one
  fiddly bit; the table-driven test asserts the
  same-status-with-newer-timestamp path.
- **`interview → assessment` is legal.** Some pipelines hand
  out a take-home after the first synchronous round. The
  graph allows the back-edge; the funnel analytics interpret
  it as a re-entry into assessment, which is the correct
  semantic.
- **`offer → withdrawn` vs. `offer → declined`.** Both are
  legal. `withdrawn` after an offer means "I pulled out
  before formally responding to the offer" (e.g., accepted
  elsewhere and never sent the decline email). `declined`
  means "I formally turned the offer down". The two are not
  the same operator action; the graph lets the operator pick
  the one that matches reality.
- **No `offer → rejected` edge.** A "company revoked the
  offer" path is rare enough on a personal tracker that the
  v1 graph omits it. If the operator hits it, the fallback is
  `offer → withdrawn` with a note, or a manual `psql UPDATE`
  pending a future ADR amendment.
- **Terminal states stay terminal.** `rejected`, `declined`,
  `withdrawn` have empty `AllowedTransitions` entries. The
  operator who wants to "reopen" a rejected application
  creates a new job row (new URL or the same URL with a
  versioning convention) rather than reviving the old one.
  The history table thus stays an honest record per row.
- **TUI keybinds.** `A` (capital) for `assessment` and `D`
  (capital) for `declined` to avoid collision with existing
  `a` (`applied`) and the lowercase keys reserved for the
  more common transitions. The keymap rendering on the help
  panel lists them under "less common"; the muscle memory
  for `a`/`i`/`o`/`r`/`w` is preserved.
- **Bot surface.** The Telegram bot's reminder reply keyboard
  surfaces Applied / Rejected today (ADR 0003). Adding
  Assessment / Declined buttons is a follow-up — the operator
  has not asked for them in chat, and the chat surface is
  one-tap-per-button-real-estate-constrained. The bot's
  callback handlers still gain the `CanTransition` gate so
  that the existing buttons stop misfiring on terminal rows.
- **Why split `rejected`/`declined` rather than overload with a
  reason column.** A "rejection reason" column on `jobs` would
  carry the by-whom signal as free text or a small enum, but
  it would not change the funnel shape — every chart would
  still need to split on the reason column to be useful, and
  the schema would carry a column whose only purpose is to
  re-derive what two status values would carry directly. The
  status enum is the right granularity for analytics; reasons
  belong in notes.
- **Drift between in-process map and Postgres.** Unlike
  `AllowedStatuses` (which mirrors a `CHECK` constraint), the
  `AllowedTransitions` map has *no* Postgres counterpart — the
  schema does not encode the state graph. The drift-detection
  test (ADR 0005) does not apply. The single source of truth
  for transitions is `internal/events/transitions.go`; the
  Store is the only consumer. Adding a Postgres-side encoding
  later (an `allowed_transitions` table) is a defensible
  follow-up but not needed for v1.
- **Schema CHECK is the floor, not the ceiling.** The CHECK
  constraint widens to admit `assessment` and `declined` — it
  says nothing about which transitions are legal. Direct
  `psql UPDATE` against `jobs.status` can still write any
  allowed value. This is the intended escape hatch for the
  operator-as-DBA case; the Store's transition check applies
  only to the event-driven path.
- **Open: assessment outcome semantic.** An assessment can end
  with "submitted and waiting", "submitted and rejected",
  "submitted and progressed to interview", "abandoned". The
  current graph models the first three (assessment →
  interview, assessment → rejected, assessment → withdrawn) but
  not the wait state's sub-states. If "submitted, awaiting
  result" turns out to be a queryable need (e.g., for a
  reminder cadence), it lands as a column on `jobs`
  (`assessment_submitted_at`) via a `JobEdited` field — not a
  new status.
