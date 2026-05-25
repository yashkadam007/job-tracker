# ADR 0011 — Editing job fields via a `job.edited` event

## Issue

The TUI's keystroke vocabulary today is "set status" (`a`, `i`, `o`,
`r`, `w`, `S`), "snooze" (`s`), "new" (`n`), "search" (`/`), and
"filter" (`f`). Every other column on `jobs` is write-once at
submit time: the operator types a URL, title, and company into the
three-step modal, and from that moment on the row's `work_mode`,
`location`, `source`, `tech_tags`, `custom_tags`, `priority`,
`comp_*`, `recruiter_*`, `description`, `deadline`, and
`resume_version` / `cover_letter_version` are frozen. There is no
event, no Publisher method, no Store handler, and no UI to change
any of them after the fact.

In practice that's wrong on two axes:

1. **The submit-time modal is deliberately three fields wide** (ADR
   0010 Decision §4, "no second modal, no two-phase flow"). Posting
   metadata that's not on the page at submit time has no entry
   point at all today. The operator currently captures the URL,
   discovers later that the posting is remote / mid-level / in
   Bangalore, and has no way to record it without a manual
   `psql UPDATE`.
2. **Application progresses, and progress accrues data.** The
   recruiter introduces themselves a week after the saved-state
   capture; the operator quotes an expected compensation number in
   the first call; tags ("rust", "infra-team") emerge after reading
   the description; priority gets bumped after a positive screen.
   None of these moments correspond to a status transition, so
   `job.status.changed` is not the event that carries them.

The operator has named six fields they reach for most often:
`work_mode`, `location`, `source`, tags (both `tech_tags` and
`custom_tags`), `priority`, and free-text notes. They have also
flagged one missing field — the **expected compensation the
operator has quoted to the company** — which is not the same thing
as the posting's `comp_min` / `comp_max` range (that's what the
employer advertised; this is what the operator asked for). The
schema has no column for it.

The tracker holds live data (per `project_tracker_in_active_use`
memory). Schema additions go through `golang-migrate` (ADR 0009);
write-path changes go through Kafka events (ADR 0001); validation
runs producer-side (ADR 0005); the TUI is the primary frontend
(ADR 0004). Any solution must respect those four boundaries.

## Decision

Three coordinated changes — one schema, one event, one TUI surface.

**1. New column `jobs.expected_comp`.**

```
expected_comp  numeric
```

Single scalar, not a range. The currency is the already-existing
`comp_currency` column — when the operator quotes a number, it's
in the same currency they're tracking the posting in. No new
`expected_comp_currency` column.

This is distinct from `comp_min` / `comp_max`, which describe the
*posting's* advertised range; `expected_comp` is the *operator's*
quoted number. Both can be present on the same row: the posting
says ₹40-60L and the operator asked for ₹55L.

No `CHECK` constraint — the operator might quote any positive
number, and validation at the producer (ADR 0005) handles "must
be ≥ 0" without baking it into the schema.

**2. New event `JobEdited` on topic `job.edited`.**

```go
type JobEdited struct {
    EventID  string    `json:"event_id"`
    JobID    string    `json:"job_id"`
    EditedAt time.Time `json:"edited_at"`

    // Every editable field is a pointer. nil = no change for this
    // field; non-nil = set the column to the dereferenced value.
    // Dereferenced zero values mean "clear to NULL" (or `'{}'` for
    // text[] columns) — the Store consumer translates accordingly.
    URL          *string          `json:"url,omitempty"`
    Title        *string          `json:"title,omitempty"`
    WorkMode     *events.WorkMode `json:"work_mode,omitempty"`
    Location     *string          `json:"location,omitempty"`
    Source       *events.Source   `json:"source,omitempty"`
    TechTags     *[]string        `json:"tech_tags,omitempty"`
    CustomTags   *[]string        `json:"custom_tags,omitempty"`
    Priority     *int             `json:"priority,omitempty"`
    ExpectedComp *float64         `json:"expected_comp,omitempty"`
}

`URL` and `Title` are *settable-only* — both columns are
`NOT NULL` (and `url` is `UNIQUE`), so a pointer to an empty
string is a validation error (`ErrMissingURL` / `ErrMissingTitle`,
reusing the `JobSubmitted` sentinels), not a clear. The "zero
means clear" convention applies to the other seven fields only.
```

Sparse-by-default: an event with only `Priority` set carries one
field on the wire. Replays are idempotent — setting `Priority = 4`
twice yields the same row. Two concurrent edits to disjoint fields
commute (last-writer-wins within a field, but no field clobbers
another field).

`event_id` is producer-generated, UNIQUE in `processed_events`
(ADR 0007 dedup pattern); replays from Kafka insert nothing on
conflict.

Notes are **not** part of `JobEdited`. The `job.note.added` topic
already exists and is append-only; the edit modal's "new note"
line publishes a separate `JobNoteAdded` alongside the `JobEdited`
on save. Two events, two topics, no schema overload.

`job.status.changed` is also not folded into `JobEdited`. Status
has its own history table (`job_status_history`) that drives
analytics (ADR 0001); collapsing the two would force the Store
consumer to write to two tables on what looks like one event,
muddying the "one event one append" model.

**3. New TUI mode `modeEdit` triggered by `e` on the selected row.**

A modal lists the editable fields with their *current* values
read from the in-memory job row. Up/Down navigates fields; Enter
edits the focused field; Esc-from-field-edit returns to the field
list; Esc-from-field-list cancels the whole modal; Ctrl+S saves.

Per-field interaction:

- **Enums** (`work_mode`, `source`): Left/Right cycles through
  allowed values + an explicit `(unset)` option that publishes a
  zero-valued pointer (clear-to-NULL).
- **Free text** (`location`): Enter opens an inline textinput
  pre-populated with the current value. Empty submit = clear.
- **Required free text** (`url`, `title`): same as free text,
  but empty submit is rejected with the modal-banner error
  (the `ErrMissingURL` / `ErrMissingTitle` sentinel) — the
  field cannot be cleared, only changed.
- **Numeric** (`priority`, `expected_comp`): Enter opens an inline
  textinput; `priority` accepts 1-5 (validated client-side and
  again at the Publisher per ADR 0005); `expected_comp` accepts a
  non-negative number. Empty submit = clear.
- **Tags** (`tech_tags`, `custom_tags`): Enter opens a textinput
  pre-populated with the comma-joined current tags; submit splits
  on comma and trims. Empty submit = clear to `'{}'`.
- **Notes** (a virtual "new note" line at the bottom): Enter opens
  a textinput. Submit appends — the existing note timeline is
  read-only here. Empty = no note added.

Ctrl+S publishes one `JobEdited` (containing only the fields whose
pointers are non-nil — i.e. fields the operator actually touched)
plus, if the note line was filled, one `JobNoteAdded`. Both events
share the same `EditedAt` / `CreatedAt` timestamp so the timeline
groups them.

The modal's field list is a single `bubbles/list` with custom row
rendering — no separate widget per field type. This keeps the
modal at one focusable thing at a time and avoids the tab-order
choreography a multi-input form would require.

## Status

Accepted.

## Group

Frontend / Edit surface.

## Assumptions

- The operator wants a single edit surface that covers all the
  fields they touch post-submit. A per-field hotkey approach
  (`m` for work_mode, `l` for location, …) was considered and
  rejected — see Positions §2.
- `expected_comp` is one number, not a range. The operator does
  not quote a range to a recruiter; they quote a target.
- `expected_comp` shares `comp_currency` with the posting's
  `comp_min` / `comp_max`. Cross-currency quoting (posting in
  USD, operator asks in INR) is rare enough on a personal tracker
  to defer.
- The fields named in the operator request (`work_mode`,
  `location`, `source`, tags, `priority`, `expected_comp`, notes)
  are the v1 surface, plus `url` and `title` for the rare
  correction case (typo at submit, posting URL changed). `comp_*`,
  `recruiter_*`, `description`, `deadline`, `resume_version`,
  `cover_letter_version`, `seniority`, and `referral` are editable
  in principle (they're in the event schema) but the modal does
  not surface them in v1. Adding a field to the modal later is a
  TUI-only change — no schema, no event change.
- Tags are two separate fields (`tech_tags`, `custom_tags`), not
  a unified one. They have different vocabularies (tech_tags is
  drift-detected per ADR 0005; custom_tags is open).
- Single operator, single producer. No concurrent-editor races to
  reason about. Two `JobEdited` events for the same job
  field-collide via last-writer-wins, which is the correct v1
  behaviour for a single-operator tool.

## Constraints

- Producers do not read from Postgres on the write path
  (ADR 0001). The TUI already holds the current row in memory
  (the modal opens against the selected `m.view[i]`), so the
  "what's the current value?" question is answered from existing
  TUI state — no extra Reader round-trip.
- Schema change through `golang-migrate` (ADR 0009). One up/down
  pair, additive (no backfill — `expected_comp` defaults to
  NULL).
- Validation runs at the Publisher (ADR 0005). New sentinels:
  `ErrInvalidPriority` (range 1-5), `ErrInvalidExpectedComp`
  (must be `> 0` — see Notes for why strict positive, not `≥ 0`),
  `ErrInvalidWorkMode` / `ErrInvalidSource` (must be in
  allow-list or the explicit "clear" sentinel). Tag values run
  through the same allowed-values check `JobSubmitted` already
  uses; unknown `tech_tags` fail validation, `custom_tags` are
  open.
- Event field names match Postgres column names (ADR 0001 rule).
  `JobEdited.ExpectedComp` ↔ `jobs.expected_comp`.
- `JobEdited` is *sparse*. The Store consumer must distinguish
  "absent" (do nothing) from "present with zero value" (set to
  NULL or `'{}'`). Pointer-based JSON gives that distinction
  without a parallel bool-field-per-field.
- `job.edited` partitions by `job_id`, matching the rest of the
  job-scoped topics. Two edits to the same job land on the same
  partition in order; the consumer applies them sequentially.
- The Store consumer applies one `JobEdited` in one transaction.
  Mixed success ("priority updated but tags failed") is not a
  state the operator can observe — either the whole event applies
  or the consumer fails the message and Kafka retries.
- `jobs.last_event_at` is updated on every `JobEdited` apply, so
  the dashboard "last event" column reflects edits as well as
  status changes.

## Positions

Alternatives considered:

1. **One `job.edited` event with a sparse pointer-based payload,
   per-field semantics in the consumer, single edit modal in the
   TUI** (this decision).
2. **Per-field events: `JobWorkModeSet`, `JobLocationSet`,
   `JobTagsSet`, `JobPrioritySet`, `JobExpectedCompSet`.** Rejected
   — five new topics, five new validation paths, five new consumer
   handlers, no shared schema benefit. The fields are co-edited in
   the same UX moment (operator opens the modal and adjusts three
   things at once); collapsing five publishes into one is a real
   win on a Kafka-backed system.
3. **One generic `job.field.changed` event with `{field_name,
   value}` and a `map[string]any` payload.** Rejected — defeats
   the typed-events principle (ADR 0001). The producer would lose
   compile-time field-name checks; the consumer would need a
   runtime switch over field names; allowed-value validation moves
   from a typed field to a string lookup. Verbose JSON for no
   payoff.
4. **Direct Postgres writes from the TUI for edits (skip Kafka).**
   Rejected — violates "no service writes to another's tables"
   (the same principle ADR 0010 explicitly carves an exception
   to for `companies.tags` / `companies.notes`, and explicitly
   names as a one-off). Doing it for `jobs.*` would mean the
   Telegram bot (ADR 0003) and any future frontend would have to
   either duplicate the SQL or be denied edit capability. The
   event path is the canonical one.
5. **Per-field hotkeys on the list view (`m` for work_mode, `l`
   for location, `p` for priority, …).** Rejected — the keymap is
   already dense (a/i/o/r/w/S/s/n/f/H/R/q/Slash) and adding seven
   more single-letter hotkeys exhausts the alphabet for common
   verbs and forces shift-modifiers. More importantly: per-field
   hotkeys give no way to *see* the current value before changing
   it, so the operator either has to memorise current state or
   look at the detail panel and then guess the cycle direction.
   A modal that lists current values is the correct discovery
   surface.
6. **A second three-step modal "URL → field → value" (CLI-style
   prompt chain).** Rejected — slower than the list-and-edit
   modal for the common case (editing two or three fields in one
   sitting). Three-step modals make sense for *create* flows
   where the steps have a fixed order; edit flows are
   commutative.
7. **Treat notes as part of `JobEdited` (a `note *string` field
   that, when non-nil, appends a note).** Rejected — overloads
   one event with two semantics (set fields, append to log).
   `JobNoteAdded` already exists and is append-only; the modal
   publishes both events, but they stay separate on the wire.
8. **Replace `comp_min` / `comp_max` with `expected_comp` (one
   compensation number).** Rejected — the posting's range and
   the operator's quote are two distinct facts about the same
   row. Conflating them loses information the analytics layer
   wants (e.g. "did I quote below the midpoint?").
9. **Add an `expected_comp_currency` column.** Rejected for v1 —
   the operator hasn't asked for cross-currency tracking, and
   `comp_currency` already lives on the row. Revisit if the
   operator starts quoting in a currency the posting doesn't use.
10. **A `dirty bool` flag on the modal that turns Ctrl+S into a
    save-all (push every field, including unchanged ones).**
    Rejected — defeats the sparse-event design and forces the
    Store consumer to overwrite every column on every edit, which
    breaks "two concurrent edits to disjoint fields commute".

## Argument

- **One event, one modal.** The operator's mental model is
  "open this row, change a few things, save". The wire format
  matches that — one `JobEdited` per save — instead of fanning
  out into five per-field publishes the operator never thinks
  about.
- **Sparse pointer semantics are the cheapest "unchanged vs.
  clear" encoding.** A parallel `SetWorkMode bool` per field
  doubles the schema surface; a `[]string changed_fields`
  metadata array means the consumer has to consult two places
  before applying any field. Pointers piggyback on JSON
  `omitempty` and Go's existing nil-vs-zero distinction.
- **`expected_comp` is a real missing column.** The operator
  named it explicitly; the schema today has no place to put it;
  shoehorning it into `custom_tags` ("quoted-55L") would lose the
  numeric value for analytics and break the comp-currency join.
- **The modal beats per-field hotkeys on discoverability.** With
  hotkeys, the operator must hold the current state in their
  head before pressing the key. With the modal, the field list
  shows current values; the edit is a deliberate "from X to Y"
  motion.
- **Validation stays at the Publisher.** Each pointer field that
  is non-nil runs through the same allowed-values check
  `JobSubmitted` already uses. No new validation pattern; just
  more callsites of `events.IsValidWorkMode` and friends.
- **The Store consumer change is small.** A single `UPDATE jobs
  SET … WHERE job_id = $1` with `COALESCE` for unchanged fields
  is one statement. The `last_event_at` bump piggybacks on the
  same `UPDATE`.
- **Notes stay where they are.** No reshape of `JobNoteAdded`,
  no new column on `job_notes`. The edit modal coordinates two
  publishes but the events themselves are unchanged.

## Implications

- **Migration.** One up/down pair under
  `internal/db/migrations/`. Up:
  ```sql
  ALTER TABLE jobs ADD COLUMN expected_comp numeric;
  ```
  Down: `ALTER TABLE jobs DROP COLUMN expected_comp`. No
  backfill (NULL is the correct value for jobs created before
  the operator started quoting). No index — analytics on
  `expected_comp` is a sequential-scan operation on a personal
  tracker; revisit if the row count ever justifies an index.
- **`internal/events/events.go`.** New const
  `TopicJobEdited = "job.edited"` and new struct `JobEdited`
  with the pointer fields above. `JobSubmitted` grows a
  matching `ExpectedComp *float64` field so the new-job modal
  can also accept a value at submit (deferred from the UI in v1
  — the modal still asks for URL/title/company only — but the
  field exists on the event so the CLI can pass `--expected-comp`
  without a follow-up event-schema change).
- **`internal/events/allowed_values.go`.** No new allow-list.
  `WorkMode` / `Source` / `Priority` reuse existing checks.
- **`internal/jobclient/validate.go`.** New `validateEdited` that:
  - Requires `EventID`, `JobID`, `EditedAt`.
  - Requires at least one non-nil field (an empty edit is a
    validation error — protects against accidental Ctrl+S on an
    untouched modal).
  - For each non-nil field, runs the same value check
    `validateSubmitted` already does.
- **`internal/jobclient/publisher.go`.** New `Edit(ctx,
  events.JobEdited) error` method. Same partition-key pattern
  (`JobID`), same JSON encoding.
- **`internal/jobclient/reader.go`.** `Job` projection grows an
  `ExpectedComp *float64` field so the edit modal can display
  the current value without a second query.
- **`internal/store/store.go`.** New `ApplyEdited` handler:
  1. Look up `processed_events` for `(consumer="store",
     event_id)`. Skip if already processed.
  2. Build a single `UPDATE jobs SET … WHERE job_id = $1`
     statement with one assignment per non-nil pointer in the
     event. Pointers to zero values become NULL / `'{}'`.
  3. Bump `last_event_at = $editedAt`.
  4. Insert into `processed_events`.
  5. Commit.
  No change to `ApplyStatusChanged` / `ApplyNoteAdded` /
  `ApplyInterviewRecorded`.
- **`internal/tui/tui.go`.** New `modeEdit`, new `e` keybind on
  the list view, new `viewEdit` renderer. The modal opens
  against `m.selectedJob()` and never re-fetches — current
  values come from the in-memory row.
- **`internal/tui/cmds.go`.** New `editJobCmd` (wraps
  `Publisher.Edit`) and `editJobAndNoteCmd` (tea.Batch of
  `Publisher.Edit` and `Publisher.AddNote` when the modal's
  note line is filled).
- **`cmd/cli/main.go`.** New `edit` subcommand mirroring the
  `JobEdited` shape: `--job-id`, optional `--work-mode`,
  `--location`, `--source`, `--tech-tags`, `--custom-tags`,
  `--priority`, `--expected-comp`. Unset flags publish nil
  pointers; flags set to `--clear` (sentinel) publish
  zero-value pointers. The CLI is the test surface for the
  event before the TUI modal lands.
- **`internal/bot`.** Out of scope for this ADR — the Telegram
  bot (ADR 0003) does not surface edits in v1. Adding it later
  is a bot-only change.
- **No drift detection.** `expected_comp` is numeric, not an
  enum.
- **Consumer-side replay.** A `JobEdited` event whose `event_id`
  is already in `processed_events (consumer="store")` is a
  no-op (ADR 0007 pattern).

## Related decisions

- **ADR 0001** — Richer schema and event contracts. This ADR
  adds one column and one event to that surface; the field-name
  symmetry and partition-key rules are reused verbatim.
- **ADR 0003** — Telegram bot. Edits are TUI-only in v1; the
  bot's edit surface is a follow-up.
- **ADR 0004** — Desktop TUI. The new `modeEdit` is added next
  to `modeNew` / `modeSearch` / `modeStatus`.
- **ADR 0005** — Producer-side input validation. `validateEdited`
  follows the existing pattern; new sentinels are added to
  `internal/jobclient/errors.go`.
- **ADR 0007** — Notifier dedup pattern. `processed_events`
  namespacing carries the new event without change.
- **ADR 0009** — Schema migrations via `golang-migrate`. The
  `expected_comp` column lands as the third real migration on
  top of `0001-init` and `companies`.
- **ADR 0010** — Companies as a first-class entity. The
  ADR explicitly defers a `CompanyEdited` event until a second
  frontend needs it. This ADR is the *job*-level edit event
  the operator's TUI surfaces; company edits remain a direct
  Postgres write from the companies panel for v1.

## Related requirements

- Editing posting metadata (`work_mode`, `location`, `source`,
  tags) after submit without a manual `psql` session.
- Editing personal scaffolding (`priority`) as the operator's
  view of the job evolves.
- Recording an expected compensation number distinct from the
  posting's advertised range.
- Adding a note in the same UX moment as the field edits.
- One event per save (no fan-out into per-field publishes).
- Producers do not read from Postgres on the write path.

## Related artifacts

- `internal/db/migrations/<ts>_expected_comp.up.sql` (new)
- `internal/db/migrations/<ts>_expected_comp.down.sql` (new)
- `internal/events/events.go` — new `TopicJobEdited`, new
  `JobEdited` struct, `JobSubmitted.ExpectedComp` field.
- `internal/jobclient/publisher.go` — new `Edit` method.
- `internal/jobclient/validate.go` — new `validateEdited`.
- `internal/jobclient/errors.go` — `ErrInvalidPriority`,
  `ErrInvalidExpectedComp`, `ErrInvalidWorkMode`,
  `ErrInvalidSource`, `ErrEmptyEdit`.
- `internal/jobclient/reader.go` — `Job.ExpectedComp` field;
  select it in `List` / `Get`.
- `internal/store/store.go` — new `ApplyEdited`; wire on the
  consumer's topic subscription list.
- `internal/tui/tui.go` — new `modeEdit`, `e` keybind, modal
  view, per-field edit state.
- `internal/tui/cmds.go` — `editJobCmd`, `editJobAndNoteCmd`.
- `cmd/cli/main.go` — new `edit` subcommand.

## Related principles

- **Events describe what happened.** `JobEdited` describes
  "the operator changed these N fields at this time" — not
  "set the jobs row to this projection".
- **Sparse over dense on the wire.** Pointer-based encoding
  means a one-field edit produces a one-field JSON payload.
- **One event one append.** Notes and field edits stay
  separate; status changes stay separate; the `job_status_history`
  table is not touched by `JobEdited`.
- **Producers don't read on the write path.** The modal's
  current-value display comes from the TUI's in-memory snapshot.
- **No service writes to another's tables.** Edits go through
  the Store consumer, not direct Postgres writes from the TUI.
- **Typed columns over schemaless escape hatches.** `expected_comp`
  is a `numeric` column, not a `custom_tags` entry.

## Notes

- **"Dereferenced zero means clear" collides with legitimately-zero
  values.** The pointer encoding distinguishes
  *unchanged* (nil pointer, dropped by `omitempty`) from
  *cleared* (pointer to the type's zero value: `""`, `0`,
  `[]string{}`). That only works when zero is never a value
  the operator might legitimately want to *set*. For v1:
  - `priority` is 1-5 — zero is never legitimate. Safe.
  - `location`, `work_mode`, `source` — empty/zero is never a
    "set" value, only a clear. Safe.
  - `tech_tags`, `custom_tags` — empty slice is the cleared
    state; a set always carries ≥ 1 tag. Safe.
  - `expected_comp` — `0.0` is *technically* a quote the
    operator could make, which would collide with "operator
    cleared the field". Resolved by validating `expected_comp
    > 0` at the Publisher (`ErrInvalidExpectedComp`). The
    operator cannot quote zero; if they ever want to, the
    fallback is option (2) in the open question below.
  If a future editable field has a legitimate zero (e.g.
  "interview score" where 0 is a real failing grade), the
  pointer-zero-means-clear convention breaks and that field
  needs an explicit `ClearX bool` companion or a different
  encoding.
- **Sparse JSON's `null` vs. absent distinction.** Standard
  `encoding/json` treats both `"work_mode": null` and an absent
  `work_mode` key as the zero pointer after decode. The event
  contract here uses **absent = unchanged, present-with-value =
  set**; explicit `null` is not generated by the Publisher and
  not honoured by the consumer. If a future encoder change
  produces `null` for cleared fields, the consumer must be
  updated in lockstep — flagged here so the test that ships
  with the consumer asserts both absent and present-with-zero
  behaviour explicitly.
- **Empty-edit guard.** The Publisher rejects a `JobEdited`
  with no non-nil fields (`ErrEmptyEdit`). This is a UX safety
  net for accidental Ctrl+S, not a correctness requirement —
  the consumer would no-op anyway, but a producer-side
  rejection gives the operator immediate feedback (banner in
  the modal, ADR 0005 pattern).
- **`last_event_at` semantics.** Bumping `last_event_at` on
  edits means the dashboard's "last event" column changes
  whenever the operator opens the modal and saves. The
  alternative is to leave `last_event_at` for status
  transitions only, which would make the dashboard
  understate activity. The bump is the intended behaviour;
  the column is "last anything happened", not "last status
  change".
- **`comp_currency` reuse.** `expected_comp` is interpreted in
  the same currency as `comp_currency`. If `comp_currency` is
  NULL, the operator hasn't recorded one — analytics that
  compare `expected_comp` to `comp_min` / `comp_max` should
  treat that row as "currency-unknown" and skip cross-currency
  arithmetic. Out of scope for this ADR; surfaced for the
  analytics work that follows.
- **Resolved (per operator review):** `JobEdited` carries
  `url` and `title` as settable-only pointers. Both columns
  are `NOT NULL`, so the "zero means clear" rule does not
  apply — a pointer to an empty string is rejected at the
  Publisher with the existing `ErrMissingURL` / `ErrMissingTitle`
  sentinels. `url` edits can additionally fail at the Store
  consumer with a unique-violation if the new value collides
  with another job; that surfaces back to the operator as a
  publish error in the modal banner. Rare-use, but the cost
  of including them is two extra pointer fields and one
  validation reuse — no new pattern.
- **Resolved (per operator review):** the v1 modal exposes
  the operator-named fields plus `expected_comp` plus
  `url` / `title` plus the note line. `recruiter_*`,
  `comp_*`, `description`, `deadline`, `resume_version`,
  `cover_letter_version`, `seniority`, and `referral` are
  not surfaced in v1; the day they are, it's a TUI-only
  commit (the event already carries them).
- **Resolved (per operator review):** `tech_tags` and
  `custom_tags` are edited as full *replacements*, not
  add/remove sets. The operator types a comma list and the
  consumer overwrites the column. Add/remove semantics need a
  richer event payload and a multi-action UI; v1 is simpler
  and matches how the new-job modal already shapes tags.
