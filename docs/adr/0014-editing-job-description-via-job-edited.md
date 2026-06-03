# ADR 0014 — Surfacing `jobs.description` in the TUI edit modal

## Issue

`jobs.description` is already a `text` column on the `jobs` table
(`internal/db/migrations/20260525110139_init.up.sql:33`) and
`JobSubmitted.Description` is already a field on the submit event
(`internal/events/events.go:111`). The CLI's `submit` subcommand
populates it via `--description-file` (`cmd/cli/main.go:155, 180-186,
213`); the Store consumer writes it on insert
(`internal/store/store.go:80, 104, 124`); the Reader projects it back
out (`internal/jobclient/reader.go:46, 194, 232`). Storage and the
write path both exist.

What does *not* exist is any way for the TUI operator to set or
change a job description:

1. **The new-job modal has no description input.** ADR 0010 §Decision
   §4 fixes the modal at three fields (URL, title, company) by
   design ("no second modal, no two-phase flow"). The CLI's
   `--description-file` is the only ingress today, and the operator's
   primary frontend is the TUI (ADR 0004).
2. **`JobEdited` deliberately omits `Description`.** ADR 0011 lists
   `description` as "editable in principle (it's a column) but the
   modal does not surface it in v1" and the `JobEdited` struct at
   `internal/events/events.go:149` carries `URL`, `Title`,
   `WorkMode`, `Location`, `Source`, `TechTags`, `CustomTags`,
   `Priority`, `ExpectedComp` — and nothing else. There is no way
   to change description after submit except a manual `psql UPDATE`.

In practice the operator captures the URL/title/company in the
moment, reads the posting later, and wants to paste the description
into the row at the moment they read it — which is not submit time
and is rarely a CLI session. The description is also one of the
fields most likely to accrete *after* the row exists (the recruiter
shares a JD via email; the original posting page expires; the
operator wants a snapshot pinned to the row before the URL 404s).

The tracker holds live data (per `project_tracker_in_active_use`
memory). Schema additions go through `golang-migrate` (ADR 0009) —
not applicable here since no schema change is needed. Write-path
changes go through Kafka events (ADR 0001); validation runs
producer-side (ADR 0005); the TUI is the primary frontend (ADR 0004);
the edit surface is `JobEdited` (ADR 0011). Any solution must
respect those four boundaries.

## Decision

Two coordinated changes — one event field, one TUI surface. **No
schema change. No new event topic. No change to the submit modal.**

**1. Add `Description *string` to `JobEdited`.**

```go
type JobEdited struct {
    EventID  string    `json:"event_id"`
    JobID    string    `json:"job_id"`
    EditedAt time.Time `json:"edited_at"`

    URL          *string   `json:"url,omitempty"`
    Title        *string   `json:"title,omitempty"`
    WorkMode     *WorkMode `json:"work_mode,omitempty"`
    Location     *string   `json:"location,omitempty"`
    Source       *Source   `json:"source,omitempty"`
    TechTags     *[]string `json:"tech_tags,omitempty"`
    CustomTags   *[]string `json:"custom_tags,omitempty"`
    Priority     *int      `json:"priority,omitempty"`
    ExpectedComp *float64  `json:"expected_comp,omitempty"`
    Description  *string   `json:"description,omitempty"`
}
```

Follows the existing sparse-pointer convention exactly: nil means
"unchanged", a pointer to a non-empty string means "set to this
value", and a pointer to `""` means "clear to NULL". The
`jobs.description` column is nullable, so "zero means clear"
applies — `description` is not part of the URL/Title settable-only
carve-out (which exists only because those columns are `NOT NULL`).

The Store consumer's `ApplyEdited` (ADR 0011 §Implications) needs
one extra `COALESCE`-style assignment in its `UPDATE jobs SET …`
statement. The pattern is identical to the existing eight
editable fields.

**2. New "Description" line in the TUI edit modal, opened as a
multi-line `bubbles/textarea`.**

`modeEdit` (ADR 0011) gains a "Description" entry in the field list,
positioned after `expected_comp` and before the virtual "new note"
line. Per-field interaction:

- Up/Down on the field list focuses Description like any other row.
- The row renders `Description: <first-line snippet, truncated to
  80 chars with "…" suffix>` so the operator can see the current
  state without entering edit mode. Empty descriptions render as
  `Description: (none)`.
- Enter on the focused row enters a sub-mode (`modeEditDescription`)
  that pushes a `bubbles/textarea` filling the available modal
  body. The textarea is pre-populated with the current description
  (or empty if NULL).
- **Ctrl+S inside the textarea** stages the typed value and returns
  to the field list. The staged value is held on the modal state
  (`m.editStaged.Description *string`) the same way every other
  field is held — non-nil if touched, nil if untouched.
- **Esc inside the textarea** discards changes and returns to the
  field list. The staged value is unchanged from its prior state
  (still nil if it was nil; still pointing to a previously-staged
  value if the operator re-entered the editor).
- Empty submit (`Ctrl+S` on an empty textarea) stages a pointer to
  `""` — which the Publisher accepts and the Store consumer
  translates to `description = NULL`.
- The textarea respects standard movement (arrows, Home/End,
  Ctrl+A/E if the bubbles version supports them) and pastes
  multi-line content via the terminal's bracketed-paste — the
  operator's primary use case is "paste the JD body from a
  recruiter email".

Ctrl+S on the *modal* (i.e., on the field list, not inside the
textarea) publishes the same single `JobEdited` ADR 0011 already
publishes, now potentially carrying a `Description` pointer.

The note line is unchanged. Descriptions and notes are different
things: description is the posting's body (one canonical value per
row, replace-on-edit); notes are an append-only operator timeline
(many per row).

## Status

Accepted.

## Group

Frontend / Edit surface.

## Assumptions

- Operators paste descriptions more often than they type them. The
  textarea must accept multi-line bracketed paste cleanly; a
  single-line `textinput` would force a newline-escaping or
  truncation rule that no posting body actually wants.
- A first-line snippet (truncated to ~80 chars) is enough preview
  in the field list. The operator does not need to see the full
  body in the list — they enter the editor when they want to
  read or change it.
- `bubbles/textarea` is already a dependency or trivially addable.
  The TUI is on Bubble Tea (ADR 0004); the textarea component
  ships in the standard `charmbracelet/bubbles` set.
- The submit modal stays three fields. The operator has not asked
  for description at submit; the CLI's `--description-file` already
  covers the "I have the JD at submit" workflow; the TUI's
  primary submit pattern is "URL + title + company now, everything
  else later via edit".
- Description is one canonical value per row. The operator does
  not want a history of description edits — if a JD is replaced
  with a different one, the prior is gone. (Notes are how the
  operator pins "the posting changed" if they want to preserve
  that.)
- Description has no allowed-value constraint. Any string,
  including the empty string (= clear), is acceptable. No
  drift-detection list (ADR 0005) applies.

## Constraints

- **No schema change.** `jobs.description` already exists as
  `text` (nullable). The migration column is untouched; no
  `golang-migrate` pair is added.
- **No new event topic.** `job.edited` is the carrier. Adding a
  topic for a single field would defeat the consolidation
  argument ADR 0011 §Positions §2 made for keeping edits on one
  topic.
- **Validation runs at the Publisher (ADR 0005).** A
  `Description` pointer requires no allowed-value check — it's
  free text — but the empty-edit guard (`ErrEmptyEdit`,
  ADR 0011 §Notes) must continue to accept a `Description`-only
  edit as a non-empty edit. (The current rule is "≥ 1 non-nil
  field"; that already covers this case as long as `Description`
  is counted as a touched field — which it is, since the pointer
  is non-nil iff the operator entered the textarea and Ctrl+S'd
  out of it.)
- **Field names match Postgres column names (ADR 0001).**
  `JobEdited.Description` ↔ `jobs.description`. Same casing
  convention (camelCase Go, snake_case wire and DB).
- **Producers do not read from Postgres on the write path
  (ADR 0001).** The textarea pre-populates from the TUI's
  in-memory `m.selectedJob().Description` — already loaded by
  the Reader on the most recent `listJobsCmd`. No second query.
- **Push-based reconciliation (ADR 0012) is unchanged.** The
  Store's `NOTIFY jobs_changed` fires after the `UPDATE jobs`
  commits a `JobEdited`; the TUI's `LISTEN` loop re-renders the
  field list with the new first-line snippet.
- **Same partition key (ADR 0001).** `JobEdited` continues to
  partition by `job_id`; the description-carrying variant
  carries no new ordering requirement.
- **`JobSubmitted.Description` is unchanged.** The submit-time
  field already exists; this ADR neither widens nor narrows it.

## Positions

Alternatives considered:

1. **Add `Description *string` to `JobEdited`; surface in the
   edit modal via an inline `bubbles/textarea`; no submit-modal
   change; no schema change** (this decision).
2. **Add description as a 4th step in the new-job modal.**
   Rejected — ADR 0010 §Decision §4 fixes the modal at three
   fields as an explicit design boundary ("no second modal, no
   two-phase flow"). The operator confirmed they don't want it
   at submit; "I have the JD now" is already handled by the
   CLI's `--description-file`.
3. **A separate post-submit prompt: 3-step modal → optional
   description editor.** Rejected — same operator decision as
   §2, and structurally it's the same control flow as "submit
   then immediately press `e` and Enter on Description", which
   the edit modal already provides without a special-case
   second-modal flow.
4. **Shell out to `$EDITOR` (vim/nvim/nano) for description
   editing.** Rejected — operator preference for inline textarea;
   breaks the pure-TUI feel; introduces a tempfile lifecycle
   (write, exec, read, delete, handle SIGINT) and a "what if
   `$EDITOR` is unset" branch the inline textarea doesn't need.
   A future `E` keybind ("open this field in `$EDITOR`") inside
   the textarea is a defensible follow-up, but not v1.
5. **Reuse the single-line `bubbles/textinput` the other free-text
   fields use.** Rejected — descriptions are routinely multi-
   paragraph; a single-line input either forces a newline-escape
   convention (`\n` literally, ugh) or truncates on paste. The
   one extra component is the right cost.
6. **New event `JobDescriptionSet` on a dedicated
   `job.description.set` topic.** Rejected — ADR 0011 §Positions
   §2 already considered and rejected per-field events. One
   extra pointer on `JobEdited` is a strict improvement over a
   new topic, a new validator, and a new Store handler.
7. **A new `description` column type — say, a `descriptions`
   child table for "history of description edits".** Rejected —
   the operator did not ask for description history. If they
   ever do, notes (`job.note.added`, append-only) already give
   them "the JD changed today, here's what it used to say"
   semantics without a new table.
8. **`Description string` (non-pointer) + a parallel
   `ClearDescription bool` field.** Rejected — breaks the
   sparse-pointer convention ADR 0011 chose for the other eight
   editable fields. Description has no "zero is a legitimate
   value" problem (an empty description body and a NULL
   description are operationally the same: "no description
   captured"), so the pointer-zero-means-clear shortcut applies
   cleanly here.
9. **Render the full description in the field-list row, not a
   first-line snippet.** Rejected — a real posting body is
   hundreds of lines; the field list would scroll. The snippet
   answers "is there one yet, and roughly what is it about"; the
   editor answers "show me the whole thing".

## Argument

- **The column exists; this is a frontend gap, not a storage
  gap.** Adding `Description *string` to `JobEdited` is one
  struct field, one Publisher validator pass-through, one
  consumer COALESCE branch, and a textarea in the modal. No
  migration, no new topic, no new handler shape.
- **One event, one modal, still.** ADR 0011's "the operator opens
  this row, changes a few things, saves" model already includes
  description in spirit — the field was deferred for UI reasons,
  not contract reasons. Promoting it to the modal completes that
  model.
- **Textarea is the cheapest correct widget.** The pasted-from-
  email use case is the dominant one; a single-line input would
  punish exactly the workflow the operator hits most.
- **Sparse-pointer semantics work here unmodified.** An empty
  string description and a NULL description carry the same
  operational meaning ("no description"); collapsing the two via
  zero-means-clear avoids both a parallel `ClearDescription` bool
  and the kind of "is the empty string a value or an absence"
  schema fight ADR 0011 §Notes already navigated for
  `expected_comp`.
- **The CLI path stays intact.** `--description-file` on `submit`
  continues to work; this ADR doesn't deprecate or duplicate it.
  A future CLI `edit --description-file=…` is a trivial addition
  if the operator wants a shell-level edit path.
- **The submit modal stays at three fields.** ADR 0010's
  three-field invariant is preserved; this ADR does not amend
  that decision.

## Implications

- **No migration.** `jobs.description` already exists. The
  `internal/db/migrations/` directory is not touched.
- **`internal/events/events.go`.** `JobEdited` gains
  `Description *string `\`json:"description,omitempty"\``. The
  field is added at the end of the struct so existing JSON
  payloads continue to decode without reordering.
- **`internal/events/allowed_values.go`.** No change. Description
  has no allow-list.
- **`internal/jobclient/validate.go`.** `validateEdited` gains
  one branch: if `Description != nil`, count it as a touched
  field for the `ErrEmptyEdit` check; no value validation
  beyond that. No new sentinel.
- **`internal/jobclient/publisher.go`.** `Edit` method
  signature is unchanged — it already takes
  `events.JobEdited`. JSON encoding picks up the new field
  automatically.
- **`internal/jobclient/reader.go`.** No change. `Job.Description`
  already exists (line 232) and is already selected.
- **`internal/store/store.go`.** `ApplyEdited` gains one
  assignment in the `UPDATE jobs SET …` statement, following the
  same COALESCE / NULL-on-zero pattern the other nullable text
  columns use. A pointer to `""` writes `NULL`; a pointer to a
  non-empty string writes the value; nil leaves the column
  untouched. `last_event_at` continues to bump as before.
- **`internal/tui/tui.go`.** New `modeEditDescription` constant
  alongside `modeEdit`. The edit modal's field list adds a
  "Description" entry; selecting it and pressing Enter
  transitions to `modeEditDescription`, which renders the
  textarea over the modal body. Ctrl+S inside the textarea
  stages the value and returns to `modeEdit`; Esc discards and
  returns. Modal-level Ctrl+S behaves as today and publishes
  the single `JobEdited`.
- **`internal/tui/cmds.go`.** No new command. `editJobCmd`
  already wraps `Publisher.Edit`; the staged `Description`
  pointer rides along inside the existing payload.
- **`internal/tui/` rendering.** Field-list row renderer for
  Description renders the first line truncated to ~80 chars
  with "…" if longer, or `(none)` if NULL. The truncation logic
  is one helper (`firstLineSnippet(s string, max int) string`)
  used only here.
- **`cmd/cli/main.go`.** Optional follow-up: an `edit
  --description-file=<path>` flag that publishes a
  `JobEdited` with `Description` set. Useful for batch /
  scripted updates and as a test harness before the textarea
  lands. Not strictly required for this ADR; flagged here so
  the door is open.
- **`internal/bot/`.** Out of scope. The Telegram bot does not
  surface edits in v1 (ADR 0011 §Implications). Adding
  description editing in chat is a follow-up; the bot's reply
  keyboard is unsuited to multi-paragraph input.
- **Dependency.** `github.com/charmbracelet/bubbles/textarea`
  is added to `internal/tui/` imports if not already present.
- **Drift detection.** None applicable — description is free
  text.
- **Consumer-side replay.** A `JobEdited` carrying only
  `Description` is processed-events-deduped on `event_id` the
  same way as any other edit (ADR 0007).

## Related decisions

- **ADR 0001** — Richer schema and event contracts. The
  `jobs.description` column was introduced here; this ADR adds
  no schema, only a frontend and a wire field.
- **ADR 0004** — Desktop TUI on Bubble Tea. The `modeEdit` /
  `modeEditDescription` modal stack lives in the same TUI
  surface; the textarea is one of the standard `bubbles`
  widgets.
- **ADR 0005** — Producer-side input validation. `validateEdited`
  gains one branch; no new sentinel. Description has no
  allow-list.
- **ADR 0007** — Notifier dedup pattern. `processed_events`
  carries the description-bearing `JobEdited` unchanged.
- **ADR 0009** — Schema migrations via `golang-migrate`. *Not*
  used in this ADR; the column already exists.
- **ADR 0010** — Companies as a first-class entity. The
  three-field submit modal invariant from this ADR is
  preserved; description is *not* added to the submit modal.
- **ADR 0011** — Editing job fields via `JobEdited`. This ADR
  extends the same event with one more pointer field and the
  same modal with one more line and one nested sub-mode.
  Conventions (sparse pointers, zero-means-clear, modal-level
  Ctrl+S publishes, Esc cancels) are reused verbatim.
- **ADR 0012** — Push-based TUI reconciliation. The
  description-carrying `JobEdited` triggers `NOTIFY
  jobs_changed` on commit and the TUI re-renders the field
  list with the new snippet.

## Related requirements

- Capture or replace a posting's description from the TUI
  without a `psql` session.
- Preserve the three-field submit modal invariant (ADR 0010).
- Reuse `JobEdited` rather than introducing a new topic for a
  single field.
- Keep producer-side validation thin: free text needs no
  allow-list, only the empty-edit guard.
- Multi-line paste must round-trip cleanly (the dominant
  workflow is "paste the JD body from email").

## Related artifacts

- `internal/events/events.go` — `JobEdited` gains
  `Description *string`.
- `internal/jobclient/validate.go` — `validateEdited` counts
  `Description != nil` toward the touched-field check.
- `internal/store/store.go` — `ApplyEdited` adds one assignment
  in the `UPDATE jobs SET …` statement.
- `internal/tui/tui.go` — new `modeEditDescription`; field-list
  "Description" entry; first-line snippet renderer.
- `internal/tui/` — `bubbles/textarea` import; staged
  description on the modal state.
- `cmd/cli/main.go` — optional `edit --description-file` flag
  (follow-up).

## Related principles

- **Events describe what happened.** A `JobEdited` with a
  `Description` pointer says "the operator set / cleared the
  description"; the Store decides what to write.
- **Sparse over dense on the wire.** Pointer encoding means a
  description-only edit produces a one-field JSON payload.
- **No business logic in events.** Description is a free-text
  column; the event carries the value, nothing more.
- **Producers don't read on the write path.** The textarea
  pre-populates from the TUI's in-memory snapshot, not a
  Postgres round-trip.
- **No service writes to another's tables.** Description edits
  go through `JobEdited` → Store consumer, not direct Postgres
  writes from the TUI.

## Notes

- **Empty-edit guard interaction.** ADR 0011's
  `ErrEmptyEdit` rejects a `JobEdited` with zero non-nil
  fields. A `Description`-only edit must pass this guard. The
  Publisher counts every non-nil pointer; adding
  `Description != nil` to that count is one line of code, but
  the semantic is "the operator entered the textarea and hit
  Ctrl+S" — which is a real touched-field signal even if the
  textarea content is empty (= clear).
- **Empty-string-vs-NULL collapse.** `jobs.description` is
  nullable text; the Store consumer treats an incoming
  pointer-to-empty-string as "set to NULL". This is correct
  for description (an empty body and an absent body carry the
  same operational meaning) and matches the pattern other
  nullable text fields use in `ApplyEdited`. If a future
  consumer wanted to distinguish "operator explicitly set
  empty string" from NULL, that would require either a
  parallel `ClearDescription` bool or a sentinel string — both
  rejected here because there is no operator workflow that
  benefits from the distinction.
- **Whitespace-only descriptions.** The Publisher does not
  trim. A pointer to `"   \n  "` writes the literal
  whitespace; the Store does not collapse it to NULL. This
  matches the way other free-text fields behave
  (`location` etc. in ADR 0011) and avoids "did the operator
  mean it?" guessing. If trim-on-clear becomes desirable,
  it's a TUI-side normalisation before publish, not a
  Publisher or consumer change.
- **First-line snippet rendering.** Truncation runs on the
  first line only (split on the first `\n`), then trimmed to
  `max` runes with a `…` suffix if longer. Rune-aware
  truncation matters because postings include non-ASCII
  (currency symbols, em-dashes); a byte-count truncation can
  split mid-codepoint and render as a replacement glyph.
- **Textarea size.** The textarea fills the modal body
  (everything below the modal header and above the modal
  footer / hint line). On terminal resize, the textarea
  re-flows via Bubble Tea's `tea.WindowSizeMsg` plumbing
  already in `internal/tui/tui.go`.
- **Modal-level Ctrl+S vs. textarea-level Ctrl+S.** Inside
  `modeEditDescription`, Ctrl+S commits the textarea content
  to the staged field and returns to `modeEdit`. Inside
  `modeEdit` (field list), Ctrl+S publishes the `JobEdited`.
  The keybind is the same character but the semantic differs
  by sub-mode; the modal footer hint line ("`Ctrl+S` stage •
  `Esc` cancel" vs. "`Ctrl+S` save • `Esc` close") makes
  this explicit.
- **No description history.** A second `JobEdited` with a
  new `Description` overwrites the prior column value. The
  operator who wants a record of "the JD changed" pins it
  with a `JobNoteAdded` ("recruiter sent updated JD, prior
  said remote, this one says hybrid") — separate event,
  separate timeline, intended workflow.
- **CLI parity.** The `submit --description-file` path is
  unchanged. A follow-up `edit --description-file` is a
  clean fit (read file → publish `JobEdited{Description:
  &body}`) and is the natural test surface for the wire
  field before the textarea lands; left as an
  implementation-time decision rather than mandated here.
- **Bot deferred.** Telegram chat is the wrong place to
  paste a 500-line JD; the bot's edit surface remains a
  future ADR (ADR 0011 already defers it). Description
  editing via the bot is not on this roadmap.
