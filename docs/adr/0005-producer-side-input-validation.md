# ADR 0005 — Producer-side input validation in `internal/jobclient`

## Issue

When a user adds or updates a job through any frontend (CLI, Telegram
bot, TUI), the request fans out as an event onto Kafka and the producer
returns success immediately. `internal/store` consumes the event some
milliseconds-to-seconds later and writes to Postgres, where the schema's
`CHECK` constraints (ADR 0001) finally validate the input.

This layering has a hole. If the user submits an invalid value —
`--status foo` from CLI, a typo in a TUI form field, a malformed URL
pasted into the bot — none of:

- the frontend
- `jobclient.Publisher`
- Kafka

rejects it. The bad event lands on the topic. The store consumer hits
the `CHECK` constraint, errors, and the only trace is a log line on a
server the user is not watching. The user has already seen "✓ added"
and walked away. The row never appears in their next `/list`, and they
have no way to know why.

The user's attention is in the frontend at the moment of submission.
That is the only place where surfacing the error is cheap — a banner,
an inline message, a non-zero exit. Every layer downstream has lost
the user.

This is the producer half of a broader "errors are async and invisible"
problem. ADR 0006 covers the consumer half (genuine infrastructure
failures, deploy-time schema drift) — different problem class, different
solution. This ADR is strictly about input the frontend could have
caught.

## Decision

Add input validation to `internal/jobclient.Publisher` for every event
it produces, returning typed sentinel errors that frontends render in
their own idiom.

**Sentinel errors** live in `internal/jobclient/errors.go`, extending
the file introduced for `ErrNotFound` in ADR 0002:

- `ErrInvalidStatus` — `status` not in the schema's allowed set.
- `ErrInvalidWorkMode`, `ErrInvalidSeniority`, `ErrInvalidSource` —
  same for those enums.
- `ErrInvalidInterviewRound` — for `JobInterviewRecorded`.
- `ErrInvalidURL` — fails `net/url.Parse`, missing scheme, or scheme
  outside `{http, https}`.
- `ErrMissingTitle`, `ErrMissingCompany`, `ErrMissingURL`,
  `ErrMissingJobID` — required fields empty.
- `ErrInvalidCompensation` — `comp_min > comp_max`, negative values, or
  currency outside an allowed ISO-4217 subset.
- `ErrInvalidDeadline` — deadline strictly in the past (today passes).

The allowed-set values are defined once in `internal/events/` as
exported slices (`AllowedStatuses`, `AllowedWorkModes`, etc.). The
validator and any future input-helper (TUI dropdown, bot keyboard) read
from the same slices. The schema's `CHECK` constraints remain the
source of truth at the persistence layer; the in-process slices mirror
them and a single test asserts they don't drift.

**Each frontend renders errors in its own idiom:**

- `cmd/cli` — `error: invalid status "foo"; allowed: saved, applied,
  interview, offer, rejected, withdrawn` to stderr, exit non-zero.
- `cmd/bot` — chat reply in the same conversation with the same
  message. The bot's command handlers already use chat replies for ack
  and dedup; one more reply form.
- `cmd/tui` — red banner above the form, focus the offending field,
  block submission. The form stays open until the user corrects the
  input or cancels; no override.

Frontends branch on `errors.Is(err, jobclient.ErrInvalidStatus)` etc. —
same idiom as the existing `errors.Is(err, jobclient.ErrNotFound)` on
`Reader`.

The Publisher methods (`Submit`, `ChangeStatus`, `AddNote`,
`RecordInterview`) each validate before producing. A validation failure
is a guaranteed-no-publish; the Kafka client is not touched.

## Status

Accepted.

## Group

Frontend / Input validation.

## Assumptions

- The set of `CHECK`-constrained enum values is stable enough that
  mirroring them in-process is cheap. New values are added in lockstep:
  edit `schema.sql`, edit the slice in `internal/events/`. The
  drift-detection test catches forgetfulness.
- Validation lives at the producer, not in `internal/events/` struct
  construction — the events package stays a pure schema layer (ADR
  0002 constraint). Validators live in `jobclient`.
- Frontends are the only producers. No third-party producer needs to
  be defended against. The schema's `CHECK` constraints remain the
  second line of defense if that assumption ever changes.
- Validation is synchronous and fast (regex, slice contains, simple
  comparisons). No I/O, no Postgres round-trip.

## Constraints

- No business logic in validators. Allowed-set checks, required-field
  checks, basic format checks (URL parses) only. Anything that asks
  "does this make sense given other data?" (status transition
  legality, e.g.) does not belong here.
- No new external dependencies. `net/url` and standard library only.
- Sentinel errors are exported and stable. They are part of
  `jobclient`'s public surface; renaming them is a breaking change for
  every frontend.
- Validators must produce error messages that include the offending
  value and the allowed set. The frontend renders the message verbatim;
  no per-frontend re-translation.
- A validation failure must not produce a partial side effect. If
  `Submit` validates the first three fields and fails on the fourth,
  no Kafka produce is attempted.

## Positions

Alternatives considered:

1. **Validation in `jobclient.Publisher` + typed sentinels in
   `errors.go`** (this decision).
2. **Validation in each frontend.** Rejected — every new frontend
   would re-implement it, drift would be invisible, and the symmetry
   with the schema's `CHECK` constraints would be lost. The whole
   point of the shared library (ADR 0002) is to avoid this.
3. **Validation in `internal/events/` constructors.** Rejected —
   `events` is the schema layer; mixing validation into it conflates
   the two. ADR 0002 already drew this line.
4. **Validation only at the store consumer, surfaced via an alert
   channel (Telegram).** Rejected — the user is no longer at the
   keyboard by the time the consumer runs. Operator-channel alerts
   for *validation* errors are the wrong audience and the wrong
   latency. (This was the prior draft's approach; the rework
   rejects it.)
5. **Reflective validation via struct tags
   (`validate:"oneof=saved applied …"`).** Rejected — pulls in a
   validation library, hides the rules from readers of the code, and
   is harder to extend with cross-field checks. Hand-written
   validators are a few dozen lines total.
6. **Reading allowed values directly from Postgres on startup
   (parsing the `CHECK` constraint expressions).** Rejected —
   fragile, requires Postgres availability at Publisher init, and
   parses constraint SQL. The drift-detection test is much cheaper.

## Argument

- **The user is in the frontend.** That is the only moment where
  "this status isn't allowed" can be turned into a corrective action
  without latency or out-of-band channels. Every layer downstream is
  a worse place to discover the same fact.
- **Three frontends, one validator.** Putting it in
  `jobclient.Publisher` is the minimal change that covers all of them.
  The TUI gets it for free the moment the package gains it.
- **Symmetry with the schema.** ADR 0001 established the `CHECK`
  constraints as the source of truth. This ADR mirrors them in-process
  so the producer can fail fast for the same reasons Postgres would
  fail later. Schema stays canonical; the producer stays honest.
- **Removes the bulk of "poison messages" upstream.** ADR 0006 will
  handle genuine infrastructure-shaped failures at the consumer. By
  solving validation here, that ADR shrinks: most things the consumer
  could have rejected, the producer rejected first.
- **No new infrastructure.** No DLQ topic, no alerter service, no
  Telegram coupling. The change is one new file
  (`internal/jobclient/validate.go`), an extended `errors.go`, and
  one call from each Publisher method.

## Implications

- `internal/jobclient/errors.go` grows from one sentinel
  (`ErrNotFound`) to ~10.
- `internal/jobclient/validate.go` (new) — `validateSubmitted`,
  `validateStatusChanged`, `validateNoteAdded`,
  `validateInterviewRecorded`. Each Publisher method calls its
  validator first.
- `internal/events/` gains exported slices `AllowedStatuses`,
  `AllowedWorkModes`, `AllowedSeniorities`, `AllowedSources`,
  `AllowedInterviewRounds`, `AllowedCurrencies` (`INR, USD, EUR,
  AUD`). Single source of truth on the Go side.
- `cmd/cli/main.go` — subcommands branch on `jobclient` sentinel
  errors and print stderr messages.
- `cmd/bot/` — the message handler that publishes job events replies
  in chat with the validator's message on failure.
- `cmd/tui/` — submit-form view stores the last validation error in
  its model, renders a banner, and blocks submission until the input
  is corrected or the form is cancelled. The bubbletea update
  function already handles per-field focus; the "jump to offending
  field" hook is small.
- `internal/jobclient/validate_test.go` (new) — loads `schema.sql`,
  parses `CHECK ... IN ('a','b',...)` for each constrained column,
  asserts the in-process slice matches. Single test, catches drift.
- No changes to consumers. They will still validate independently via
  `CHECK` constraints (defense in depth), and ADR 0006 will define
  what happens when they fail anyway.

## Related decisions

- **ADR 0001** — Schema and event contracts. The `CHECK` constraints
  define the allowed sets this ADR mirrors.
- **ADR 0002** — Shared `jobclient` library. Establishes the package
  as the home for cross-frontend producer logic and the `errors.go`
  sentinel pattern.
- **ADR 0003** — Telegram bot. Will render validation errors as chat
  replies.
- **ADR 0004** — Desktop TUI. Will render validation errors as form
  banners.
- **ADR 0006** — Consumer-side error classification. Sibling to this
  ADR. Handles the failure modes this one cannot prevent (infra
  outages, deploy-time schema drift).

## Related requirements

- A user submitting an invalid job through any frontend learns about
  it in the same UI, in the same moment.
- Validation rules live in one place, mirrored from the schema, and
  don't drift across three frontends.
- Failed validation never produces a Kafka message.
- The producer half of the "errors are async and invisible" problem
  is closed.

## Related artifacts

- `internal/jobclient/errors.go` — extended sentinel set.
- `internal/jobclient/validate.go` (new) — validator functions.
- `internal/jobclient/publisher.go` — validator wiring on each
  Publisher method.
- `internal/jobclient/validate_test.go` (new) — schema-drift
  detection.
- `internal/events/allowed_values.go` (new) — exported allowed-set
  slices.
- `cmd/cli/main.go`, `cmd/bot/`, `cmd/tui/` — render validation
  errors.
- `internal/db/schema.sql` — unchanged; remains the source of truth.

## Related principles

- **Thin frontends, shared infrastructure.** Validators belong in the
  shared library, not in each frontend.
- **No business logic in events.** Allowed-set checks and
  required-field checks are validation, not business rules; they
  belong in `jobclient`, not `internal/events/`.
- **Fail at the boundary.** The cheapest place to detect bad input
  is where it was typed.
- **Schema as canonical truth.** In-process slices mirror, never
  override.

## Notes

- **URL validation.** `net/url.Parse` accepts almost anything; the
  validator additionally requires `scheme in {http, https}` and a
  non-empty `Host`. Stricter checks (DNS resolution, reachability)
  are not part of validation — they're network calls that don't
  belong here.
- **Compensation validation.** `comp_min`, `comp_max`, `comp_currency`
  are validated as a group, all optional. When provided, `comp_min`
  and `comp_max` must be non-negative and `comp_min <= comp_max`;
  `comp_currency` must be one of `{INR, USD, EUR, AUD}`. A single
  `ErrInvalidCompensation` covers all three failure modes — frontends
  render the validator's message verbatim, which names the specific
  problem. Split into finer sentinels later if a frontend needs to
  branch on the sub-cause. `comp_equity` stays free-form (ADR 0001
  Notes); no validator.
- **Deadline-in-past check.** Optional field. When provided, the
  validator compares against today in UTC; deadlines equal to today
  pass. Most postings don't carry a deadline, so timezone precision
  isn't worth the cross-frontend coordination cost.
- **Tags (`tech_tags`, `custom_tags`).** No allowed-set check; tags
  are free-form by design (ADR 0001). Only a per-tag non-empty check.
- **Interview rounds.** `round IN (...)` is a CHECK constraint per
  ADR 0001; mirrored. `scheduled_at` future-date check is
  intentionally skipped — recording a past interview after the fact
  is a real workflow.
- **Event-time fields.** `first_seen_at` and `EventID` are not set
  by the Publisher and not validated here. They are populated by
  `internal/store` at the moment of the Postgres write, which is the
  point at which "first seen" is meaningful. The Publisher and the
  validator stay agnostic to them.
- **TUI error behavior.** Validation errors block form submission:
  the form stays open until the user corrects the input or cancels.
  A red banner above the form renders the validator's message, and
  focus jumps to the offending field. No override.
