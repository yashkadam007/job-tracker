# ADR 0001 — Richer Postgres schema and Kafka event contracts

## Issue

The current schema captures only what the CLI needed to publish a job and
fire a reminder: URL, title, company, status, and two timestamps. There is
no place for the posting metadata a real job-search workflow accumulates
(work mode, location, compensation, seniority, source, tech tags,
description, deadline), no place for application details (resume/cover
version, referral, recruiter contacts), no place for the interview
pipeline a job moves through, and no place for personal scaffolding
(priority, custom tags, free-text notes).

It also can't answer the analytics questions the user actually asks
of a job tracker — conversion ratios (`applied → interview`,
`interview → rejected`), per-company counts, weekly throughput,
median time-in-status. Those questions need *transitions over time*,
not just the latest status, and the current `jobs.status` column
discards the transition history.

ADR 0003 (Telegram bot) and ADR 0004 (macOS TUI) are queued behind this
decision: both want to display and edit interviews, notes, and posting
metadata. Building them against the current schema would force either a
follow-up rewrite or ad-hoc columns added one at a time. Better to land
the schema once, before two new frontends grow against it.

There is no production data and no compatibility constraint.

## Decision

Redesign `internal/db/schema.sql` and `internal/events/events.go` in one
pass.

**Schema, additive:**

- `jobs` PK is a producer-generated `job_id` (text, UUID at the
  producer). `url` stays on `jobs` as `NOT NULL UNIQUE` so dedup and
  external lookup still work, but it's no longer the identity column.
  Postings rename and redirect; `job_id` doesn't.
- Grow `jobs` with typed columns for posting metadata, compensation,
  application details, and the personal layer (priority, custom tags).
  Tags are `text[]` with GIN indexes; enums are `text` columns with
  `CHECK (col IN (...))` constraints.
- Add `job_status_history` — every transition with `(job_id, status,
  changed_at, event_id)`. `jobs.status` becomes the denormalised cache
  of the latest row. This is the source of truth for time-based
  analytics.
- Add `job_interviews` — 1:N child of `jobs`, keyed by a
  producer-generated `interview_id` so a later `complete` event
  upserts the same row as the earlier `schedule` event. FK to
  `jobs(job_id)`.
- Add `job_notes` — append-only timeline per job. FK to `jobs(job_id)`.
- Add FK + `ON DELETE CASCADE` from `reminders.job_id` to
  `jobs(job_id)`. Shape change called out: `reminders` previously had
  no FK, and previously referenced `url`.
- `processed_events` is unchanged.

**Events, additive:**

- Every job-scoped event carries `job_id` as the identity. The CLI
  generates it on `add` and prints it; follow-up commands take
  `--job-id` explicitly (mirroring the existing `--interview-id`
  pattern). `job_id` is also the Kafka partition key, so all events
  for a given job land on the same partition in order.
- `JobSubmitted` grows to carry `job_id` and all the new submit-time
  fields. URL is still carried as descriptive metadata. Field names
  match column names (snake_case both sides).
- `JobStatusChanged` carries `job_id` (replaces `url`).
- New topic `job.note.added` with `JobNoteAdded` keyed by `job_id`.
- New topic `job.interview.recorded` with `JobInterviewRecorded` keyed
  by `job_id`. One topic handles both "scheduled" and
  "completed/updated" — the store upserts on `interview_id`.
- `JobReminder` carries `job_id` (identity) plus `url` (denormalised
  for the Notifier's display).

Indexes are added only where an analytics query needs them: company,
`(status, work_mode)`, `(status, first_seen_at)`, GIN on each tag
array, `(job_id, status, changed_at)` and `(status, changed_at)` on
the history table, partial index on `job_interviews.scheduled_at`.
`jobs.url UNIQUE` gives a free btree index for URL-based dedup.

## Status

Implemented.

## Group

Foundational / Schema and contracts.

## Assumptions

- No production data exists; `schema.sql` is re-applied on startup with
  `CREATE IF NOT EXISTS`. Greenfield rewrite is safe.
- Single operator. No multi-tenant columns, no row-level security.
- JSON-over-Kafka stays. No Avro/Protobuf.
- The set of statuses, interview rounds, and sources is stable enough
  for `CHECK` constraints. Adding a value is a one-line `schema.sql`
  edit; that's an acceptable churn rate.
- Two new frontends (Telegram bot, TUI) are coming and both will want
  to read/write the new tables. The schema must serve them, not just
  the CLI.
- Analytics queries are run interactively (psql, a `report` subcommand)
  and need to be fast on a single-author dataset — hundreds to low
  thousands of rows, not millions. Indexes are sized for usefulness,
  not for scale.

## Constraints

- Typed columns and new tables only. No `jsonb` escape-hatch columns.
- Enums via `CHECK (col IN (...))`, not `CREATE TYPE` — keeps
  `schema.sql` idempotent and editable.
- Multi-valued fields use `text[]` *or* a join table, picked per field
  with a justification (see Notes).
- Job identity is `job_id` (producer-generated text/UUID), not `url`.
  Every child-table reference to `jobs(job_id)` is a real foreign key
  with `ON DELETE CASCADE` unless there's a reason otherwise. URL
  stays as `NOT NULL UNIQUE` on `jobs` for external lookup and dedup.
- Every new index is justified by a named analytics query.
- At most two new event topics. Both are justified below; no more are
  added.
- Event field names match Postgres column names.
- Event payloads describe what happened, never what should happen next.
- `processed_events` and `reminders` shape changes are explicitly
  called out: `reminders` gains an FK and now references `job_id`
  instead of `url`.

## Positions

Alternatives considered:

1. **Typed columns + transitions log + small set of child tables**
   (this decision).
2. **`jsonb` columns on `jobs` for posting metadata and personal
   layer.** Rejected — schemaless columns defer the problem, lose
   `CHECK` constraints, and make analytics queries awkward. The
   constraint list rules them out for good reason.
3. **Single denormalised `jobs` table with no transitions log.**
   Rejected — current status alone can't answer `applied → interview`
   conversion or median time-in-status. The schema would force every
   analytics query to scan event logs in Kafka.
4. **`CREATE TYPE ... AS ENUM` for statuses, rounds, sources.**
   Rejected — `ALTER TYPE ... ADD VALUE` doesn't compose with the
   "re-run `schema.sql` on startup" deploy model. `CHECK` constraints
   are trivially editable and only marginally slower in practice.
5. **Join tables for `tech_tags`, `custom_tags`, `interviewers`.**
   Rejected per-field — tags carry no metadata, never join to
   anything, and the only query is "jobs with tag X" which a GIN
   index handles cleanly. A join table would triple row count for
   zero gain.
6. **Three or more new event topics (separate
   `job.interview.scheduled` and `job.interview.completed`).**
   Rejected — both events carry the same payload shape and converge
   on the same row. A single upsert topic keyed by `interview_id` is
   simpler for both producer and consumer.
7. **Keep `url` as the primary key of `jobs`.** Rejected — URLs
   rename (recruiter portals rotate IDs, postings move domains,
   query-param normalisation drifts) and that movement would
   cascade through every child table and every event payload. A
   producer-generated `job_id` is stable for the job's lifetime;
   URL becomes a `NOT NULL UNIQUE` column that can be updated in
   place without losing notes, interviews, history, or pending
   reminders.

## Argument

The schema does three things at once and each pays for itself:

- **Typed columns make analytics queries trivial.** "Applied remote
  jobs" is `WHERE status='applied' AND work_mode='remote'`, served by
  one composite index. The same query in `jsonb` is a casted
  expression plus a custom index. Typed columns also force the CLI to
  validate inputs at the boundary, which is the right place.
- **The transitions log is the smallest unit that unlocks the time
  analytics.** `jobs.status` answers "what is it now?"; everything
  else (conversion ratios, weekly counts, median time-in-status)
  needs *when did it become that?*. `job_status_history` adds one
  table and two indexes; in exchange it answers every time-based
  query in the analytics target list with a single SQL statement.
- **Two new event topics — no more.** `job.note.added` and
  `job.interview.recorded` cover the two new entities that have
  their own lifecycle independent of status. Notes are append-only;
  interviews are upserted by id. Anything else (priority changes,
  tag edits) is rare enough to be a submit-time field today, and can
  grow into a `job.edited` event later if it becomes hot. Building
  that event speculatively now would add producer/consumer code
  without a real user story.

The cost is moderate: a `schema.sql` rewrite, a `cmd/cli` reshape to
publish the new fields and the two new events, two new handlers in
`internal/store`. None of it is structurally hard. All of it is a
strict prerequisite for ADRs 0003 and 0004 carrying their weight.

Doing it before the two new frontends ship is much cheaper than doing
it after, when three places would need to update at once.

## Implications

- `cmd/cli` grows new flags on `submit` (`--work-mode`, `--seniority`,
  `--source`, `--tech-tag`, `--comp-min`/`--comp-max`/`--comp-currency`,
  `--resume`, `--cover-letter`, `--referral`, `--recruiter-*`,
  `--priority`, `--custom-tag`, `--description-file`, `--deadline`)
  and gains two new subcommands: `note add` and `interview
  schedule|update`. Each new subcommand maps to exactly one event.
- `internal/store` consumer:
  - `JobSubmitted` handler inserts into `jobs` *and* writes the
    initial `job_status_history` row using the submit's `event_id`.
  - `JobStatusChanged` handler updates `jobs.status` *and* writes a
    history row. `ON CONFLICT (event_id) DO NOTHING` keeps replays
    idempotent without touching `processed_events`.
  - Subscribes to two new topics; gains two new handlers (`note`
    insert with `ON CONFLICT (event_id)`, `interview` upsert with
    `ON CONFLICT (interview_id)`).
- `internal/scheduler` is unchanged.
- `internal/notifier` is unchanged — the bot/TUI ADRs will decide how
  notes and interviews surface in messages.
- `internal/jobclient` (per ADR 0002) gains read/publish methods for
  notes and interviews when those frontends need them. Out of scope
  for this ADR.
- Operational: `schema.sql` re-run on startup adds the new tables and
  indexes idempotently. No data migration step needed (greenfield).
- Cosmetic: `interview update` needs an `interview_id` to upsert
  against; the CLI either keeps a small local id→job map or queries
  Postgres to resolve it. See open question in Notes.

## Related decisions

- **ADR 0002** — Shared `internal/jobclient` library. Will gain
  Publisher methods for `JobNoteAdded` and `JobInterviewRecorded`,
  and Reader methods for notes/interviews lists.
- **ADR 0003** — Telegram bot. Depends on these schemas to render
  pipeline state and accept note/interview updates from chat.
- **ADR 0004** — Desktop TUI. Same dependency, with richer rendering
  surface.

## Related requirements

- Capture enough metadata per job that the tracker is worth using
  three months in, not just three days in.
- Answer the standard "how's my search going?" questions in one SQL
  query, not a script.
- Land the contract once, before two new frontends ossify against the
  current minimal one.
- Keep event payloads descriptive ("this happened"), not
  prescriptive ("do this next").

## Related artifacts

- `internal/db/schema.sql` (rewritten).
- `internal/events/events.go` (extended; new topics, new structs).
- `cmd/cli/main.go` (new flags, two new subcommands).
- `internal/store/` (two new handlers, history-row writes on submit
  and on status change).
- `internal/jobclient/` (future: publisher/reader methods for notes
  and interviews — out of scope for this ADR).

## Related principles

- **Event-driven core; events describe what happened.** The two new
  events are records, not commands. The store decides what to write;
  the event doesn't tell it to.
- **No business logic in the schema or in events.** `CHECK`
  constraints validate; they don't compute. Status transitions stay
  the domain of whoever issues the change.
- **Typed columns over schemaless escape hatches.** Push validation
  to the boundary; keep the query plan obvious.
- **No service writes to another's tables.** `internal/store` owns
  the new tables; frontends mutate via events.

## Notes

- **Multi-valued field choices, justified per field:**
  - `jobs.tech_tags` — `text[]` + GIN. Only query is "jobs with tag
    X"; tags carry no metadata.
  - `jobs.custom_tags` — `text[]` + GIN. Same reason; kept separate
    from `tech_tags` so analytics can filter on tech without dragging
    in personal tags like `dream_company`.
  - `job_interviews.interviewers` — `text[]`. Display-only; no
    analytics on interviewer name. A join table would add a third
    table for purely cosmetic data.
- **Compensation flattened, not nested.** Columns `comp_min`,
  `comp_max`, `comp_currency`, `comp_equity`, `comp_bonus` sit on
  `jobs` to satisfy the "event field name == column name" rule.
  Nesting under `compensation` in the event would have broken that
  symmetry for no payoff.
- **`comp_equity` is `text`, not numeric.** Real postings express
  equity as `0.1%`, `5000 RSU`, `$80k value`, etc. A numeric column
  would force the CLI to pick one normalisation; free-form is
  honest.
- **`job_status_history.event_id UNIQUE` is the idempotency key for
  history rows.** Replays insert nothing on conflict. This is
  deliberately separate from `processed_events`, which dedupes at
  the *consumer* level; the unique constraint dedupes at the *row*
  level, which is cheaper inside a single insert.
- **`job_interviews` PK is `interview_id` (producer-generated text),
  not `bigserial`.** This makes upsert-on-id natural and avoids a
  lookup-by-(url, round, scheduled_at) heuristic when "complete the
  interview" arrives separately from "schedule the interview".
- **`jobs.job_id` is producer-generated text (UUID), not
  `bigserial`.** Same reasoning as `interview_id`: the producer
  decides the id at the moment of creation, the CLI prints it, and
  follow-up commands quote it back. A `bigserial` PK would force a
  round-trip to Postgres on `add` just to learn the id, which
  contradicts the "CLI doesn't talk to Postgres" property. The cost
  is a slightly longer key on disk; the win is that the producer
  and every subsequent event already know the id without a lookup.
- **`jobs.url` is `NOT NULL UNIQUE`, not nullable.** Every job in
  v1 has a URL; nullability would only earn its keep if "no URL"
  postings became common (recruiter-only leads, say). Easy to relax
  later if that workflow shows up. The UNIQUE constraint preserves
  the existing dedup behaviour at submit time — a re-submit with
  the same URL but a different `job_id` will fail loudly rather
  than silently creating a duplicate job.
- **No `job.edited` event yet.** Most posting metadata is captured
  at submit time and changes rarely. If it becomes a frequent
  workflow, add `JobEdited` later as a sparse-field event the store
  applies as `UPDATE ... SET col = COALESCE($n, col)` — and that
  same event is the natural carrier for a URL rename, which is the
  main reason `url` is mutable in the new schema. Speculative now.
- **Open question:** how should `cli` discover ids (`job_id`,
  `interview_id`) for follow-up commands? v1 answer: print them on
  the creating command and require the user to quote them back. The
  longer-term options remain (a) local file cache written by the
  creating command; (b) `cli` reads Postgres directly. (b) matches
  the bot's `/list` precedent in ADR 0003. Revisit when either
  frontend lands.
- **Open question:** "ended in rejected" in the conversion metric is
  modelled as "a `rejected` transition exists after the `interview`
  transition." If the user prefers "current status is `rejected`,"
  the query simplifies but loses jobs that bounced from interview
  to rejected and then back to applied (rare, but possible). Confirm
  before wiring a report.
