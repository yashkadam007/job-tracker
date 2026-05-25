# ADR 0007 — Notifier-side dedup for `job.reminder` via `processed_events`

## Issue

`fireDue` in `cmd/scheduler/main.go` publishes a `JobReminder` to
Kafka and then marks the row fired in Postgres as two separate,
non-atomic steps:

```
ProduceSync(rec)       // 1. Kafka acks
…
sch.MarkFired(id, now) // 2. Postgres UPDATE
```

If the scheduler process dies, loses DB connectivity, gets SIGTERMed
during graceful shutdown, or panics between (1) and (2), the row
stays `fired_at IS NULL`. The next ticker pass calls `FetchDue` again,
sees the same row, and publishes a second Kafka record with the same
deterministic `event_id` (`"reminder-<id>"`).

The comment in `fireDue` says this is intentional and that "the
Notifier's own ledger turns it into a no-op." This comment is wrong.
`cmd/notifier/main.go` is a plain auto-commit consumer with no
`processed_events` writes, no in-memory event-ID set, and no Postgres
connection at all. Its own comment acknowledges that one duplicate
notification per notifier crash is the accepted worst case — but that
is a separate window from the scheduler's, and neither is actually
deduped today.

Net effect: routine scheduler restarts (deploys, OOMs, container
reschedules) produce duplicate Telegram messages, in contradiction of
the system's stated at-least-once-with-dedup semantics.

Related observations:

- `processed_events(consumer, event_id, processed_at)` already exists
  in `internal/db/schema.sql`. `cmd/store` and `internal/bot` both
  claim into it via `db.ClaimEvent` (transactional) and a direct
  `pool.Exec` (non-transactional), respectively.
- The dedup key is the event ID, not the reminder row ID. Event IDs
  are already deterministic at the Scheduler — `"reminder-<id>"` — and
  available on the `JobReminder` event itself.
- `reminders.job_id` has `ON DELETE CASCADE`. A column on `reminders`
  could disappear if a job is deleted while a duplicate record is
  still pending in Kafka.

## Decision

The Notifier claims each `event_id` in `processed_events` (consumer =
`"notifier"`) before delivering. Claim-then-deliver. If the claim
returns "already processed," the Notifier skips delivery and lets the
Kafka offset advance.

Concretely:

- `cmd/notifier/main.go` connects to the Postgres pool on startup
  (`db.Connect`), same as `cmd/scheduler` and `cmd/bot`.
- For each consumed `JobReminder`, the Notifier executes
  `INSERT INTO processed_events (consumer, event_id) VALUES
  ('notifier', $1) ON CONFLICT DO NOTHING`. If `RowsAffected() == 0`,
  the event is a duplicate; skip.
- Auto-commit on the consumer client stays. The dedup table is the
  source of truth for "did we already deliver this." A Kafka offset
  that advances past a not-yet-delivered record is acceptable because
  the next scheduler republish will be deduped, not redelivered.
- The misleading comment in `cmd/scheduler/main.go`'s `fireDue` is
  corrected to reference `processed_events` instead of an imaginary
  notifier ledger.

No schema change. No new table. No new service. No outbox.

## Status

Implemented.

## Group

Reliability / Event delivery semantics.

## Assumptions

- `processed_events` is the right home. It's already the ledger for
  every other consumer in the system; using it for the Notifier keeps
  one idiom across services.
- Notifier–Postgres availability is acceptably coupled. The Notifier
  already depends on Kafka and (optionally) Telegram. Adding Postgres
  is a third dependency, but it's the same database the rest of the
  system uses, so a Postgres outage already stops the system; the
  Notifier failing in lockstep is not a new operational mode.
- A Notifier crash *between* a successful claim commit and a
  successful Telegram send produces a missed reminder rather than a
  duplicate. This window (~one Telegram round-trip, hundreds of ms)
  is acceptable in exchange for eliminating the much larger
  scheduler-crash window.
- Telegram send failures (network blip, 429, bot revoked) after a
  successful claim mean the message is dropped. This matches today's
  behaviour (the current code logs `telegram: %v` and moves on) and is
  the same trade-off `cmd/bot` already makes.

## Constraints

- No new tables. Reuse `processed_events`.
- No outbox. No two-phase commit. The "smallest, simplest" change that
  makes the system match its own stated semantics.
- The consumer namespace is `"notifier"`. Event IDs from `job.reminder`
  collide with no other consumer's keys because `processed_events` is
  keyed `(consumer, event_id)`.
- The claim happens *before* delivery. Deliver-then-claim does not
  fix the scheduler-crash bug — the second Kafka record arrives,
  notifier delivers, *then* discovers the claim conflict; the
  duplicate has already been sent.
- The Notifier remains single-purpose: deliver job-related
  notifications. The dedup is an internal correctness mechanism, not
  a new responsibility.

## Positions

Alternatives considered:

1. **Notifier claim into `processed_events`** (this decision).
2. **Add `notified_at` column to `reminders`.** Rejected.
   - Cross-service write coupling: the Notifier would now UPDATE a
     table owned by the Scheduler.
   - Dedup key would have to be reverse-engineered from the event ID
     (`"reminder-<n>"` → integer), creating a leaky coupling to the
     Scheduler's ID scheme. A future `job.reminder` producer (manual
     fire from the bot, replay tool) would break the parse.
   - `reminders.job_id` has `ON DELETE CASCADE`; deleting a job
     deletes the dedup record. A duplicate Kafka message still in a
     partition could then re-deliver for the deleted job.
   - Single-consumer by construction. A future second consumer of
     `job.reminder` would need its own column.
3. **Mark-fired-then-publish in the Scheduler.** Rejected. Inverts
   the failure mode from over-delivery to under-delivery. A crash
   between mark and publish results in a row that's marked fired but
   was never sent — strictly worse for a reminder system. Would also
   need a sweeper for unpublished-but-fired rows.
4. **Transactional outbox.** Rejected for v1. The canonical fix, but
   requires a new outbox table, a relay loop, and a non-trivial
   refactor of `fireDue`. Overkill for one event type. Re-evaluate if
   a second produce-and-update flow appears in the system.
5. **Document the duplicates, change nothing.** Rejected. The
   misleading comment in `fireDue` is fixable for free; the
   underlying duplicate is fixable with one INSERT. Accepting the
   bug is harder to justify than fixing it.
6. **Switch the Notifier to manual commit.** Rejected as a separate
   change. Functionally equivalent under claim-then-deliver: a crash
   between claim and send misses the message either way (the claim
   row already says "processed"). Manual commit adds code without
   buying additional correctness.

## Argument

- **The fix matches the stated semantics.** The scheduler's own
  comment claims dedup-by-event-id at the consumer. This ADR makes
  that true rather than rewriting the comment to match a worse
  reality.
- **The infrastructure already exists.** `processed_events` is there,
  the pattern is used by `cmd/store` and `internal/bot`, and event
  IDs are already deterministic. The change is one INSERT and a
  conditional skip.
- **Claim-then-deliver picks the right failure mode.** The original
  bug produces *duplicates* — a frequent, visible, noisy failure
  triggered by routine deploys. The new failure mode is a missed
  reminder in a narrow window around a Notifier crash mid-send. The
  Notifier crash is rarer than a Scheduler restart, the window is
  smaller, and a missed reminder is no worse than a Telegram delivery
  failure (which the code already silently accepts).
- **No cross-service writes.** The Notifier writes only to its own
  rows in `processed_events`. The Scheduler keeps full ownership of
  `reminders`.
- **Multi-consumer ready.** Any future `job.reminder` consumer
  (analytics, webhook bridge, second bot) gets its own dedup space
  via the `(consumer, event_id)` key.

## Implications

- `cmd/notifier/main.go`:
  - Imports `job-tracker/internal/db`.
  - On startup: `pool, err := db.Connect(ctx, dsn())`. Reads
    `DATABASE_URL` the same way `cmd/scheduler` does.
  - A `claim(ctx, pool, eventID) (bool, error)` helper performs the
    INSERT and returns `RowsAffected > 0`.
  - `deliver` is gated by the claim. Missing or empty `event_id` on
    an incoming `JobReminder` is treated as a decode-class error:
    log and skip (the producer is expected to set it; ADR 0005's
    producer-side validation should already cover this).
  - The auto-commit comment is updated to reflect the new reality
    (dedup via `processed_events`, not "naturally idempotent enough").
- `cmd/scheduler/main.go`:
  - The comment above `fireDue` is corrected to reference
    `processed_events` instead of a non-existent notifier ledger.
- `internal/db/schema.sql`: no change. `processed_events` already
  defined at line 139.
- `docker-compose.yml`: no change. Notifier already runs in the
  same network as Postgres; it just hasn't been connecting.
- No new env vars, no new ports, no new topics, no new tables.

## Related decisions

- **ADR 0001** — Establishes `processed_events` as the dedup
  primitive for this system.
- **ADR 0005** — Producer-side validation ensures `event_id` is
  non-empty on `JobReminder`. The Notifier trusts this; a missing
  `event_id` is treated as a decode-class skip.
- **ADR 0006** — Consumer error classification. The Notifier does
  not yet adopt the full classification scheme; that's out of scope
  for this ADR. A missing `event_id` or a DB failure during claim
  follows the same minimal "log and continue" path the Notifier
  already uses for decode errors.

## Related requirements

- `job.reminder` must be delivered at-least-once with consumer-side
  dedup, as the rest of the system already documents.
- Routine Scheduler restarts must not produce duplicate user-facing
  Telegram messages.
- The Notifier remains a single-purpose, side-effect-only service
  whose only durable state is the dedup ledger.

## Related artifacts

- `cmd/notifier/main.go` — DB pool, claim helper, gated `deliver`.
- `cmd/scheduler/main.go` — corrected `fireDue` comment.
- `internal/db/schema.sql` — unchanged; `processed_events` already
  present.

## Related principles

- **Match the comments to the code.** Either fix the comment or fix
  the code; never let them drift.
- **Reuse existing primitives.** `processed_events` is the dedup
  ledger. New event types use it; they don't grow new columns on
  unrelated tables.
- **Pick the failure mode you can live with.** A rare missed
  reminder in a sub-second window beats a routine duplicate on every
  deploy.

## Notes

- **Missing `event_id`.** Treated as a decode-class skip: log the
  topic/partition/offset and continue. Should not happen in practice
  given ADR 0005's producer-side validation; the check exists so a
  future producer bug fails loudly instead of crashing on the INSERT.
- **DB unavailable during claim.** Today the Notifier would log and
  continue (no DB to fail against). With this change, a DB outage
  causes the claim to error; we treat that as a transient failure
  and skip the delivery. Worse than today for that specific record,
  but the system as a whole is already unavailable when Postgres is
  down (Scheduler can't mark fired, Bot can't process callbacks).
  Aligning failure modes across services is fine.
- **No change to `MarkFired` semantics.** The Scheduler still marks
  rows fired after a successful publish. The dedup ledger is the
  Notifier's invariant, not the Scheduler's.
- **Open question:** should the Notifier adopt the ADR 0006
  classification scheme (typed sentinels, retry-infra, skip counter,
  admin HTTP endpoint)? Probably yes eventually, but it's a separate
  ADR. This one fixes a specific data-flow bug with the minimum
  change.
