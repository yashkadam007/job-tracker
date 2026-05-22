# ADR 0002 — Shared `internal/jobs` library for frontends

## Issue

`cmd/cli` is currently the only frontend that publishes domain events to
Kafka. Its `publish()` helper opens a fresh Kafka client per call,
hard-codes topic names, partition key, ack policy, and JSON marshalling.

ADR 0003 (Telegram bot) and ADR 0004 (macOS TUI) will both
need to publish the same events and read jobs from Postgres. Three
copies of the same wiring across three frontends would diverge quickly —
e.g., one updating ack policy, another forgetting to set the partition
key — and would obscure the shared contract between the frontends and
the event-driven core.

A small shared library is needed before we add the next two frontends,
not after.

## Decision

Introduce a new package `internal/jobs/` exposing two narrow types:

- **`jobs.Publisher`** wraps a long-lived `kgo` client and provides:
  - `Submit(ctx, JobSubmitted) error`
  - `ChangeStatus(ctx, JobStatusChanged) error`

  Owns topic names, partition key (URL), ack policy
  (`AllISRAcks`), and JSON encoding. Frontends construct one Publisher
  at startup and reuse it.

- **`jobs.Reader`** wraps a `pgxpool.Pool` and provides:
  - `List(ctx, ListFilter) ([]Job, error)` — optional status filter,
    sort, limit.
  - `Get(ctx, url) (Job, error)`
  - `PendingReminders(ctx) ([]PendingReminder, error)` — `LEFT JOIN`
    of `jobs` and unfired/uncancelled `reminders`.

  Read-only. No event-handling, no writes.

`cmd/cli` is rewritten to use `jobs.Publisher`. `cmd/bot` (per ADR 0003)
and `cmd/tui` (per ADR 0004) use both.

`internal/store` and `internal/scheduler` are unchanged — they own the
write-side persistence for their respective Kafka consumers, a different
concern from the read/publish path needed by frontends.

## Status

Proposed.

## Group

Refactoring / Code organization.

## Assumptions

- The set of frontends will grow. CLI alone wouldn't justify this; CLI
  - bot + TUI does.
- Frontends never write to Postgres directly — every mutation flows
  through Kafka events. `Reader` is therefore read-only by design.
- The DB queries needed by frontends are simple (list/get/join);
  no need to introduce a query builder or ORM.
- Event schemas in `internal/events/` are stable enough (after
  ADR 0001's redesign) that the Publisher API can be a thin
  pass-through.

## Constraints

- No business logic in `internal/jobs/`. Anything that smells like a
  decision (computing next reminder time, deciding whether a transition
  is legal, etc.) stays in `internal/scheduler` or `internal/store`.
- `Publisher` must own Kafka client lifecycle (`Close()`); frontends
  must `defer pub.Close()`.
- `Reader` must not expose `*pgxpool.Pool` to callers — the package
  owns the connection abstraction so the SQL surface stays inside the
  package.
- No new external dependencies. Use existing `kgo`, `pgx`.

## Positions

Alternatives considered:

1. **New `internal/jobs/` package** (this decision).
2. **Add helpers to `internal/events/`.** Rejected — `events` is the
   schema layer; mixing transport (Kafka client setup) into it conflates
   two different concerns.
3. **Per-cmd `cmd/<x>/internal/`.** Rejected — defeats the point of
   sharing.
4. **Add publish methods to `internal/store/`.** Rejected — `store` is
   the Store consumer's write side; making it produce events to itself
   creates a confusing dependency loop in the codebase.
5. **Do nothing; let new frontends copy `cli.publish`.** Rejected —
   the very reason this ADR exists.

## Argument

- Single source of truth for "how to publish a job event": ack policy,
  partition key, topic name, timeouts. Changes apply to every frontend
  at once.
- Single source of truth for "how to read jobs from Postgres":
  predictable list filters, predictable joins with `reminders`.
- Discoverability: a new contributor opens `internal/jobs/reader.go`
  and sees the read API in one screen.
- Cost is low (~2 hours of mostly mechanical extraction), and the work
  is a strict prerequisite for ADRs 0001 and 0003 — better to do it
  once, cleanly, before two new frontends grow against a stale
  pattern.
- Keeps the "thin frontend" principle: each cmd/ is mostly UX + glue,
  with shared infrastructure in `internal/`.

## Implications

- `cmd/cli/main.go` reorganises: a top-level `Publisher` is constructed
  in `main()` (or per subcommand) instead of per `publish()` call.
  Behaviourally identical; structurally cleaner.
- `cmd/bot` (ADR 0003) and `cmd/tui` (ADR 0004) get to depend on the
  same package, with no copy/paste.
- Adds `DATABASE_URL` as a config requirement for any frontend that
  uses `Reader`. That's already a familiar pattern from
  `cmd/scheduler` and `cmd/store`.
- `Publisher` being long-lived means resource cleanup matters; ensure
  `Close()` is called on shutdown signals.
- Unit tests become straightforward — `Reader` against a test
  Postgres, `Publisher` against a mock or test cluster. Tests are out
  of scope for v1 but the refactor doesn't preclude them.
- `internal/store` and `internal/scheduler` continue to import
  `internal/events` directly (they don't need the Publisher abstraction
  because they're consumers, not producers).

## Related decisions

- **ADR 0001** — Richer schema and event contracts. Strict
  prerequisite; this library's Publisher/Reader are built against
  the redesigned schema and event set.
- **ADR 0003** — Telegram bot. Depends on `jobs.Publisher` and
  `jobs.Reader`.
- **ADR 0004** — macOS TUI. Depends on both.

## Related requirements

- DRY across frontends.
- Easy onboarding for the next frontend without code duplication.
- Preserve event-driven separation of read and write paths.

## Related artifacts

- `internal/jobs/publisher.go` (new)
- `internal/jobs/reader.go` (new)
- `internal/jobs/jobs.go` (new — shared types: `Job`, `ListFilter`,
  `PendingReminder`)
- `cmd/cli/main.go` (rewritten to use `Publisher`)
- `internal/events/events.go` (unchanged; consumed by the new package)
- `internal/db/` (unchanged; `Connect` reused)

## Related principles

- **Event-driven core; events are the contract.** The Publisher
  doesn't define new schemas — it forwards existing ones.
- **Thin frontends, shared infrastructure.**
- **No service writes to another's tables.** `Reader` is read-only;
  writes happen via events consumed by the Store/Scheduler services
  that own the corresponding tables.

## Notes

- Naming: the package `jobs` shares its name with the `jobs` Postgres
  table. If readers find this confusing, rename to `jobsclient` or
  `jobsapi`. v1: keep `jobs`.
- `Publisher` defaults: `AllISRAcks`, `ProducerLinger(0)` (matches CLI
  today). For interactive frontends (TUI), linger 0 is correct —
  responsiveness over batching.
- `ListFilter` minimum surface: `Status *events.JobStatus`,
  `Limit int`, `OrderBy string` (allowlist: `last_event_at`,
  `first_seen_at`). Extend on demand.
- `PendingReminders` returns at most one reminder per URL (the
  earliest unfired/uncancelled). Frontends rarely need the full
  reminder history.
- A future read API (HTTP) can wrap `Reader` 1:1 without changing
  consumers. Worth keeping in mind when shaping method signatures.
