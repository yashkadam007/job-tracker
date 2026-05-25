# ADR 0009 — Schema migrations via `golang-migrate` and a root `Makefile`

## Issue

`internal/db/db.go` embeds `schema.sql` and runs it on every `Connect`
call. Every service that connects to Postgres — `store`, `scheduler`,
`bot`, `notifier`, `jobtracker` — re-applies the full schema on
startup. The header comment frames this as deliberate: every statement
is `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`, so the
apply is idempotent for additive changes.

That framing only holds while schema changes are additive. The current
strategy has no concept of "previous version" vs. "current version":

- A column rename is two operations (drop old, create new) that can't
  be expressed as one idempotent statement. Editing `schema.sql` in
  place either drops the column on every startup (data loss) or leaves
  the rename unrepresented.
- A constraint tightening (`CHECK`, `NOT NULL`, narrower enum) can't
  be re-applied to existing rows by `CREATE TABLE IF NOT EXISTS` —
  the second-and-subsequent apply is a no-op even if the constraint
  drifted.
- A column removal would have to be expressed as a separate
  `ALTER TABLE` outside the embedded SQL, with no record of whether
  it has been applied.
- Every service races to apply on boot. The five-way concurrent DDL
  works today only because the statements are all `IF NOT EXISTS`;
  any real DDL would deadlock or duplicate work.

Until now this was acceptable: the project was greenfield, no
production data existed, and the schema header explicitly relied on
"re-running on a clean database reproduces the full schema." That
assumption no longer holds — there is production data on the home
server that we don't want to lose, and the next schema change will
either need to be additive-only or work around the fact that
`schema.sql` is the only artifact describing the database.

The `internal/jobclient/validate_test.go` drift detector parses
`db.SchemaSQL` to assert that `CHECK (col IN (…))` constraints match
the in-process `events.Allowed*` slices. It is the only reader of
`db.SchemaSQL` outside `Connect` itself.

A separate but related friction: common operator tasks (`docker
compose up`, `go test ./...`, "ensure topics", and now migrations)
are unwritten — every contributor reconstructs the commands from
`compose.yml` and the README. The introduction of a migration
workflow is the right moment to land a `Makefile` as the single
discoverable entry point for the local dev loop.

## Decision

Adopt the same migration pattern as the
`carers-academy-backend` repo: the standalone `golang-migrate`
CLI driven from a root `Makefile`, with a timestamped pair of
`*.up.sql` / `*.down.sql` files per migration.

Concretely:

1. **Migrations directory.** `internal/db/migrations/` replaces
   `internal/db/schema.sql`. Files are named
   `YYYYMMDDHHMMSS_<slug>.up.sql` and `YYYYMMDDHHMMSS_<slug>.down.sql`.
   The first pair is `<timestamp>_init.up.sql` containing today's
   `schema.sql` verbatim and `<timestamp>_init.down.sql` dropping
   every table in reverse dependency order.

2. **Runner.** `golang-migrate` is invoked as the
   `docker.io/migrate/migrate:latest` Docker image, with a fall-back
   to a locally-installed `migrate` binary if present. The CLI talks
   to Postgres directly; no Go code links the migration library. The
   `schema_migrations` table (created and owned by `golang-migrate`)
   is the source of truth for "what version is the DB on."

3. **`Connect` stops applying schema.** `db.Connect` becomes a plain
   `pgxpool.New` wrapper. Services no longer run DDL on startup.
   Booting a service against a DB that has not been migrated is
   expected to fail at the first query — the operator's signal to
   run `make migrate-up`.

4. **Prod bootstrap.** The existing production database already has
   today's schema. To adopt migrations without re-running DDL, we
   run `make migrate-force VERSION=<init-timestamp>` once on prod.
   That inserts a row into `schema_migrations` recording 0001 as
   applied without executing the file. Fresh dev databases run
   `make migrate-up` from zero and get the full schema applied
   normally.

5. **Makefile.** A root `Makefile` lands with these target groups:
   - **Migrations**: `migrate-up`, `migrate-up-one`, `migrate-down-one`,
     `migrate-down-all`, `migrate-version`, `migrate-force`,
     `migrate-new`. Copied in shape from `carers-academy-backend`'s
     Makefile and adjusted for this repo's compose service names and
     env conventions.
   - **Build & test**: `build`, `test`, `fmt`, `vet`, `tidy`.
   - **Compose lifecycle**: `up`, `down`, `logs`, `restart`, `ps`.
   - `.env` is loaded if present (compose-style key=value), so the
     same `DATABASE_URL` / `KAFKA_BOOTSTRAP` overrides work across
     `docker compose` and `make`.

6. **Drift detector.** `internal/jobclient/validate_test.go` is
   deleted. The `CHECK (col IN (…))` ↔ `events.Allowed*` invariant
   is now enforced by the database itself: any producer that emits a
   value outside the allowed set fails at INSERT time in any
   integration test (or in production) that exercises the consumers.
   The pure-unit drift detector earned its keep when `schema.sql`
   was the only schema artifact in the repo; with N migration files
   accruing over time, regex-parsing all of them just to repeat what
   Postgres enforces is not worth the maintenance burden. `db.SchemaSQL`
   is removed; nothing else reads it.

## Status

Proposed.

## Group

Infrastructure / Data layer.

## Assumptions

- **Production data exists and must survive.** The home-server
  Postgres has accumulated real job history since the project went
  live. Any migration strategy that risks dropping or rewriting that
  data on startup is unacceptable. This is the load-bearing change in
  premises since the original schema-on-Connect design.
- **`golang-migrate` is well-fit for this project.** It is the
  reference's choice, it is widely used, the file format is plain
  SQL (no DSL), the CLI is small, and the `schema_migrations` table
  is a single-column ledger. No buy-in beyond "the file naming
  convention is fixed."
- **CLI over library.** The reference repo uses the standalone CLI
  via Docker. Matching that pattern keeps the same idiom across
  projects on this machine and avoids pulling another Go dependency
  into every service binary. The CLI also gives us `force`, which
  is exactly what the prod bootstrap step needs and which the
  library API does not expose as cleanly.
- **`make migrate-up` is the only way migrations run.** No service
  applies migrations on startup. No compose dependency wires migrate
  in as a pre-service hook. The trade-off — operator runs one command
  before bringing services up on a fresh DB — is acceptable for a
  single-operator project and matches "explicit, visible operations
  over implicit self-healing" (the spirit of ADR 0008's "visible
  boundaries over implicit precedence").
- **Down migrations are written.** They cost little (the inverse of
  the up), enable `migrate-reset` for local dev, and force the author
  of each migration to think about reversibility. Operators are not
  expected to run them on prod casually.
- **`.env` is acceptable convention.** The reference uses it. This
  repo doesn't have one today, but `compose.yml` already references
  `${TELEGRAM_BOT_TOKEN:-}` and similar — operators are already
  setting env vars somewhere. Centralising them in a gitignored
  `.env` matches existing patterns elsewhere in the operator's
  workflow.

## Constraints

- **No data loss on the prod DB.** The bootstrap step must mark
  0001 as applied without re-executing it.
- **No new runtime dependency in the service binaries.** Services
  must not link `golang-migrate`. The migration runner is an
  operator tool, not part of the request path.
- **No behaviour change for additive-only schema work today.** A
  contributor adding a new index or column gets the same outcome:
  write the SQL, run `make migrate-up`, ship.
- **Migration files are immutable once merged.** Standard
  `golang-migrate` discipline: never edit an applied migration;
  write a new one to alter the world it created. The Makefile
  helper `migrate-new` enforces the timestamp convention.
- **The migrations directory is the schema source of truth.** No
  parallel `schema.sql` snapshot is regenerated. Reviewers reading
  a PR see the diff against the most recent migration file; reading
  the cumulative schema means reading the directory.
- **`internal/db` stays small.** `Connect` plus `ClaimEvent` plus
  the package doc. Migration logic does not live in Go code.

## Positions

Alternatives considered:

1. **Standalone `golang-migrate` CLI via Docker + Makefile**
   (this decision). Matches the reference; zero new Go deps; clean
   separation between operator workflow and service runtime; `force`
   available for prod bootstrap.

2. **`golang-migrate` library embedded in a `cli migrate` subcommand.**
   Originally recommended in the discussion that led to this ADR;
   rejected after looking at the reference. Embedding ties migration
   workflow to a Go rebuild, hides `force` behind a subcommand we'd
   have to design, and diverges from a working pattern the operator
   already uses elsewhere. The library is the right call if the same
   binary is being shipped to operators who can't run Docker; that
   isn't this project.

3. **`goose` instead of `golang-migrate`.** Comparable, supports Go
   migrations as well as SQL, single binary. Rejected purely for
   consistency with the reference; no functional reason to prefer
   one over the other for this project.

4. **Hand-rolled migration runner: numbered files + a
   `schema_migrations` table managed by `internal/db`.** Considered;
   gives full control and zero deps. Rejected because writing a
   transactional, lock-safe migrator correctly is more work than
   reusing one, and the reference shows the cost of the Docker-CLI
   pattern is low.

5. **Keep `schema.sql` + add `ALTER` statements appended at the bottom,
   gated by checks.** Considered briefly. Rejected — this is exactly
   the "no concept of version" failure mode the ADR opens with, just
   with more lines. There is no way to express "rename column X to Y"
   safely under `IF NOT EXISTS` semantics.

6. **Compose-level one-shot `migrate` service that runs before others.**
   Considered for `docker compose up` ergonomics. Rejected for v1 —
   adds compose complexity for a workflow `make up` can sequence
   explicitly (`make migrate-up && make up`). Revisit if the
   compose-only path becomes the primary one (e.g. if a CI deploys via
   `docker compose`).

7. **Keep the drift detector by embedding migrations and concatenating
   `*.up.sql` at test time.** Considered. Rejected — the test
   re-implements a small regex parser over SQL, and as migrations
   accrue, the cumulative output drifts from any single file the
   reviewer reads. Postgres enforces the constraint at write time;
   that is the test we actually want. Removing the in-process check
   is a small loss of unit-test speed against a real win in
   maintenance.

8. **Do nothing.** Rejected — the next non-additive schema change
   has no safe path under the current setup. The cost of fixing this
   *after* a data-loss incident is much higher than the cost of
   landing the runner now while the only schema change pending is
   the initial extraction.

## Argument

- **The bug is structural, not stylistic.** `schema.sql` works for
  greenfield projects and nothing else. We are no longer greenfield.
  Either we move to migrations now, or every future schema change
  becomes a manual `psql` session against prod with no record.

- **The reference pattern is already validated.** The Makefile
  shape, the migration file naming, the Docker-image runner, and the
  `.env` convention all come from `carers-academy-backend`, where
  they have been used in anger. Copying them keeps two repos on
  the same idiom and shortens the learning curve when context-
  switching.

- **Operator-explicit beats service-implicit.** Today every service
  silently mutates schema on every boot. After this ADR, schema
  changes happen exactly when an operator runs `make migrate-up`,
  on a DB they named explicitly via `DATABASE_URL`. The
  failure mode "I forgot to migrate" surfaces as a loud "column X
  does not exist" at first query — strictly better than "I
  accidentally migrated" surfacing as data corruption.

- **The `Makefile` is overdue.** Half of the friction around
  contributing to this repo today is "what's the command for X."
  Migrations need a Makefile; folding in `up`/`down`/`test`/`fmt`
  at the same time costs almost nothing and removes that friction
  for every other task.

- **The drift detector's removal is a deliberate trade.** It was a
  good check while `schema.sql` was the schema. With migrations,
  enforcing the same invariant in a unit test means re-reading
  every `*.up.sql` and concatenating them — a worse mirror of the
  real schema than just letting Postgres enforce it on insert. The
  integration tests already exercise consumers against a real
  Postgres; the invariant is covered.

## Implications

- **New files:**
  - `internal/db/migrations/<timestamp>_init.up.sql` — contents of
    today's `internal/db/schema.sql`, verbatim. Module-level header
    comment (greenfield, CHECK constraints, etc.) preserved.
  - `internal/db/migrations/<timestamp>_init.down.sql` — `DROP TABLE`
    in reverse dependency order: `reminders`, `processed_events`,
    `job_notes`, `job_interviews`, `job_status_history`, `jobs`.
  - `Makefile` at the repo root.
  - `.env.example` documenting `DATABASE_URL`, `KAFKA_BOOTSTRAP`,
    `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, the reminder/poll
    knobs, and `JOB_TRACKER_ADMIN_HOST`. `.env` itself stays
    gitignored.
  - `.gitignore` entry for `.env`.

- **Deleted files:**
  - `internal/db/schema.sql`.
  - `internal/jobclient/validate_test.go`.

- **Modified files:**
  - `internal/db/db.go` — remove the `//go:embed schema.sql` line,
    the `SchemaSQL` variable, and the `pool.Exec(ctx, SchemaSQL)`
    call inside `Connect`. Update the package and function doc
    comments. `ClaimEvent` and `ErrAlreadyProcessed` unchanged.
  - The reference in `internal/jobclient/reader.go:22` to
    `db.Connect → defer pool.Close()` stays accurate; only the
    Connect body changes.
  - `README.md` — short section pointing to `make` targets and
    explaining the migration workflow.
  - `docs/runbook.md` — entry for "first-run on a fresh DB",
    "applying a new migration", and "bootstrapping migrations on a
    DB that pre-dates this ADR" (the `migrate-force` step).

- **Operator workflow change:**
  - **Fresh dev DB**: `make up` (compose) → `make migrate-up` →
    `make ensure-topics` (existing `cli` subcommand) → services
    come up cleanly.
  - **Existing prod DB (one-time)**: `make migrate-force
    VERSION=<init-timestamp>`. After this, prod is on the migration
    runner; future schema changes ship as new `*.up.sql` files plus
    a `make migrate-up`.
  - **Adding a schema change**: `make migrate-new NAME=<slug>`
    generates the pair of files; author writes both sides; opens a
    PR; reviewer reads the diff; merge; operator runs
    `make migrate-up` against each environment.

- **No env-var changes.** Same `DATABASE_URL`, same
  `KAFKA_BOOTSTRAP`, same `JOB_TRACKER_*` namespace per ADR 0008.
  The Makefile reads them with the same defaults `internal/config`
  uses, so `make migrate-up` against an unset env hits the same
  localhost DSN as the services do.

- **No compose service added.** A future ADR may add a one-shot
  `migrate` compose service; explicitly out of scope here.

- **No Go dependencies added.** `go.mod` does not gain
  `golang-migrate`. The runner is a Docker image / system binary,
  not a Go library.

## Related decisions

- **ADR 0008** — Shared `internal/config`. Same shape of reasoning
  applied to env reads: one landing site for behaviour that was
  copy-pasted across cmd/. The Makefile's `DATABASE_URL` resolution
  matches what `internal/config.DSN("")` resolves to, so `make` and
  the services see the same database by default.

- **ADR 0001** — Richer schema and event contracts. The original
  schema lives on; this ADR changes how it's deployed, not what
  it contains. Future schema evolution lands in migration files,
  not in `schema.sql` (which no longer exists).

- **ADR 0005** — Producer-side input validation. Picks up the role
  that `validate_test.go` used to play in unit tests: the producer
  rejects values that violate the same constraints the schema
  enforces. The schema's `CHECK` clauses are now the second line of
  defence, not the source of the truth list. The truth list lives in
  `internal/events`; the producer enforces it before publish.

## Related requirements

- Schema changes against a database with production data must be
  expressible safely (renames, drops, tightenings — not just
  additive `IF NOT EXISTS`).
- Every contributor and the future-me-reading-this-in-six-months
  must have a discoverable list of common commands.
- Operator must be able to bootstrap migrations on a pre-existing DB
  without re-running DDL.

## Related artifacts

- `internal/db/migrations/<timestamp>_init.up.sql` (new — copy of
  today's `schema.sql`).
- `internal/db/migrations/<timestamp>_init.down.sql` (new).
- `internal/db/db.go` (modified — `Connect` no longer applies DDL;
  `SchemaSQL` removed).
- `internal/db/schema.sql` (deleted).
- `internal/jobclient/validate_test.go` (deleted).
- `Makefile` (new, root).
- `.env.example` (new).
- `.gitignore` (modified — adds `.env`).
- `README.md`, `docs/runbook.md` (modified — workflow docs).

## Related principles

- **Explicit operations over implicit self-healing.** A service that
  silently mutates schema on boot is convenient until it isn't. An
  operator command that mutates schema on demand is louder, slower
  to mis-trigger, and easier to reason about.
- **Match the reference idiom across repos.** Two projects on the
  same machine using different migration tools is a tax on
  context-switching. When one of them works, copy it.
- **Database invariants enforced at the database.** The drift
  detector did the right thing for the wrong layer. Postgres is the
  source of truth for what values are allowed; tests that talk to
  Postgres test the invariant; tests that re-implement Postgres do
  not.

## Notes

- **Why timestamp prefixes and not sequence numbers.** Sequence
  numbers conflict when two branches add a migration in parallel;
  timestamps don't. The reference uses timestamps. The cost is
  uglier filenames, which is fine.
- **Why `.env` not `direnv` / `envrc`.** `.env` is what compose
  already understands. Adding `direnv` is a third tool for the
  operator to install and is not justified by current scope.
- **CI does not yet exist in this repo.** When CI is added, the
  migration step is `make migrate-up` against a disposable test
  DB. Out of scope for this ADR.
- **What about backups?** `pg_dump` of the home-server DB before
  the first `migrate-force` is a no-brainer one-liner; folding it
  into `make backup` is plausible but premature — one operator,
  one DB, one bootstrap step. Calling it out in the runbook is
  enough.
- **What about a "migrate" subcommand on `cli`?** Considered and
  dropped per Positions #2. If the operator workflow ever needs a
  pure-Go path (e.g. a single static binary to ship to a host
  without Docker), revisit. The cost of adding it later is small;
  the cost of carrying two migration entry points now is not.
- **What if a migration needs Postgres data manipulation in Go,
  not SQL?** Out of scope today. `golang-migrate` supports Go
  migrations via the library API; we'd switch from CLI-only to
  CLI+library at that point. No current migration needs this.
