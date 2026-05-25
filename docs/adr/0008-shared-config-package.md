# ADR 0008 — Shared `internal/config` for cmd entry points

## Issue

`brokers()` and `dsn()` are copy-pasted across the cmd/ entry points
with identical bodies and a deliberate-but-divergent env namespace:

- `cmd/store/main.go:240`, `cmd/scheduler/main.go:269`,
  `cmd/bot/main.go:59`, `cmd/notifier/main.go:150` each define
  byte-identical `brokers()` + `dsn()` reading `KAFKA_BOOTSTRAP` and
  `DATABASE_URL`.
- `cmd/cli/main.go:95` has a fifth copy of `brokers()` (producer-only,
  no `dsn()`).
- `cmd/jobtracker/main.go:67` reads the namespaced
  `JOB_TRACKER_KAFKA_BOOTSTRAP` / `JOB_TRACKER_DATABASE_URL` per
  ADR 0004's `~/.zshrc`-namespace rationale.

Four-to-five copies of the same env-read + default-fallback wiring.
Today the four non-namespaced copies are still byte-identical, but the
moment TLS, a connection-string flag, or a startup retry needs to land,
it lands in one cmd/ and silently skips the others. The namespacing
choice is intentional and stays — but it doesn't justify duplicating
the read/default logic five times across the tree.

## Decision

Introduce `internal/config/` exposing two plain functions:

```go
func Brokers(prefix string) []string
func DSN(prefix string) string
```

Both accept an env-var prefix:

- The four services (`store`, `scheduler`, `bot`, `notifier`) and
  `cmd/cli` call `config.Brokers("")` / `config.DSN("")` and continue
  reading `KAFKA_BOOTSTRAP` / `DATABASE_URL`.
- `cmd/jobtracker` calls `config.Brokers("JOB_TRACKER_")` /
  `config.DSN("JOB_TRACKER_")` and continues reading
  `JOB_TRACKER_KAFKA_BOOTSTRAP` / `JOB_TRACKER_DATABASE_URL`.

Dev defaults stay baked in (`localhost:9092` for brokers, the local
Postgres DSN for `dsn`) — moving them now would change behaviour for
every entry point in the same commit as the refactor, and the goal
here is a strict de-duplication, not a config-policy redesign.

Each cmd/ entry point deletes its private `brokers()` / `dsn()` and
imports `internal/config`. No public API change for operators; every
existing env var continues to work with the same precedence and the
same defaults.

## Status

Implemented.

## Group

Refactoring / Code organization.

## Assumptions

- The namespace divergence between `jobtracker` and the four services
  is permanent (ADR 0004). The shared helper must accommodate both,
  not collapse them.
- Future config additions (TLS, startup retry, connection-string
  flags) will want one landing site, not five — that's the whole
  reason the duplication is worth removing now, before such a change
  arrives and the divergence becomes silent.
- `brokers()` + `dsn()` is the right v1 scope. Other env reads
  (`JOB_TRACKER_ADMIN_HOST` in `cmd/jobtracker/main.go:92`,
  `envDuration` in `cmd/scheduler/main.go:286`) are single-call-site
  helpers today — moving them now adds churn without removing
  duplication. They can join `internal/config` on demand the next
  time a second caller appears.
- Plain functions are sufficient. A typed `Config` struct loaded once
  via `Load()` would be nicer if config grew, but with two values it's
  ceremony. Revisit if `internal/config` grows past ~4–5 functions.

## Constraints

- No behaviour change for any operator. Same env var names, same
  precedence, same defaults, same fall-back string.
- No new external dependencies. Standard library only (`os`,
  `strings`).
- `internal/config` must not depend on any other `internal/` package.
  It's a leaf — every cmd/ and most `internal/` packages may
  eventually import it; it imports none of them.
- The prefix argument is a literal string, not a "namespace" type.
  Callers pass `""` or `"JOB_TRACKER_"`. Trailing underscore is the
  caller's responsibility — keeps the helper boring.

## Positions

Alternatives considered:

1. **`internal/config` with explicit prefix arg** (this decision).
2. **Two-tier lookup in one no-arg helper.** A single `Brokers()`
   that checks `JOB_TRACKER_KAFKA_BOOTSTRAP` first then falls back to
   `KAFKA_BOOTSTRAP`. Rejected — every cmd would silently accept both
   namespaces, eroding the ADR 0004 boundary. The point of the
   namespace is that `jobtracker` reads `JOB_TRACKER_*` and the
   services don't; a fall-back collapses that on/off distinction into
   a precedence rule that's only visible by reading the helper.
3. **Keep `jobtracker`'s helpers separate; consolidate the four
   services only.** Rejected — leaves two parallel implementations
   of the same logic, which is the situation this ADR exists to end.
4. **Typed `Config` struct via `config.Load()`.** Rejected for v1.
   Nicer if config grows; ceremony for two fields. Revisit when a
   third value joins.
5. **Move `brokers()` / `dsn()` into `internal/jobclient`.** Rejected
   — `jobclient` is the frontend read/publish library (ADR 0002).
   Consumer services (`store`, `scheduler`, `notifier`) don't import
   `jobclient`; pulling them into it just to share an env-read would
   invert the dependency direction.
6. **Do nothing.** Rejected — the duplication is the bug; the four
   copies are byte-identical today only because no one has had a
   reason to change them yet.

## Argument

- Single source of truth for "how a cmd/ reads its broker list and
  DSN". The next config knob (TLS, retry, timeout) lands once.
- Honours the existing ADR 0004 namespacing without re-litigating it
  — the prefix argument makes the boundary visible at the call site
  (`config.Brokers("JOB_TRACKER_")` reads as "this binary uses the
  namespaced env vars") rather than hiding it inside a fall-back
  chain.
- Mechanical, low-risk change. No behaviour shift; every existing
  env var continues to resolve identically.
- Keeps `internal/config` small and boring. The package earns new
  functions only when a second call site appears — same bar
  `internal/jobclient` was held to in ADR 0002.

## Implications

- New package `internal/config/` with `Brokers(prefix string)
  []string` and `DSN(prefix string) string`.
- Six entry points modified to delete their private helpers and
  import `internal/config`:
  `cmd/store`, `cmd/scheduler`, `cmd/bot`, `cmd/notifier`, `cmd/cli`,
  `cmd/jobtracker`.
- `cmd/cli` loses only its `brokers()` (no `dsn()` to remove —
  producer-only).
- The dev-default fall-back strings live in `internal/config` now.
  Changing them is a one-line change visible to every entry point at
  once — which is the entire point.
- No migration step for operators. Existing `KAFKA_BOOTSTRAP`,
  `DATABASE_URL`, `JOB_TRACKER_KAFKA_BOOTSTRAP`, and
  `JOB_TRACKER_DATABASE_URL` continue to work unchanged.
- Tests are minimal — the helpers are pure `os.Getenv` + default. A
  table test over `(prefix, env value)` → expected output covers it.

## Related decisions

- **ADR 0002** — Shared `internal/jobclient` library. Same shape of
  reasoning (extract shared infrastructure before drift sets in);
  this ADR applies it to the env-read layer that `jobclient` itself
  doesn't cover.
- **ADR 0004** — Desktop TUI. Source of the `JOB_TRACKER_*`
  namespace that the prefix argument exists to honour.

## Related requirements

- DRY across cmd/ entry points.
- One landing site for future broker/DSN config additions (TLS,
  startup retry, …).
- Preserve the `~/.zshrc`-namespace separation for the TUI binary.

## Related artifacts

- `internal/config/config.go` (new)
- `cmd/store/main.go` (delete `brokers`/`dsn`, import `config`)
- `cmd/scheduler/main.go` (delete `brokers`/`dsn`, import `config`)
- `cmd/bot/main.go` (delete `brokers`/`dsn`, import `config`)
- `cmd/notifier/main.go` (delete `brokers`/`dsn`, import `config`)
- `cmd/cli/main.go` (delete `brokers`, import `config`)
- `cmd/jobtracker/main.go` (delete `brokers`/`dsn`, import `config`
  with `"JOB_TRACKER_"`)

## Related principles

- **Thin frontends, shared infrastructure.** Same principle as
  ADR 0002, applied one layer lower.
- **Visible boundaries over implicit precedence.** The namespacing
  difference between `jobtracker` and the services is a real
  design choice; the prefix argument keeps it legible at the call
  site instead of burying it in a fall-back rule.

## Notes

- `Brokers` continues to split on `,` with no trimming, matching
  every existing copy. A future helper could trim whitespace and
  drop empties; out of scope here.
- If `internal/config` accrues a third function (e.g.
  `AdminHost(prefix)`), revisit the "plain functions vs. typed
  struct" call. The threshold is roughly: when a caller needs three
  or more values, `Load()` returning a struct beats three function
  calls.
- The prefix string is passed verbatim. `config.Brokers("foo")`
  would read `fooKAFKA_BOOTSTRAP` — caller bug, not helper bug.
  Documenting the trailing-underscore convention in the package
  doc comment is enough.
