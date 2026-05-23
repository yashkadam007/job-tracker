# ADR 0006 — Consumer-side error classification in `cmd/store` and `cmd/scheduler`

## Issue

After ADR 0005 lands, most input errors are caught at the producer
boundary before a Kafka message is ever written. What remains at the
consumer side is a smaller, more genuinely operational set:

- **Infrastructure errors** — Postgres connection drops during a
  container restart (`pgx` connection errors), Kafka transient broker
  errors. Will succeed on retry within seconds.
- **Deploy-time schema drift** — producer publishes a field the
  consumer doesn't know how to decode, or vice versa, after a partial
  deploy. Permanent until code is fixed.
- **Truly unexpected** — a panic in a domain method, a constraint
  violation the producer-side validator didn't anticipate, an unknown
  topic. "Should never happen" cases.

Today (`cmd/store` and `cmd/scheduler`, identical shape), every error
category collapses to the same path: log a line, return from the
`EachRecord` closure, skip `MarkCommitRecords` for that record. This
causes silent data loss (offset jumps past the bad record when
surrounding records succeed) and stuck partitions across restarts
(when the bad record is the last unprocessed one).

Three problems with a single error path:

1. **Transient errors get treated as permanent.** A Postgres blip
   during deploy can leak rows through the offset-skip mechanism.
2. **Permanent errors get retried implicitly forever via Kafka
   redelivery on restart.** Partition blocked until manual intervention.
3. **Truly unexpected errors get swallowed alongside known-shaped
   ones.** No fail-fast signal that the operator should look.

## Decision

Each consumer (`cmd/store`, `cmd/scheduler`) gains its own `errors.go`
with typed sentinel errors and a classification policy that branches
the consumer loop into three paths.

**Sentinel errors** in `internal/store/errors.go` and
`internal/scheduler/errors.go`:

- `ErrDecode` — `json.Unmarshal` failure. Permanent.
- `ErrUnknownTopic` — `default` case in the topic switch. Permanent.
- `ErrInfraUnavailable` — `pgx` connection errors, Kafka transient
  errors, context-deadline shape. Transient.
- `ErrConstraintViolation` — Postgres `CHECK` or FK violation that
  producer-side validation should have caught. Permanent (and a
  signal that producer/consumer validation has drifted, per ADR 0005).

Domain methods (`store.ApplySubmitted` and friends, `scheduler.Handle*`)
wrap their internal errors with these sentinels using
`fmt.Errorf("…: %w", ErrInfraUnavailable)` so callers branch with
`errors.Is`.

**Classification policy** in each consumer's main loop:

```
err := handle(ctx, …)
switch {
case err == nil:
    MarkCommitRecords(r)
case errors.Is(err, ErrInfraUnavailable):
    retry with capped exponential backoff, indefinitely
case errors.Is(err, ErrDecode),
     errors.Is(err, ErrUnknownTopic),
     errors.Is(err, ErrConstraintViolation):
    structured log with full payload, topic, offset
    increment skip counter
    MarkCommitRecords(r)
default:
    log.Fatalf — crash, container restarts
}
```

**Retry policy** for `ErrInfraUnavailable`: 100ms, 250ms, 500ms, 1s,
2s, 5s, 10s — then 10s indefinitely. No upper bound on attempts. A
multi-hour Postgres outage keeps the consumer alive and resumes
cleanly when the service returns.

**Skip counter** is in-process (an atomic int per consumer) exposed at
`GET /skip-count` on a small admin HTTP server bound to localhost
(`:9090` for store, `:9091` for scheduler). Returns JSON
`{"count": N, "by_class": {"decode": …, "unknown_topic": …,
"constraint_violation": …}}`.

**Operator surface** is the TUI's status panel (ADR 0004). On entry to
the status view, the TUI fetches both endpoints and displays the
counters. A non-zero value signals "you have something to investigate
in the logs."

No DLQ topic. No alerter service. No Telegram coupling in the
consumers. `cmd/notifier` is unchanged.

## Status

Implemented.

## Group

Reliability / Consumer error handling.

## Assumptions

- ADR 0005 lands first. Producer-side validation kills the bulk of
  would-be-poison messages before they enter Kafka. The residual set
  this ADR handles is small.
- The classifications (`Decode`, `UnknownTopic`, `InfraUnavailable`,
  `ConstraintViolation`) cover the failure modes that actually occur.
  New ones can be added; the structure is stable.
- A multi-minute (or multi-hour) infra retry loop is the right
  behaviour for a solo-operator system. Crashing the consumer and
  relying on container restart-loops is strictly worse — restart-loops
  lose backoff state and hammer the recovering service.
- The TUI is a sufficient operator dashboard. No Prometheus, no
  Grafana. `/skip-count` is the minimum useful counter.
- A `log.Fatalf` on the `default` case is acceptable. The operator
  notices on next session via container restart count or logs. If
  this proves too quiet, escalate to a flipping healthcheck endpoint;
  out of scope for v1.
- Telegram is for users (reminders, captures, status changes from
  ADR 0003); the TUI is for the operator. This separation is honest
  and not to be muddled.

## Constraints

- No DLQ topic.
- No alerter service. No new container.
- No Telegram coupling in `cmd/store` or `cmd/scheduler`. Those
  consumers do not import `internal/telegram` and do not read
  `TELEGRAM_CHAT_ID`. Operator-facing surfaces stay in the operator's
  existing tool (TUI). `cmd/notifier` remains single-purpose:
  delivering job-related notifications to users (with email as a
  possible future channel — see ADR 0003).
- Sentinel errors are exported and used with `errors.Is`. Type
  assertions on concrete error types are not part of the API.
- Domain methods (`store.Apply…`, `scheduler.Handle…`) are responsible
  for wrapping internal errors with the right sentinel. The consumer
  loop trusts the classification.
- Counter HTTP endpoint binds to localhost only. No external exposure.
- A skip path must always log the full payload, topic, partition, and
  offset. Without that, the structured log is useless for replay.

## Positions

Alternatives considered:

1. **Per-consumer `errors.go` + three-way classification +
   in-process counter + TUI surface** (this decision).
2. **DLQ topic + Telegram alerter.** Rejected. (a) ADR 0005 makes
   most of it unnecessary; (b) the alerter conflicts with
   `cmd/notifier`'s actual job of delivering job-related
   notifications to users; (c) standing up a separate alerter
   service is too much infrastructure for a solo-operator project;
   (d) the TUI is a natural and underused surface.
3. **Fail-fast on every error.** Rejected — a 5-second Postgres blip
   during routine container restart would crash the consumer.
   Restart-loop hides legitimate state and hammers recovering
   infrastructure.
4. **Skip + log only, no classification.** Rejected — preserves the
   silent-data-loss problem identified in the original draft.
5. **Prometheus + Alertmanager.** Rejected for v1 — too much
   infrastructure relative to the actual problem volume. Revisit if
   a second operator ever uses this system.
6. **Counter in Postgres (table `consumer_skips`).** Rejected —
   durable across restarts but requires Postgres availability to
   record an error caused by Postgres unavailability. In-process
   counter with HTTP is simpler and the loss-on-restart is acceptable
   (operator notices via logs too).
7. **Capped retry budget on infra errors with crash on exhaustion.**
   Rejected — converts long but recoverable outages into manual
   intervention. Indefinite retry is the right default; the operator
   notices a stuck consumer via the TUI status panel ("processed N
   records in last hour: 0").

## Argument

- **ADR 0005 has done most of the work.** With producer-side
  validation catching enum, format, and required-field errors, what
  remains is genuinely an operations problem (infra) or a deploy bug
  (schema drift). The right responses for those are different from
  the right response for bad input.
- **Three classes, three behaviours.** Transient → retry. Permanent →
  log loudly, skip, count. Unexpected → fail fast. Each is the right
  choice for its class; collapsing them is what created the original
  bug.
- **Indefinite retry on infra is correct.** Postgres-going-away during
  deploy is the most common consumer error in this system. A capped
  retry budget would convert routine deploys into data loss. The
  "operator notices the stuck consumer" failure mode is acceptable
  because the alternative (silent skip) is much worse.
- **The TUI is the operator surface.** ADR 0004 introduced a desktop
  tool the operator uses for triage. A status panel that shows "N
  skipped messages since boot" leverages the existing UI for the
  existing audience. No new channel, no Telegram noise.
- **The `default` case is honest.** A fail-fast on unclassified
  errors is a contract: "if you see this, the classification missed
  a case, please add it." Better than silently swallowing.
- **No new infrastructure.** Files only — sentinel definitions, a
  switch statement, a tiny admin HTTP handler.

## Implications

- `internal/store/errors.go` (new) — sentinel set + a small helper
  `Classify(err) ErrorClass` for use by the consumer loop.
- `internal/scheduler/errors.go` (new) — same shape.
- `internal/store/store.go` and the `Apply*` methods — wrap returned
  errors with the appropriate sentinel using `%w`. Specifically:
  - `pgx.ErrNoRows` is *not* an error (current code already treats
    "missing parent row" as a logged-but-non-error case); unchanged.
  - `*pgconn.PgError` with code class `23` (constraint violation) →
    `ErrConstraintViolation`.
  - `*pgconn.ConnectError`, `errors.Is(err, context.DeadlineExceeded)`
    on the DB ctx, → `ErrInfraUnavailable`.
- `internal/scheduler/scheduler.go` and `Handle*` — same wrapping.
- `cmd/store/main.go` — consumer loop gains the classification
  switch, the retry helper, the structured-log skip path, and the
  admin HTTP server goroutine.
- `cmd/scheduler/main.go` — same shape.
- `internal/tui/` — status panel reads both `/skip-count` endpoints
  via `http.Client` with a short timeout. Displays counts; flags
  any non-zero value in red.
- `docker-compose.yml` — store and scheduler expose `9090:9090` and
  `9091:9091` to the host so the TUI can reach them.
- No new topics. No changes to `internal/events/`. No changes to
  `internal/jobclient/`. No changes to `cmd/notifier`.

## Related decisions

- **ADR 0001** — Schema and event contracts. The constraint
  violations sentinel-wraps point back at this schema.
- **ADR 0002** — Shared `jobclient`. Establishes the sentinel-error
  pattern this ADR follows for consumer packages.
- **ADR 0004** — Desktop TUI. Provides the operator surface this ADR
  uses for skip-counter display.
- **ADR 0005** — Producer-side input validation. Strict prerequisite —
  without it, the skip path would catch what should have been caught
  at the producer.

## Related requirements

- A poison message must not cause silent data loss.
- A poison message must not block a partition indefinitely.
- A transient infrastructure outage must not lose data and must not
  require operator intervention.
- A truly unexpected error must fail loudly enough for the operator
  to notice on next session.
- All operator-facing surfaces stay inside operator tools (TUI), not
  user channels (Telegram).
- Each service handles its own errors gracefully via typed sentinels.

## Related artifacts

- `internal/store/errors.go` (new).
- `internal/scheduler/errors.go` (new).
- `internal/store/store.go`, `internal/scheduler/scheduler.go` —
  sentinel-wrap returned errors.
- `cmd/store/main.go`, `cmd/scheduler/main.go` — classification
  switch, retry loop, admin HTTP server.
- `internal/tui/` — status panel reads `/skip-count`.
- `docker-compose.yml` — expose admin ports.

## Related principles

- **No silent skip.** Every record is one of: marked after success,
  marked after structured-log skip with counter, retried until
  success, or crashes the process.
- **Operator surfaces stay in operator tools.** Telegram is for users;
  TUI is for the operator.
- **Single-purpose services.** `notifier` delivers user-facing
  notifications; consumers consume; this ADR adds no new
  responsibilities to any existing service.
- **Fail-fast on the unknown.** The `default` case is an explicit
  contract, not a swallow.

## Notes

- **Indefinite retry vs liveness probes.** Some platforms kill
  containers whose health endpoint fails. The admin HTTP server's
  `/healthz` returns 200 even while a consumer is retrying — retrying
  *is* healthy by this ADR's definition. Only the `default` case
  crash-loop signals unhealthy.
- **Counter granularity.** v1 exposes a single total plus the by-class
  breakdown. The TUI panel renders both; the operator can drill into
  logs for the offending offsets.
- **Counter persistence.** In-memory; resets on container restart.
  The operator sees the *current session*'s count, which is the
  operationally interesting number. Persisting across restarts would
  require Postgres or a file, both more work than warranted.
- **Drift detection.** If `ErrConstraintViolation` is ever observed
  at the consumer, that's a bug in ADR 0005's producer validation
  (or schema-vs-validator drift). The skip+log+counter pathway gives
  the operator a chance to notice; the test in ADR 0005's
  drift-detection should also be reviewed.
- **Structured log format.** JSON line at level=error with fields:
  `topic`, `partition`, `offset`, `class`, `error`, `payload_b64`.
  Base64 keeps binary-safe in log aggregators; a one-liner `base64
  -d | jq` recovers it for replay.
- **Replay procedure.** Manual for v1: copy the `payload_b64` from
  the log, decode, fix the upstream cause (deploy the schema fix or
  re-validate via ADR 0005), then publish back to the original topic
  with `kcat`. Document in `docs/runbook.md` when the path first runs
  in anger.
- **Scheduler admin HTTP server.** The scheduler's failure mode is less
  severe (missed reminder, not missed row), but it still runs the same
  admin HTTP server shape as `cmd/store`. Uniformity across consumers
  is worth the few lines — the TUI status panel treats both endpoints
  identically.
- **Panic recovery in the consumer loop.** Each consumer wraps its
  handler invocation with `defer recover()`. A recovered panic is
  treated as the `default` case: structured log with
  `topic/partition/offset/payload_b64` and a stack snippet, then
  `log.Fatalf`. Same crash outcome as an unrecovered panic, but the
  operator gets a structured line identifying the offending message
  instead of a raw stack trace.
- **Open question:** rate-limiting on the structured log path. If a
  burst of 1000 identical decode errors arrives, the log fills with
  noise. Worth a per-class log-coalesce ("N more of the same in the
  last 10s")? Defer until observed.
