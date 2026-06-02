# ADR 0012 — Push-based TUI reconciliation via Postgres `LISTEN`/`NOTIFY`

## Issue

The TUI's write path is two-hop: a keystroke publishes a Kafka
event via `jobclient.Publisher` (ADR 0001 / ADR 0004), the Store
consumer (a separate process, `cmd/store`) reads the event off
Kafka and commits a projection into Postgres (`internal/store/store.go`).
Reads come exclusively from that projection — every render is a
fresh `jobclient.Reader.List` against Postgres (ADR 0004).

Today, the TUI reconciles optimistic UI with the projection by
firing a `listJobsCmd` immediately after the publish ack. The
comment at `internal/tui/tui.go:287-291` calls out the race:

```go
// Re-query so the new row appears once the Store consumer has
// processed the event. There's a small race here — if the
// reload arrives before the consumer commits, the row is
// briefly absent. Acceptable.
return m, listJobsCmd(m.cfg.Reader, m.statusFilter)
```

The race is not acceptable in practice. The observed symptom is:
the operator presses `n`, fills the new-job modal, hits Enter, and
the row sometimes does not appear. Pressing `R` (reload) reveals
it. The interval between publish-ack and consumer-commit is the
gap; whether the immediate `listJobsCmd` lands inside or outside
that gap is a coin flip determined by Kafka delivery latency,
consumer scheduling, and Postgres commit latency.

The race is masked for status changes, edits, snooze, and rejected
states because those keystrokes run an *optimistic* in-memory
mutation first (`applyStatus`, `internal/tui/tui.go:581-595`); the
eventual reload reconciles, but the row never *disappears* from
the user's view. The new-job path has no equivalent optimism
because the JobID does not exist on screen at all until the
projection produces it. So submits are the visible failure mode
of a general problem.

Fixing the symptom for submits with another optimism path is
possible but exposes a second hazard: if the operator
immediately presses `a` (apply) against an optimistically-inserted
row, the TUI publishes `job.status.changed` for a JobID the
consumer has not yet projected. The two events live on different
Kafka topics (`job.submitted`, `job.status.changed` — see
`internal/jobclient/publisher.go:61, 69`), and Kafka guarantees
ordering *within a partition of one topic*, not *across topics*.
Cross-topic delivery to the Store consumer can interleave; the
status change may be applied against a non-existent row, which
`ApplyStatusChanged` handles by setting `missing=true` (see
`internal/store/store.go:175-179`) — the projection silently
drops the status change, the operator's optimistic UI says
"applied", and the eventual reload reveals "saved". A second
event-sourced bug masquerading as a TUI bug.

The root cause is not specific to submits. It is that the TUI
*guesses at the consumer's commit time* with a fixed-delay
reload. Any heuristic timing fix is a sharper guess. The system
has the information to be exact — Postgres knows the moment the
commit lands; the TUI is on the same Postgres. The producer and
consumer just need a wakeup channel that fires at commit time.

The tracker holds live data (per `project_tracker_in_active_use`
memory). Reads must remain on Postgres (ADR 0004); writes must
remain via Kafka (ADR 0001); producers do not read on the write
path (ADR 0001). Any fix must respect those boundaries.

## Decision

Add a Postgres `LISTEN`/`NOTIFY` wakeup channel from the Store
consumer to any reading frontend. The Store fires `NOTIFY
jobs_changed` inside the same transaction that commits the
projection; the TUI holds a dedicated pgx connection running
`LISTEN jobs_changed` and routes every notification into a
`tea.Msg` that triggers `listJobsCmd`. The new row appears the
instant the projection commits — no polling, no optimism for
submits, no cross-topic ordering hazard.

Four coordinated changes:

**1. New channel `jobs_changed`.**

The channel name is fixed and global to the Store consumer's
projection. Single channel for every job-projection-affecting
event; the payload identifies which event class fired so the TUI
(or a future frontend) can choose to ignore some. No DDL — channel
names in Postgres are implicit; the first `LISTEN` or `NOTIFY`
brings them into being.

Payload shape:

```json
{ "job_id": "...", "event": "submitted" }
```

`event` is one of: `submitted`, `status_changed`, `edited`,
`note_added`, `interview_recorded`. Payload is small (well under
the 8 KB `pg_notify` limit). No `event_id` in the payload — the
notification is a *wakeup*, not the event itself; the canonical
event log is still Kafka. The TUI does not deduplicate
notifications; a coalesced `listJobsCmd` is idempotent.

**2. Store consumer fires `NOTIFY` inside each apply transaction.**

Every `Apply*` method in `internal/store/store.go` calls
`pg_notify('jobs_changed', $1)` after its projection write and
before `tx.Commit`. Running the notify inside the tx means the
notification is delivered if and only if the commit succeeds —
no false wakeups on rollback, no missed wakeups on a successful
commit. Five callsites: `ApplySubmitted` (line 44),
`ApplyStatusChanged` (line 152), `ApplyNoteAdded` (line 194),
`ApplyEdited` (line 230), `ApplyInterviewRecorded` (line 319).

For `ApplyStatusChanged` and `ApplyNoteAdded` the missing-job
short-circuit (`ct.RowsAffected() == 0` / `exists=false`) skips
the notify — there is no projection change to wake the frontend
about. The event still claims its slot in `processed_events` so
it isn't re-delivered.

**3. New TUI cmd `listenJobsChangedCmd`.**

A long-running tea.Cmd that acquires one connection from the
pool, runs `LISTEN jobs_changed` once, then blocks on
`pgxConn.WaitForNotification` and emits a `jobsChangedMsg` for
each notification. The cmd re-arms itself from the message
handler (returns a fresh `listenJobsChangedCmd` in the `tea.Batch`
alongside `listJobsCmd`) so one notification produces one
reconciling read and one new wait.

The LISTEN connection is *dedicated* — `LISTEN` is connection-
scoped in Postgres; sharing the conn with `Reader.List` would
mean any unrelated query overlapping the wait would either fail
or reset the subscription. The TUI's pool already sizes for
multi-connection use (`internal/db/db.go`); reserving one
connection for the listen is cheap.

**4. Remove the immediate post-mutation reload.**

The `case submittedMsg`, `case statusChangedMsg`, `case editedMsg`
branches in `internal/tui/tui.go:248-311` currently fire
`listJobsCmd` immediately after publish ack. With LISTEN in place
those reloads either (a) race the consumer and miss the row
(today's bug) or (b) duplicate work the notify will trigger
seconds later. Remove them. The optimistic in-memory mutations
(`applyStatus`) stay — they make keystrokes feel instant; the
notify-driven reload reconciles when the projection catches up.

Submits get no optimism. The row appears when the projection
produces it (~10-50 ms median). That is fast enough not to need
optimistic insertion, and avoids the cross-topic-ordering hazard
described in the Issue.

## Status

Implemented.

## Group

Frontend / Reconciliation.

## Assumptions

- The Store consumer is the sole writer to the `jobs` /
  `job_status_history` / `job_notes` / `job_interviews` tables.
  No other process commits to those tables, so a notify fired
  from `Apply*` is sufficient to cover every projection update.
  Direct `psql UPDATE` sessions by the operator (the ADR 0011
  fallback) will not fire the notify — the TUI will not auto-
  refresh — and the operator can press `R` exactly as today.
- Postgres `LISTEN`/`NOTIFY` is reliable while the connection
  is alive. The Postgres docs guarantee delivery to every
  connection that holds `LISTEN` at notify time; lost-on-the-
  wire is not a documented failure mode of the in-process notify
  queue.
- Connection drops are the only path to missed notifications.
  A dropped LISTEN connection re-establishes from the TUI side;
  during the gap, any events that committed are missed by the
  push channel. The cold-path reload-on-reconnect (`listJobsCmd`
  fired in the reconnect handler) restores convergence.
- The TUI is one process per operator. There is one LISTEN
  client. Multi-frontend scenarios (Telegram bot, future web
  UI) are addressed by each frontend opening its own LISTEN,
  not by fanout from the Store.
- The Telegram bot (ADR 0003) does not need real-time
  reconciliation — operator interaction is synchronous over
  the bot conversation, and the bot's reads are reaction-time,
  not subscription-time. Out of scope.
- The Notifier (ADR 0007) and Scheduler are Kafka consumers,
  not Postgres readers, and do not need a wakeup.
- `pg_notify` payload stays small (`job_id` + `event` kind).
  The Postgres 8 KB payload limit is a non-issue for this shape.

## Constraints

- Producers do not read from Postgres on the write path
  (ADR 0001). The notify lives on the *consumer* side, not the
  producer — the TUI's write path is unchanged.
- Schema unchanged. `pg_notify` is built-in; channel names are
  implicit. No migration.
- Notify runs inside the consumer's apply transaction. A
  rolled-back apply produces no wakeup; a successful apply
  produces exactly one wakeup per `Apply*` call. Replays (via
  `processed_events`, ADR 0007) are no-ops on the projection
  *and* skip the notify by checking the `applied=false` path —
  no spurious wakeups on duplicate event delivery.
- The LISTEN connection is checked out of the pool and held for
  the lifetime of the TUI process. Default pool size (10+) has
  ample headroom for one long-lived conn plus the Reader's
  short queries.
- The TUI's `Init` returns `tea.Batch(listJobsCmd, listenJobsChangedCmd)`
  — the initial snapshot still arrives synchronously, then the
  LISTEN takes over for subsequent reconciliation.
- Notification payloads are JSON, hand-marshalled in SQL via
  `json_build_object` (or Go-side `json.Marshal` passed into
  `pg_notify`). The TUI parses with `encoding/json`; a parse
  failure is logged and the wakeup is still honoured (parse
  failure does not block the reconciling read).
- The TUI debounces nothing. One notification = one
  `listJobsCmd` invocation. A burst of five events for the same
  job (operator runs an `edit` followed by an `applied`
  hotkey) produces five fetches in quick succession. The List
  query returns ≤ 500 rows from a single-operator tracker;
  five fetches per second is well within budget. Revisit if
  the tracker grows.

## Positions

Alternatives considered:

1. **Postgres `LISTEN`/`NOTIFY`, fired inside the Store
   consumer's apply tx, consumed by the TUI on a dedicated pgx
   connection** (this decision).
2. **Consumer-offset read-your-writes.** Have
   `Publisher.Submit` (and the other publishers) return the
   produced Kafka `(topic, partition, offset)`. Add a
   `/committed-offset?topic=X&partition=Y` endpoint to the
   Store admin server (same shape as the ADR 0006 `/skip-count`).
   After publishing, the TUI polls until `committed >= published`,
   then fetches. Strictly correct, but five new moving parts
   (publisher returns offsets, admin endpoint, polling logic,
   per-publish state, partition-aware bookkeeping) vs.
   LISTEN/NOTIFY's two (notify call site, listen loop). The
   latency floor is identical — both wait for the consumer
   commit — but the push model avoids any polling at all.
3. **Optimistic insert with pending tracking.** Synthesize a
   `jobclient.Job` from the `JobSubmitted` event, keep it
   visible across reloads until the snapshot returns the same
   JobID, then reconcile. Fastest perceived UX (row appears
   the instant Enter is pressed) but exposes the cross-topic
   ordering hazard (Issue, paragraph 4): an immediate `a`
   keystroke against a pending JobID publishes a status
   change for a row the consumer has not yet projected, the
   status change is silently dropped by `ApplyStatusChanged`'s
   missing-row branch (`internal/store/store.go:175-179`), and
   the operator's optimistic UI lies. Mitigating the hazard
   requires the TUI to *defer* status changes against pending
   JobIDs until the projection catches up — a queueing layer
   inside the TUI. Two systems doing the same job (the
   queue inside the TUI, the consumer outside) is worth
   avoiding. Considered as an additive *on top of* LISTEN/NOTIFY
   for perceived snappiness; deferred to a follow-up ADR if
   the LISTEN-only solution feels slow in practice.
4. **Retry-until-visible polling.** After the publish ack, fire
   `listJobsCmd` on a 300 ms tick until the new JobID appears
   in the result, then stop. Functionally correct, but the row
   is *briefly absent* between the publish ack and the first
   successful fetch — which is exactly today's symptom, just
   with shorter duration. Also produces wasted reads in the
   common fast path. LISTEN/NOTIFY hits the projection-commit
   moment exactly, not before, not after.
5. **Delay the immediate post-mutation reload.** Insert a
   200-500 ms `tea.Tick` between publish ack and `listJobsCmd`.
   Reduces the race probability but does not eliminate it;
   tail latency still misses. Heuristic; rejected on
   "guesses at consumer commit time" grounds.
6. **Direct Postgres writes from the TUI for submits** (skip
   Kafka on the producer side). The TUI inserts the row
   itself, then publishes the event to Kafka for downstream
   consumers (notifier, scheduler). The Store consumer
   short-circuits on its own `event_id` claim. Violates "no
   service writes to another's tables" (the same boundary
   ADR 0010 and ADR 0011 explicitly preserve). Inverts the
   event-sourced model to fix a UX gap.
7. **TUI tails Kafka directly.** The TUI itself consumes
   `job.submitted` / `job.status.changed` / etc. and reads
   its own publishes off the topic. The TUI becomes a stateful
   Kafka consumer. Heavy: introduces consumer state in a UI
   process that has none today; doesn't help the *Postgres*
   projection visibility question because the projection
   still lags Kafka delivery. The notification we care about
   is "the projection now contains this row", not "Kafka has
   the event"; LISTEN/NOTIFY fires on the former.
8. **Postgres logical replication / `pg_recvlogical`** as the
   TUI's read path. Replaces `Reader.List` with a streaming
   replication slot. Massive footprint for a personal tracker;
   replication slots are a per-database resource that must be
   cleaned up; the slot survives a TUI crash and leaks WAL.
   Wrong tool.
9. **Materialised view + `REFRESH MATERIALIZED VIEW
   CONCURRENTLY` triggered by the consumer.** Adds a layer
   between projection and read with no benefit — the TUI's
   read is already a single `SELECT` on the canonical tables.
10. **`NOTIFY` from a Postgres `AFTER INSERT/UPDATE` trigger
    on `jobs` directly.** Bypasses the consumer's
    `Apply*` boundary — the trigger fires for any write to
    `jobs`, including a hypothetical direct `psql UPDATE`.
    Tempting (covers the manual-update case too) but
    misplaced: the trigger fires inside the writer's
    transaction, not the consumer's, so the wakeup arrives
    while the consumer is still mid-transaction on the
    `processed_events` insert — the LISTEN-side reload might
    see the row before the event-processing ledger has it.
    Keeping the notify inside the consumer's tx, after the
    projection write, after the ledger insert, before commit,
    is the position that aligns the wakeup with "this event
    is fully applied". The direct-`psql` case stays a manual
    `R` press, which the operator can do.

## Argument

- **The system already has the information to be exact.**
  Postgres knows the moment the projection commits; the TUI is
  on the same Postgres. A wakeup channel between them is the
  natural way to surface that information, and `LISTEN`/`NOTIFY`
  is the built-in primitive.
- **One mechanism covers every event class.** Status changes,
  edits, notes, interview rows — all of them race the
  consumer's commit just like submits do; the optimistic-update
  pattern is what's been masking the bug. LISTEN drives a
  reconciling read for *any* projection change, no per-event
  bespoke handling.
- **The push model removes the heuristic.** Today's reload
  guesses at consumer latency; LISTEN/NOTIFY fires exactly when
  the row becomes readable. No polling tick to tune, no
  retry budget to bound.
- **Architectural fit.** Kafka stays the event log. Postgres
  stays the projection. Producers still don't read on the write
  path. The Store consumer remains the only writer to job
  tables. The notify is a *side channel*, not a new source of
  truth.
- **Cross-topic ordering hazard avoided.** Submit-then-apply
  works because the operator sees nothing on submit until the
  projection commits, and the projection's `ApplyStatusChanged`
  cannot land before `ApplySubmitted` for the same JobID if
  the operator can only press `a` *after* the row appears.
  The race vanishes because the optimism vanishes.
- **Minimal blast radius.** Five call sites in the Store
  consumer, one new tea.Cmd in the TUI, one new tea.Msg, one
  new handler. No schema change, no event-contract change, no
  consumer-protocol change.
- **The cost of failure is the existing UX.** If the LISTEN
  connection drops and reconnect lags, the operator's manual
  `R` keystroke is the same fallback they have today. The
  failure mode of the new mechanism is "back to the old
  behaviour", not "new class of bug".

## Implications

- **`internal/db/notify.go`** (new). One helper:
  ```go
  func NotifyJobsChanged(ctx context.Context, tx pgx.Tx, jobID, kind string) error {
      payload, _ := json.Marshal(map[string]string{"job_id": jobID, "event": kind})
      _, err := tx.Exec(ctx, `SELECT pg_notify('jobs_changed', $1)`, string(payload))
      return err
  }
  ```
  `tx pgx.Tx` (not `pgxpool.Pool`) — the notify must run inside
  the apply transaction.
- **`internal/store/store.go`.** Five new call sites — one per
  `Apply*` method, immediately before `tx.Commit`:
  - `ApplySubmitted` → `NotifyJobsChanged(ctx, tx, ev.JobID, "submitted")`.
  - `ApplyStatusChanged` → `"status_changed"`, skipped when
    `ct.RowsAffected() == 0`.
  - `ApplyNoteAdded` → `"note_added"`, skipped when `!exists`.
  - `ApplyEdited` → `"edited"`, skipped when
    `ct.RowsAffected() == 0`.
  - `ApplyInterviewRecorded` → `"interview_recorded"`,
    skipped when `!exists`.
  Duplicate-event short-circuits (the `ErrAlreadyProcessed`
  branch at the top of each apply) return before reaching the
  notify, so replays do not wake the frontend.
- **`internal/tui/cmds.go`.** New message and cmd:
  ```go
  type jobsChangedMsg struct {
      JobID string
      Event string
      err   error // populated on parse / listen error
  }

  func listenJobsChangedCmd(pool *pgxpool.Pool) tea.Cmd {
      return func() tea.Msg {
          conn, err := pool.Acquire(context.Background())
          if err != nil { return jobsChangedMsg{err: err} }
          // The conn is released to the pool only on shutdown — held for
          // the lifetime of the TUI. LISTEN is connection-scoped.
          if _, err := conn.Exec(context.Background(), `LISTEN jobs_changed`); err != nil {
              conn.Release()
              return jobsChangedMsg{err: err}
          }
          n, err := conn.Conn().WaitForNotification(context.Background())
          if err != nil {
              conn.Release()
              return jobsChangedMsg{err: err}
          }
          var payload struct{ JobID, Event string }
          _ = json.Unmarshal([]byte(n.Payload), &payload)
          return jobsChangedMsg{JobID: payload.JobID, Event: payload.Event}
      }
  }
  ```
  The conn-acquire-then-release pattern above is a sketch; the
  production version holds the conn on the Model so subsequent
  `WaitForNotification` calls reuse it (re-acquiring per
  notification would re-issue `LISTEN` every time and lose
  any notifications queued between calls).
- **`internal/tui/tui.go`.**
  - `Model` gains a field for the LISTEN connection handle (or
    a channel pumped by a background goroutine — see Notes).
  - `Init` returns `tea.Batch(listJobsCmd(...), listenJobsChangedCmd(...))`.
  - New `case jobsChangedMsg` in `Update`. On success: fire
    `listJobsCmd` and re-arm the listen. On error: fire
    `listJobsCmd` (cold-path fallback), log via `m.err`,
    re-arm the listen after a 1 s backoff (`tea.Tick`).
  - Remove the trailing `listJobsCmd` from `case submittedMsg`
    (line 287-291), `case statusChangedMsg` (line 248-258),
    `case editedMsg` (line 293-311). Optimistic in-memory
    mutations in `applyStatus` (line 581-595) stay.
  - The comment block at line 287-291 is deleted along with the
    line it documents.
- **Connection lifecycle.** The LISTEN conn is acquired once on
  `Init` and held until the TUI exits. On `tea.Quit`, the conn
  is released via a deferred cleanup. A dropped conn (Postgres
  restart, network blip) surfaces as a `jobsChangedMsg{err: ...}`
  and triggers the reconnect-with-backoff path; the missed
  notifications during the gap are recovered by the cold-path
  reload that fires alongside the reconnect.
- **No CLI changes.** `cmd/cli` is one-shot and exits after a
  publish; it does not read its own writes.
- **No bot changes** (ADR 0003). The Telegram bot reacts to
  operator messages, not Postgres state. If a future feature
  needs the bot to react to projection changes, it opens its
  own LISTEN.
- **Tests.** A consumer-side test asserts that a successful
  `Apply*` produces exactly one `NOTIFY` on `jobs_changed`, and
  a duplicate/no-op apply produces zero. A TUI-side test stubs
  the notification source and asserts `listJobsCmd` fires on
  each `jobsChangedMsg`.
- **Pool size.** The TUI pool size (currently default — see
  `internal/db/db.go`) gains a permanently-checked-out connection.
  Default pool size is comfortably above 1; no config change
  required.
- **Observability.** A dropped LISTEN appears in the existing
  error banner as `listen: <reason>`. There is no separate
  metric — the personal tracker has no metrics stack.

## Related decisions

- **ADR 0001** — Richer schema and event contracts. This ADR
  preserves the event-as-source-of-truth model; the notify is
  a derived wakeup, not an event.
- **ADR 0003** — Telegram bot. Bot is out of scope for the
  TUI's reconciliation problem.
- **ADR 0004** — Desktop TUI on Bubble Tea. The new tea.Cmd /
  tea.Msg pair sits next to `listJobsCmd` / `jobsLoadedMsg`.
- **ADR 0006** — Consumer error classification. The skip-count
  path is unrelated; this ADR's notify is unconditional on
  successful apply, so a skipped event does not produce a
  wakeup. Operators check the H panel for skip activity exactly
  as today.
- **ADR 0007** — Notifier dedup pattern. `processed_events`
  short-circuits a replayed event before the notify is reached,
  preserving "one apply, one wakeup".
- **ADR 0009** — Schema migrations via `golang-migrate`. No
  migration in this ADR — `pg_notify` is built-in.
- **ADR 0011** — Editing job fields via `job.edited`. The edit
  modal's post-save reload is rerouted through the LISTEN
  channel; the optimistic edit-modal staging stays.

## Related requirements

- New jobs appear in the list immediately after submit, without
  manual `R`.
- Status changes, edits, and note additions reconcile in the
  same UX moment they were keyed in.
- No new failure modes when Postgres is unavailable — the TUI
  degrades to today's behaviour.
- No new event-contract or schema surface.

## Related artifacts

- `internal/db/notify.go` (new) — `NotifyJobsChanged(ctx, tx, jobID, kind)`.
- `internal/store/store.go` — five new notify call sites in
  `ApplySubmitted`, `ApplyStatusChanged`, `ApplyNoteAdded`,
  `ApplyEdited`, `ApplyInterviewRecorded`.
- `internal/tui/cmds.go` — new `jobsChangedMsg`, new
  `listenJobsChangedCmd`.
- `internal/tui/tui.go` — `Init` batched with the listen cmd;
  new `case jobsChangedMsg` handler; immediate post-mutation
  reloads removed from `case submittedMsg` / `statusChangedMsg`
  / `editedMsg`.
- `cmd/jobtracker/main.go` — no change (the existing pool /
  reader / publisher wiring carries through).

## Related principles

- **Producers don't read on the write path.** Notify is a
  *consumer*-side concern; the TUI's publish path is untouched.
- **One mechanism per problem.** The push channel replaces the
  immediate-reload heuristic — the two are not layered.
- **Side channels for liveness, not for truth.** The Kafka
  event log remains the source of truth; the notify is a
  wakeup, not a record.
- **Same-transaction wakeups.** A notification that fires
  inside the consumer's apply transaction is delivered if and
  only if the projection commits. No false wakeups; no missed
  wakeups within the connection's lifetime.

## Notes

- **`tea.Cmd` blocks; the LISTEN loop must not.** A naive
  implementation that calls `WaitForNotification` directly from
  a `tea.Cmd` body blocks the Bubble Tea program's command
  dispatcher only for the duration of the wait — `tea.Cmd`s
  run on goroutines, so this is fine in practice. The cmd
  returns one `jobsChangedMsg`, the handler re-arms with a new
  `listenJobsChangedCmd`, and the next goroutine takes over.
  This is the pattern used for any long-poll in Bubble Tea
  (e.g. `tea.Tick` is itself a one-shot cmd that re-arms).
- **Holding the conn across cmd invocations.** The cleanest
  implementation moves the conn onto the Model and the cmd
  closes over it; re-arming uses the same conn. The sketch in
  Implications acquires-per-cmd for readability; the production
  shape is conn-on-Model.
- **`LISTEN` and pool eviction.** pgxpool evicts idle
  connections by default. The LISTEN conn must be checked out
  with `Pool.Acquire` and held — not returned to the pool —
  otherwise the pool will close it under us. Releasing the
  conn back to the pool also drops the LISTEN subscription.
  This is the single sharpest pitfall in the implementation;
  the implementer must verify the conn lifetime by writing the
  test for "kill the Store consumer's commit mid-flight and
  confirm the LISTEN side never times out".
- **`pg_notify` payload encoding.** Postgres `NOTIFY` accepts a
  string payload; non-ASCII bytes are passed through. JSON is
  safe; the payload is opaque to Postgres. The Go side parses
  with `encoding/json` after `WaitForNotification`.
- **Burst coalescing.** A series of edits in quick succession
  produces a series of notifications and a series of fetches.
  The TUI does not coalesce. On a single-operator tracker
  this is fine. A debounce window (e.g. drop the second
  `listJobsCmd` if one is already in flight) is a
  micro-optimisation deferred until a real workload shows it
  matters.
- **The "first listen, then start consuming events" ordering
  is not a concern.** The TUI's `Init` fires both
  `listJobsCmd` and `listenJobsChangedCmd` simultaneously. If
  an event commits *before* the LISTEN is registered, the
  initial `listJobsCmd` covers it. If an event commits *after*
  the LISTEN is registered, the notification covers it. There
  is no window where both miss.
- **Skipped-by-classification events.** When the Store consumer
  rejects an event via the ADR 0006 skip path, the apply tx
  rolls back; no notify fires. The operator sees the row
  unchanged and the skip-count panel grows — consistent with
  today's semantics.
- **Why not notify outside the tx?** A notify after `tx.Commit`
  is racy (the commit succeeded but the notify could fail or
  be skipped after a crash). A notify inside the tx is
  atomic with the commit and is the documented Postgres
  pattern.
- **Cold-path on reconnect.** When the LISTEN conn reconnects
  after an error, fire `listJobsCmd` once. Any events that
  committed during the connection gap are recovered by the
  fetch; subsequent events are picked up by the new LISTEN.
  This is the only place "missing notifications" can occur,
  and it is bounded by the reconnect duration.
- **Future frontends.** A web UI (SSE / WebSocket) can sit on
  the same `jobs_changed` channel by adding a tiny gateway that
  forwards notifications to connected browsers. Out of scope;
  flagged here so the channel name and payload shape are
  designed for it.
