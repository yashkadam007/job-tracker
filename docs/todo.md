# Future work

Things deferred from accepted ADRs or noticed in passing. Not scoped, not
scheduled — promote to an ADR or an issue when picking one up.

## Telegram bot — rate limiting & token hygiene

Source: ADR 0003.

Today the bot's only authentication is the `TELEGRAM_CHAT_ID` filter:
updates from any other chat are dropped. This is sufficient for the
single-operator case, but it leaves two gaps worth addressing later:

- **No rate limiting on inbound updates.** A flood of messages (whether
  from a misbehaving client, an accidental loop, or a leaked token being
  probed) would be processed one-by-one with no backpressure. Consider:
  - per-chat token bucket on `getUpdates` handling,
  - cap on `/add` URL fetches per minute (also protects the outbound
    side from being mistaken for a scraper),
  - circuit-break on repeated parse failures from the same chat.
- **No token rotation story.** The bot token is a long-lived secret in
  env. If it leaks, the attacker can impersonate the bot to Telegram but
  cannot message the operator's chat. Still worth:
  - documenting rotation steps (BotFather → revoke → update env →
    restart `bot` service),
  - considering whether the token should live in a secret store rather
    than `compose.yml` env.

Neither is urgent given single-operator usage and outbound-only network
posture, but both should land before the bot is ever exposed to a second
user or a less trusted environment.

## Unified `/health` endpoint across services

Source: ADR 0006.

ADR 0006 introduces a per-consumer admin HTTP server (`:9090` for store,
`:9091` for scheduler) exposing `/skip-count` and `/healthz`. Other
services (`bot`, `notifier`) have no health surface at all.

For v2: consolidate into a single `/health` endpoint per service that
returns detailed status — uptime, last-processed offset (where
applicable), skip counts by class, downstream dependency reachability
(Postgres, Kafka), and any service-specific liveness signals. The TUI's
status panel would then read one endpoint per service instead of
ad-hoc per-feature endpoints.

Out of scope for ADR 0006's v1 because the minimum useful surface there
is just the skip counter; the broader health view is a separate
concern.

## Structured logging via `log/slog`

Source: ADR 0006 (open question on log-rate-limiting), noticed in passing.

Today every service uses the stdlib `log` package. The error path that
matters most for debugging — `consumeradmin.LogSkip` — already emits a
structured JSON line (level, event, topic, partition, offset, class,
error, payload_b64), so the poison-message replay story is intact. The
gap is everywhere else: per-event success lines in `cmd/store/main.go`
and `cmd/scheduler/main.go` share the same writer and level as fetch
errors and retry warnings, so there's no way to suppress chatter
without also suppressing signal.

Not urgent at single-operator volume (a handful of events per day), but
worth doing once either of the following bites:

- the success chatter starts drowning real warnings in `docker logs`,
- the ADR 0006 open question on log-coalesce ("N more of the same in the
  next Ns") becomes a real need.

When picked up:

- switch `LogSkip` to `slog.Error` with typed attrs — same JSON shape on
  the wire, less hand-rolled marshaling,
- introduce a `JOB_TRACKER_LOG_LEVEL` env (default `info`) so the
  per-event success lines can be demoted to `debug` and silenced in
  steady state without losing them during incidents,
- keep `log.Fatalf` for the ADR 0006 `default` crash path — the
  container-restart signal is the point, structured framing isn't.

Explicitly out of scope: adding a third-party logging library or a log
aggregator. `log/slog` is stdlib since Go 1.21; nothing else is needed.
