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
