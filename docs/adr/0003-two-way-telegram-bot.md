# ADR 0003 — Two-way Telegram bot for capture and status updates

## Issue

Adding a job and updating its status currently requires SSH-ing into the home
server and running a long `podman-compose run --rm --no-deps cli ...` command.
Reminders are already delivered via Telegram (one-way), but the user must
leave Telegram and shell into the server to act on them. This friction is
worst on mobile — exactly the device where most "I just saw a JD" moments and
most reminder acknowledgements happen. Without a lower-friction interface,
saved jobs go uncaptured and reminders are silently ignored, defeating the
purpose of the system.

## Decision

Add a two-way Telegram interface. The bot lives in the existing notifier
service (or a sibling `bot` service if isolation is preferred later) and:

1. Long-polls Telegram's `getUpdates` for messages and callback queries from
   the configured `TELEGRAM_CHAT_ID`. Other senders are dropped.
2. Translates user inputs into the same Kafka events the CLI already
   produces (`job.submitted`, `job.status.changed`). The bot is purely a
   producer; Store and Scheduler are unchanged.
3. Attaches inline keyboard buttons to outgoing reminder messages
   (`✅ Applied`, `❌ Rejected`, `💤 Snooze 1d`). A button press fires a
   `job.status.changed` event (or, for snooze, a new reminder row) without
   the user typing anything.
4. Supports a small command grammar:
   - `/add <url>` — fetches the page, extracts title/company from OG tags,
     publishes `job.submitted` with `status=saved`. Missing fields are
     resolved with follow-up prompts in the same chat.
   - `/list` / `/list saved` — reads jobs from Postgres (or asks Store via
     a read API) and replies with a numbered list.
   - `/applied <n>`, `/rejected <n>`, `/offer <n>`, etc. — operate on the
     last `/list` result.

Dedup uses Telegram's monotonically increasing `update_id` plus the existing
`processed_events` ledger (`bot` consumer namespace).

## Status

Proposed.

## Group

Integration / Presentation. The bot is a new presentation channel that
integrates with the existing event bus; it adds no business logic.

## Assumptions

- Single operator. One Telegram chat ID is authoritative; no multi-tenant
  concerns.
- Telegram is already configured and reachable from the home server
  (existing notifier proves this).
- Outbound HTTPS to `api.telegram.org` is permitted; no inbound port
  needed because long-polling is used instead of webhooks.
- Volume is low (≤ a handful of interactions per day) — long-polling with
  a 25–30s timeout is more than sufficient.
- The bot can read jobs directly from Postgres for `/list`. This is the
  first component besides Store and Scheduler to read the DB; acceptable
  for v1 given a single deployment unit, revisitable if/when a read API
  is introduced.

## Constraints

- Chat-ID filter is the only authentication. The bot must hard-reject
  updates from any other chat.
- The bot must be idempotent against Telegram redelivery. `update_id`
  goes into `processed_events` under consumer `bot`.
- No new public ports, no TLS termination, no DNS records.
- URL parsing for `/add <url>` is best-effort. When OG tags / `<title>`
  are missing, the bot asks the user instead of failing.
- Bot crash recovery: any partially-handled callback (status change
  published but `answerCallbackQuery` not sent) must converge — the event
  is the source of truth; the visual ack is cosmetic.

## Positions

Alternatives considered:

1. **Two-way Telegram bot** (this decision).
2. **HTTP API + iOS/Android share-sheet shortcut**, exposed over Tailscale.
   Captures the "browsing a JD" moment well via the system share sheet.
3. **CLI `list` command + local `jt` shell wrapper + Tailscale SSH.**
   Cheapest to build; doesn't materially help on mobile.
4. **Browser bookmarklet → HTTP API.** Desktop-only; brittle across JD
   sites; superseded by (2) for the same use case.
5. **Full web UI / PWA.** Highest ceiling, much more work; punt until
   (1) is shown insufficient.

## Argument

The Telegram bot has the highest leverage per unit of effort for this
codebase and this user:

- **No new infra.** Long-polling needs no inbound port, no TLS, no DNS,
  no auth scheme beyond the existing `TELEGRAM_CHAT_ID` filter. Compare
  to options (2)/(5), which require Tailscale or public exposure.
- **No new app to install.** Telegram is already on every device the user
  carries.
- **One-tap reminder actions.** The reminder is the moment the user is
  most willing to act. Inline buttons collapse "open SSH, type podman
  command" into a single tap *inside the message that pinged them*. No
  other option in the list achieves this.
- **Architecturally cheap.** The bot is a Kafka producer that publishes
  the exact same events the CLI publishes today. Store and Scheduler are
  unchanged. The blast radius of the change is one service.
- **Reuses existing patterns.** Idempotency via `processed_events`,
  config via env vars, deployment via compose — all already in place.

Cost: roughly one day of focused work, mostly the long-poll loop, the
callback-query handler, and the `/add` URL fetcher.

The main thing this option does *not* solve well is browsing a JD on
desktop and capturing it in-place — that's where option (2) or a future
TUI shines. The bot is sufficient on its own to ship; (2) is a worthwhile
follow-up, not a prerequisite.

## Implications

- New dependency on a Telegram bot library (`go-telegram/bot` or
  `github.com/go-telegram-bot-api/telegram-bot-api`) or hand-rolled
  `getUpdates` calls.
- The notifier process gains a long-running poll loop alongside its Kafka
  consumer. If this coupling becomes awkward, split into a `bot` service
  later; the wire protocol (Kafka events) stays the same.
- The bot becomes a Kafka producer — needs producer client wiring like
  CLI has.
- A new `bot` namespace enters `processed_events` (for `update_id`
  dedup).
- Adds a Postgres read dependency to the bot (for `/list`). The bot must
  receive `DATABASE_URL` in compose.
- Reminder message format changes (now has buttons). Any external
  consumer of those messages — there is none today — would need to
  ignore the callback payload.
- `Snooze` introduces a new reminder kind (`snooze`) or reuses
  `followup_saved` with a fresh `due_at`. Recommend the latter for v1 to
  avoid schema changes.
- Operational: bot failure is silent unless the user notices reminders
  aren't ack-able. A simple liveness log line per poll cycle is enough
  for v1.

## Related decisions

- **ADR 0001** — Richer schema and event contracts. Strict
  prerequisite: the bot reads and publishes against the redesigned
  schema and event set.
- **ADR 0002** — Shared `internal/jobclient` library. Strict
  prerequisite: the bot uses `jobclient.Publisher` and
  `jobclient.Reader`.
- **ADR 0004** — Desktop TUI in Bubble Tea for power-user job
  management. Complements the bot on macOS; not a replacement.
- **Future ADR** — Read API in front of Postgres if multiple non-Store
  components need read access (currently only the bot would need it).
- **Existing** — Telegram one-way delivery, implemented in
  `cmd/notifier/main.go`.

## Related requirements

- Reduce capture friction so saved-job hit-rate goes up.
- Make reminders actionable in the moment they fire.
- Avoid exposing any service to the public internet.
- Keep the home-server-as-deployment-target assumption intact.

## Related artifacts

- `cmd/notifier/main.go` — current Telegram sender; likely host of the
  new bot loop.
- `cmd/cli/main.go` — reference for event production patterns.
- `internal/events/` — event schemas the bot will produce.
- `internal/db/schema.sql` — `processed_events`, `reminders`, `jobs`
  tables the bot reads/writes.
- `compose.yml` — needs `DATABASE_URL` added to the notifier/bot
  service.

## Related principles

- **Event-driven core, thin interfaces.** New interfaces add producers,
  not new sources of truth.
- **No service writes to another's tables.** The bot writes only via
  Kafka events; reads from `jobs` are read-only.
- **Local-first / no public surface.** Outbound-only is preferred over
  inbound listeners.

## Notes

- Long-poll timeout: 25–30s. Cancellable via context for clean shutdown.
- Inline button callbacks must be acknowledged with
  `answerCallbackQuery` to clear Telegram's spinner; this is cosmetic
  but visible.
- `/add <url>` autofill: try OG tags (`og:title`, `og:site_name`),
  fall back to `<title>` minus boilerplate, fall back to asking the
  user.
- Open question: should `/list` results expire (i.e., the numbering is
  only valid until the next `/list`)? Recommend yes, stored in-memory
  per chat — keeps state simple.
- Snooze semantics: button = `due_at = now + 1d` for v1; no separate
  reminder kind.
