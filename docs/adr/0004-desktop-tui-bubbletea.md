# ADR 0004 — Desktop TUI (Bubble Tea) for session-based job triage

## Issue

ADR 0003 makes mobile and reactive flows ergonomic — capturing a job
from anywhere and reacting to reminders with one tap. It is, however,
poorly suited to the "weekly triage" session: open my laptop, see every
saved job, mark these three applied, snooze two, drop one, take notes
on the rest. That kind of bulk, keyboard-driven work in a chat UI is
slow and visually noisy.

The user works primarily on macOS for focused job-search sessions and
wants a snappy local tool that is keyboard-first, mouse-free, and shows
the full picture at a glance. They specifically asked for a Go +
Bubble Tea TUI. The architectural question is *how* it should plug into
the existing event-driven system without growing a separate backend or
duplicating frontend logic.

## Decision

Build `cmd/tui` using
[Bubble Tea](https://github.com/charmbracelet/bubbletea) with the
`bubbles` (widgets) and `lipgloss` (styling) companion libraries.

Architecture:

- Single Go binary, installed locally on macOS (`go install ./cmd/tui`).
  No daemon, no service to run.
- Talks directly to the home-server's Postgres and Kafka **over
  Tailscale** (or LAN if the laptop is at home). No API tier.
- Uses `internal/jobclient.Publisher` and `internal/jobclient.Reader`
  from ADR 0002 — zero duplicated transport code.
- Stateless: each launch reads fresh state from Postgres. No local
  cache to invalidate.

UX shape (v1):

- A list view: paginated table of jobs (`bubbles/table`) with columns
  `status • title • company • last_event`. Optional status filter
  pill at the top.
- A detail view: selected job's full info plus its pending reminder
  (if any).
- Keyboard hotkeys for status transitions on the highlighted row:
  `a`=applied, `r`=rejected, `o`=offer, `w`=withdrawn, `s`=snooze 1d,
  `n`=new (modal: prompts for URL/title/company), `/`=search, `q`=quit.
- Each transition publishes a `job.status.changed` event via
  `jobclient.Publisher`. The UI then re-queries
  `jobclient.Reader.List` to reflect the new state — no optimistic
  UI for v1.

Out of scope for v1:

- Live updates from Kafka (so a Telegram-bot action elsewhere refreshes
  the TUI automatically). Considered for v2.
- Editing title/company/notes. v1 is status-and-snooze only;
  metadata edits stay rare enough to do via `cli` or `bot`.
- Multi-select bulk actions. Add when single-row triage proves
  insufficient.

## Status

Proposed.

## Group

Presentation.

## Assumptions

- The Mac can reach the home server's Postgres (`:5432`) and Kafka
  (`:9092`). Tailscale is the recommended path; same-LAN works too.
- Single user, single device. No multi-tenant or auth concerns;
  network-level access (tailnet) is sufficient.
- v1 is desktop-only (macOS); the same binary should compile and run
  on Linux but isn't a release target.
- `internal/jobclient` (ADR 0002) is in place before this is built.
- Triage volume is small (dozens, not thousands of saved jobs) — a
  full-table re-query after each action is fine.

## Constraints

- Must not require a new backend service or HTTP API. Reuses existing
  Postgres + Kafka.
- Must reuse `internal/jobclient` for all read/publish operations —
  no copy/paste of `kgo` wiring or SQL.
- Must respect the event-driven contract: every mutation is published
  as an event; no direct UPDATE on `jobs` from the TUI.
- Must shut down cleanly (Bubble Tea + signal context) so the
  long-lived Kafka producer flushes pending records.

## Positions

Alternatives considered:

1. **Bubble Tea TUI talking directly to Postgres + Kafka over
   Tailscale** (this decision).
2. **Web UI served by the home server**, accessed from the Mac
   browser. Rejected for v1: adds a service to operate, an auth story
   to design, and a frontend toolchain to maintain — none of which the
   user asked for. Worth reconsidering if a second user appears.
3. **Native macOS app (SwiftUI).** Rejected: Swift is outside the
   project's language; productivity per hour is much lower than Bubble
   Tea for a tool this small.
4. **`fzf` + shell scripts.** Rejected: limited interactivity, painful
   to evolve, no per-row context.
5. **Extend the existing `cli` with a `list` subcommand and call it
   done.** Useful regardless (we'll likely add it), but not a
   replacement for an interactive triage view.

## Argument

- **Bubble Tea is Go-native** — slots into the existing build
  (`go build ./...`) and team's language. Mature, well-documented,
  active.
- **Direct DB + Kafka over Tailscale is the simplest viable
  architecture** for one user on a known device. No API design, no
  TLS, no DNS, no auth. The trust boundary is "you're on my
  tailnet."
- **Reuses `internal/jobclient`** — Publisher for writes (as Kafka
  events), Reader for the list/detail/search queries. Zero duplicated
  code.
- **Stateless launch** means there's never a "the TUI got out of sync
  with reality" failure mode. Every render is a fresh read.
- **Keyboard-first hotkeys** make weekly triage near-instant: one key
  per status transition is faster than any chat or web UI.
- **Composes with ADR 0003** rather than competing with it. Telegram
  for "everywhere", TUI for "at my desk". Both publish the same
  events; the system stays a single source of truth.

The chief downside is that the TUI assumes network reachability of
the home server. Offline use is not supported. Given the tool's
session-based nature (you're sitting down to job-hunt, you have wifi),
this is acceptable.

## Implications

- **`KAFKA_ADVERTISED_LISTENERS` must include a non-`localhost`
  external hostname.** Today `compose.yml:23` advertises
  `EXTERNAL://localhost:9092`. A client on the Mac connecting to
  `homeserver:9092` will be redirected by Kafka to "localhost:9092"
  and try to connect to its own loopback — and fail. Either:
  - Change the EXTERNAL advertised listener to the home server's
    tailnet hostname (e.g.,
    `EXTERNAL://homeserver.tailnet.ts.net:9092`), or
  - Add a third listener (e.g., `TAILNET://0.0.0.0:9094`) advertised
    as the tailnet hostname, leaving local-host clients undisturbed.

  Recommend the second option to avoid breaking host-side dev
  workflows that connect via `localhost:9092`. This is the single
  most important compose change implied by this ADR and worth
  capturing as a sub-task.
- Postgres on Tailscale needs `listen_addresses = '*'` (default
  in the official image is `*`) and a `pg_hba.conf` rule that allows
  the tailnet CIDR. The current compose just exposes `5432:5432` on
  the host — Tailscale traffic will arrive on that interface, so as
  long as the host is on tailnet, no further DB-side changes are
  needed.
- New cmd directory `cmd/tui/`. Build via existing `go build ./...`.
  Distribution is `go install ./cmd/tui` to `~/go/bin/jt` on the
  Mac.
- TUI config: env vars `KAFKA_BOOTSTRAP`, `DATABASE_URL` (same names
  as services). A small `~/.config/job-tracker/tui.toml` is overkill
  for v1; defer.
- Bubble Tea pulls in `github.com/charmbracelet/bubbletea`,
  `bubbles`, `lipgloss`. All MIT, all stable.
- Operational: the TUI is a long-lived Kafka producer per session.
  Producer client must close cleanly on `q` / SIGINT to flush.

## Related decisions

- **ADR 0001** — Richer schema and event contracts. Strict
  prerequisite: the TUI reads and publishes against the redesigned
  schema and event set.
- **ADR 0002** — Shared `internal/jobclient` library. Strict
  prerequisite.
- **ADR 0003** — Telegram bot. Covers mobile/reactive flows. The TUI
  is its desktop/proactive complement; both publish the same events.
- **Future** — Live updates from Kafka in the TUI (consumer-side).
  Deferred to v2.
- **Future** — Compose change to advertise an off-box Kafka listener.
  Worth its own small ADR or a runbook entry; capturing here for now.

## Related requirements

- Fast desktop triage of saved jobs.
- Keyboard-driven workflow (no mouse, no chrome).
- Reuse the event pipeline; no new persistence path.
- No new public exposure of services.

## Related artifacts

- `cmd/tui/` (new)
- `internal/jobclient/` (from ADR 0002; the TUI is a primary consumer)
- `compose.yml` — `KAFKA_ADVERTISED_LISTENERS` likely needs an
  additional entry (see Implications).
- `docs/runbook.md` — should grow a "Install the TUI on your Mac"
  section once shipped.

## Related principles

- **Event-driven core, thin frontends.** TUI is a producer; events
  remain the contract.
- **Local-first / no public surface.** Tailnet, not internet.
- **One source of truth.** No local cache, no optimistic UI; every
  render is reality.

## Notes

- Bubble Tea programs are typically structured as a single `Model`
  with `Init/Update/View` methods. For this app, split into a parent
  model that owns the list/detail subview state.
- Reads in `Update` must be async (`tea.Cmd`), not blocking; a slow
  tailnet should not freeze the UI.
- `lipgloss` styles: keep a small palette (status colours: saved →
  blue, applied → yellow, offer → green, rejected → grey). Use the
  terminal's truecolor if available.
- "Snooze 1d" semantics: insert a new reminder row with
  `due_at = now + 24h`, kind = `followup_saved` (reuses existing
  scheduler logic, no schema change).
- Search (`/`) is client-side over the current list result for v1.
  Server-side LIKE search can come later if the list grows large.
- The advertised-listener compose change is genuinely the trickiest
  thing about shipping this; test with a host-side `kafka-console-
  producer` first to confirm the new listener works before debugging
  the TUI itself.
