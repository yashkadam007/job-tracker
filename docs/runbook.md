# Runbook — Testing job-tracker on the Ubuntu server

A copy-pasteable walkthrough for bringing the stack up on a fresh
machine, running the full pipeline, and inspecting state. Everything
runs in containers — no Go toolchain needed on the host.

---

## 0. Prerequisites

Check on the Ubuntu host once:

```bash
podman --version            # any recent version
podman-compose --version    # or: podman compose version
psql --version              # only needed if you want to query Postgres directly
```

If `podman-compose` is missing:

```bash
sudo apt install podman-compose          # ubuntu 23.10+
# OR:
pipx install podman-compose
```

Rootless podman is fine; nothing here needs root.

> Newer podman ships `podman compose` (space, no hyphen) as a drop-in.
> Both work — substitute whichever you have.

---

## 1. Clone & build images

```bash
git clone <your-github-url> ~/job-tracker
cd ~/job-tracker
podman-compose build
```

The build step compiles all five binaries (`cli`, `store`, `scheduler`,
`notifier`, `bot`) into a single image and takes ~1–2 minutes on first
run. Subsequent builds are cached.

---

## 2. Start the infra (Kafka + Postgres + Kafka UI)

```bash
podman-compose up -d kafka postgres kafka-ui
podman-compose ps
```

Expected: three containers, all `Up (healthy)` after ~20 seconds.
Kafka takes the longest because it formats its log dir on first boot.

Tail logs if a container is unhealthy:

```bash
podman-compose logs -f kafka
podman-compose logs -f postgres
```

Open Kafka UI in a browser (port-forward from your laptop if needed):

```
http://localhost:8088
```

You should see one cluster ("job-tracker"), no topics yet.

---

## 3. Create topics

```bash
podman-compose run --rm --no-deps cli ensure-topics
```

Expected output:

```
created: job.interview.recorded
created: job.note.added
created: job.reminder
created: job.status.changed
created: job.submitted
```

Re-running prints `exists: …` for each — the command is idempotent.

In Kafka UI → Topics, you should now see all five.

---

## 4. Run the services

Start all four as detached containers:

```bash
podman-compose up -d store scheduler notifier bot
```

> The `bot` service needs `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`
> in your env (or `.env`). It will crash-loop without them. See §10
> for how to obtain both, or skip `bot` from the list above if you're
> only testing the CLI pipeline.

Watch the logs (one terminal per service, or combined):

```bash
podman-compose logs -f store
podman-compose logs -f scheduler
podman-compose logs -f notifier
podman-compose logs -f bot

# or all at once:
podman-compose logs -f store scheduler notifier bot
```

Expected startup lines:

- **store:** `store: consuming job.submitted, job.status.changed, job.note.added, job.interview.recorded (group=store)`
- **scheduler:** `scheduler: group=scheduler, saved=168h0m0s, applied=336h0m0s, poll=30s`
- **notifier:** `notifier: consuming job.reminder (group=notifier)`
- **bot:** `bot: long-polling Telegram (chat_id=<id>, timeout=25s)`

### Smoke-test timings

To shorten the scheduler's delays for a smoke test, set overrides in
`.env` (or your shell) before starting the scheduler:

```bash
cat >> .env <<'EOF'
REMINDER_SAVED_SECONDS=10
REMINDER_APPLIED_SECONDS=15
REMINDER_POLL_SECONDS=2
EOF

podman-compose up -d --force-recreate scheduler
```

Startup line should now read `saved=10s, applied=15s, poll=2s`.
Remove the lines from `.env` and recreate again to restore defaults.

---

## 5. Smoke tests

### 5a. Happy path

```bash
podman-compose run --rm --no-deps cli add \
  --url https://example.com/job/1 \
  --title "Senior Backend Engineer" \
  --company "Acme"
```

Expected reactions (watch with `podman-compose logs -f …`):

- **CLI:** `published: Senior Backend Engineer @ Acme (saved)` then
  `job_id: <uuid>` (save this — every follow-up command needs it).
- **store:** `submitted: Senior Backend Engineer @ Acme (saved)`
- **scheduler:** `scheduled reminder for <job_id> (saved)`
- **notifier:** (~10s later, with smoke-test env)
  `REMINDER followup_saved — Senior Backend Engineer @ Acme (saved, due ...) :: Still interested? ...`

The bare three-flag form above still works. To exercise the richer
metadata path, pass any of the optional flags:

```bash
podman-compose run --rm --no-deps cli add \
  --url https://example.com/job/2 \
  --title "Staff Engineer" --company "Globex" \
  --work-mode remote --seniority staff --source linkedin \
  --tech-tag go --tech-tag postgres --custom-tag dream_company \
  --comp-min 180000 --comp-max 230000 --comp-currency USD \
  --priority 5
```

Inspect the row to confirm the typed columns + arrays landed:

```sql
SELECT job_id, url, work_mode, seniority, tech_tags, custom_tags, priority,
       comp_min, comp_max, comp_currency
  FROM jobs WHERE url = 'https://example.com/job/2';
```

### 5b. Status change

Use the `job_id` printed by `add` (URLs aren't the identity anymore):

```bash
podman-compose run --rm --no-deps cli status <job-id> applied
```

Expected:

- **store:** `status: <job-id> → applied`
- **scheduler:** `reminders updated for <job-id> (applied)`
- The old "saved" reminder is cancelled; a new "applied" reminder is scheduled.
- **notifier:** within ~15s, fires the `followup_applied` reminder.

### 5c. Terminal status (no new reminder)

```bash
podman-compose run --rm --no-deps cli status <job-id> rejected
```

Expected:

- store updates the row.
- scheduler cancels pending reminders, schedules nothing new.
- notifier stays quiet.

### 5d. Note + interview pipeline

```bash
podman-compose run --rm --no-deps cli note add \
  --job-id <job-id> \
  --body "Recruiter said they'll decide by Friday."
```

Expected store log: `note: added for <job-id>`.
Verify the row:

```sql
SELECT job_id, body, created_at FROM job_notes
 ORDER BY created_at DESC LIMIT 5;
```

Then schedule and update an interview. **Save the printed `interview_id`** —
`interview update` needs it (the CLI doesn't persist any local state).

```bash
podman-compose run --rm --no-deps cli interview schedule \
  --job-id <job-id> \
  --round phone_screen \
  --scheduled-at 2026-06-01T15:00:00Z \
  --interviewer "Alex" --interviewer "Sam" \
  --notes "30 min screening"

# copy interview_id from the output, then:
podman-compose run --rm --no-deps cli interview update \
  --interview-id <id-from-above> \
  --job-id <job-id> \
  --completed-at 2026-06-01T15:35:00Z \
  --outcome passed
```

Verify the upsert merged the two events (round + interviewers from
schedule, completed_at + outcome from update — neither got wiped):

```sql
SELECT interview_id, job_id, round, scheduled_at, completed_at, outcome, interviewers
  FROM job_interviews ORDER BY updated_at DESC LIMIT 5;
```

Status transitions also leave a trail now — every `cli status …` writes
to `job_status_history`:

```sql
SELECT job_id, status, changed_at FROM job_status_history
 ORDER BY changed_at;
```

### 5e. Idempotency check

Restart the store container:

```bash
podman-compose restart store
podman-compose logs -f store
```

You'll see it consume the old events again. Each one prints
`submitted: dup event_id=... skipped` / `status: dup event_id=... skipped`.
**No duplicate rows or status flips in Postgres** — that's the
`processed_events` ledger doing its job.

---

## 6. Inspecting state

### Postgres

From the host (if `psql` is installed):

```bash
psql postgres://jobtracker:jobtracker@localhost:5432/jobtracker
```

Or use the container's `psql`:

```bash
podman exec -it job-tracker-postgres psql -U jobtracker -d jobtracker
```

Useful queries:

```sql
\dt                                                -- list tables
SELECT * FROM jobs;
SELECT * FROM reminders ORDER BY id DESC LIMIT 10;
SELECT consumer, count(*) FROM processed_events GROUP BY consumer;

-- "what's about to fire?"
SELECT r.id, r.job_id, j.url, r.kind, r.due_at
  FROM reminders r JOIN jobs j ON j.job_id = r.job_id
 WHERE r.fired_at IS NULL AND NOT r.cancelled
 ORDER BY r.due_at;

-- transitions over time (source of truth for time-based analytics)
SELECT job_id, status, changed_at FROM job_status_history
 ORDER BY changed_at DESC LIMIT 20;

-- pipeline state per job
SELECT job_id, round, scheduled_at, completed_at, outcome
  FROM job_interviews ORDER BY scheduled_at DESC NULLS LAST LIMIT 20;

-- notes timeline for one job (resolve URL → job_id first if you only
-- have the URL)
SELECT n.created_at, n.body
  FROM job_notes n JOIN jobs j ON j.job_id = n.job_id
 WHERE j.url = 'https://example.com/job/1'
 ORDER BY n.created_at;
```

### Kafka UI (http://localhost:8088)

- **Topics → job.submitted → Messages**: the JSON events you produced.
- **Consumer Groups**: should show `store`, `scheduler`, `notifier` each with a current offset and a lag of 0 (or near 0) when caught up.
- **Topics → job.reminder → Messages**: the reminder events the scheduler published.

The two consumer groups (`store` + `scheduler`) reading the same
two topics is the "fan-out" pattern in action — proof that consumer
groups are independent.

---

## 7. Stopping

```bash
podman-compose stop                 # stop containers, keep volumes
podman-compose down                 # remove containers, keep volumes
podman-compose down -v              # ALSO delete Kafka logs + Postgres data
```

The `-v` form is the easy "reset everything" — handy while iterating.

---

## 8. Troubleshooting

### Containers won't go healthy

```bash
podman-compose logs kafka | tail -50
```

Most common cause on a fresh boot: Kafka can't format `/var/lib/kafka/data`
because the volume has stale data from a previous run with different
config. Fix: `podman-compose down -v && podman-compose up -d`.

### App service crash-loops at startup

```bash
podman-compose logs store
```

Usually one of:

- Kafka or Postgres isn't healthy yet — wait for them, then
  `podman-compose restart store scheduler notifier`.
- The image is stale after a code change — rebuild:
  `podman-compose build && podman-compose up -d --force-recreate`.

### CLI hangs on `ensure-topics`

Kafka isn't reachable from the `cli` container. Check that
`kafka` is healthy (`podman-compose ps`) and that you're using
the compose-defined `cli` service (which already points at
`kafka:29092`), not a stray host invocation.

### "missing go.sum entry"

You changed a dependency without running `go mod tidy`. Fix it
on your dev machine, commit `go.sum`, push, then rebuild the
image on the server: `podman-compose build`.

### Notifier never fires

Three things to check, in order:
1. Is the reminder row actually in the DB?
   `SELECT * FROM reminders WHERE fired_at IS NULL;`
2. Is its `due_at` in the past?
   If not, you're just waiting. Drop `REMINDER_SAVED_SECONDS` for testing.
3. Is the scheduler ticking?
   Its log should show `fired reminder id=... job_id=... url=... kind=...`
   when it fires.

### "topic already exists" error on `ensure-topics`

That's expected behaviour, printed as `exists: <topic>`. Not an error.

### `reminders` won't create on an old DB volume

The schema now declares `reminders.job_id REFERENCES jobs(job_id)`.
`CREATE TABLE IF NOT EXISTS` is a no-op against a pre-existing
`reminders` table that lacks the FK (or that still keys on `url`),
so the new shape silently doesn't land. This is greenfield (per
ADR 0001) — wipe and recreate:
`podman-compose down -v && podman-compose up -d`.

### Want to clear a single consumer group's offset?

```bash
podman exec -it job-tracker-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group store --reset-offsets --to-earliest --all-topics --execute
```

Replays every event for that consumer group from the start. The
`processed_events` ledger turns the replay into a no-op for already-seen
events, which is exactly what you want — that's the test that
idempotency works end-to-end.

---

## 9. Telegram bot — end-to-end

The `bot` service (ADR 0003) gives you a Telegram chat that captures
jobs (`/add <url>`), lists them (`/list`), and lets you tap inline
buttons on reminders to mark them Applied / Rejected / Snoozed.

### 9a. One-time: create the bot and find your chat ID

1. **Create the bot.** Message [@BotFather](https://t.me/BotFather) →
   `/newbot` → follow prompts. Save the token (looks like
   `123456789:AAExampleTokenFromBotFather`).
2. **Send your bot any message** (e.g. `hi`). The bot won't reply yet,
   but Telegram now has an `update` queued for it.
3. **Find your chat ID.** From your laptop:
   ```bash
   curl "https://api.telegram.org/bot<TOKEN>/getUpdates"
   ```
   Look for `"chat":{"id":123456789,...}` in the JSON. That number is
   your `TELEGRAM_CHAT_ID`. (For DMs it's positive; for groups it's
   negative — both work.)

### 9b. Put the credentials in `.env`

```bash
cat >> .env <<'EOF'
TELEGRAM_BOT_TOKEN=123456789:AAExampleTokenFromBotFather
TELEGRAM_CHAT_ID=123456789
EOF

podman-compose up -d --force-recreate notifier bot
```

Both services read the same two env vars — notifier uses them to send
reminders (now with inline buttons), bot uses them to receive replies
and callbacks.

`bot`'s startup log should show:

```
bot: long-polling Telegram (chat_id=123456789, timeout=25s)
```

If it logs `TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required`,
the env vars didn't land — double-check `.env` and re-run with
`--force-recreate`.

### 9c. `/add` — capture a job from a URL

In your Telegram chat with the bot:

```
/add https://example.com/job/42
```

Expected: the bot replies `Fetching … ` and then either:
- **All metadata extracted:** `Saved: Senior Backend Engineer @ Acme … job_id: <uuid>`
- **Missing title:** `What's the job title?` — your next plain message becomes the title.
- **Then missing company:** `Which company?` — your next plain message becomes the company.

Once both are filled in, the bot publishes `job.submitted`. Verify:

```bash
podman-compose logs --tail=20 store     # submitted: <title> @ <company> (saved)
podman-compose logs --tail=20 scheduler # scheduled reminder for <job_id> (saved)
```

```sql
SELECT job_id, url, title, company, status FROM jobs ORDER BY first_seen_at DESC LIMIT 5;
```

> Sending any `/`-command while a `/add` is mid-flow cancels the
> pending prompt — useful if you want to bail out.

### 9d. `/list` and numeric shortcuts

```
/list
```

Bot replies with a numbered list of the last 20 jobs:

```
1. Senior Backend Engineer @ Acme (saved)
   https://example.com/job/42
2. Staff Engineer @ Globex (applied)
   https://example.com/job/2
…
```

Numbering is in-memory per chat. Mark job 1 as applied:

```
/applied 1
```

Bot replies `Senior Backend Engineer @ Acme → applied`. Verify the
status change landed:

```bash
podman-compose logs --tail=10 store     # status: <job_id> → applied
```

Same shape: `/rejected <n>` and `/offer <n>`. Filter by status:

```
/list saved
```

> The list expires on the *next* `/list` — there's no durable
> numbering across restarts. Re-run `/list` if you've lost your place.

### 9e. Reminder buttons — Applied / Rejected / Snooze 1d

Shorten the reminder delays (§4 "Smoke-test timings") so you don't
have to wait a week, then add a fresh job:

```
/add https://example.com/job/buttons
```

After `REMINDER_SAVED_SECONDS` elapses, the notifier sends a reminder
message to the same chat — but now with three inline buttons:

```
🔔 followup_saved — <title> @ <company>
Status: saved
Due: ...
[✅ Applied] [❌ Rejected] [💤 Snooze 1d]
```

- **Tap ✅ Applied:** bot publishes `job.status.changed` (→ applied),
  scheduler cancels old reminders and schedules a `followup_applied`
  one. Bot replies `✓ job <id> → applied`.
- **Tap ❌ Rejected:** same flow → status=rejected, no new reminder.
- **Tap 💤 Snooze 1d:** bot inserts a fresh `followup_saved`
  reminder one day out (the bot's one direct DB write — by design,
  per ADR 0003). Bot replies `💤 job <id> — snoozed 1d`.

Verify a snooze actually scheduled a new reminder:

```sql
SELECT id, job_id, kind, due_at, fired_at, cancelled
  FROM reminders ORDER BY id DESC LIMIT 5;
```

### 9f. Other-chat rejection (security smoke test)

The bot only accepts messages from the configured `TELEGRAM_CHAT_ID`.
If you DM the bot from a *different* Telegram account, **nothing
happens** — no reply, no log line beyond the bare `getUpdates` poll.
That's the chat-ID allowlist in action; it's the bot's only auth.

### 9g. Idempotency check (bot)

Restart the bot to force Telegram to redeliver any unconfirmed
updates:

```bash
podman-compose restart bot
podman-compose logs -f bot
```

Updates you'd already handled before the restart get dropped silently
(matched against `processed_events` with `consumer='bot'`,
`event_id='tg-update-<update_id>'`). To eyeball the ledger:

```sql
SELECT consumer, count(*) FROM processed_events GROUP BY consumer;
-- expect a row with consumer='bot' equal to the number of Telegram
-- updates you've sent the bot.
```

---

## 10. Desktop TUI on the Mac

The `jobtracker` binary (ADR 0004) is the keyboard-driven triage tool
for weekly review sessions — runs locally on macOS, talks to the home
server's Postgres + Kafka over Tailscale. Single Go binary, no daemon,
stateless on launch.

### 10a. One-time: expose Kafka on the tailnet

The home server's compose already declares a third Kafka listener
(`TAILNET://0.0.0.0:9094`) advertised under
`${KAFKA_TAILNET_HOSTNAME:-homeserver.tailnet.ts.net}:9094`. The
default placeholder hostname will not resolve from the Mac — set
your actual MagicDNS name (Tailscale Admin → Machines → the home
server) and recreate Kafka:

```bash
# On the Ubuntu host:
cat >> .env <<'EOF'
KAFKA_TAILNET_HOSTNAME=ubuntu-home.tailXXXX.ts.net
EOF

podman-compose up -d --force-recreate kafka
```

Postgres needs nothing extra — `5432:5432` already binds to all
host interfaces, and Tailscale traffic arrives on the same socket.

### 10b. Verify the TAILNET listener from the Mac

From your Mac, before wiring the TUI itself (per ADR Implications):

```bash
brew install kcat                                  # one-time
kcat -b ubuntu-home.tailXXXX.ts.net:9094 -L         # list metadata
```

Expected: a `Metadata for all topics` block listing the five job
topics with brokers advertised as
`ubuntu-home.tailXXXX.ts.net:9094`. If kcat prints
`Connection refused` or `Name or service not known`, the listener
or your tailnet hostname is misconfigured — fix this *before* the
TUI, since its failure mode is much less informative.

Also confirm Postgres is reachable:

```bash
psql "postgres://jobtracker:jobtracker@ubuntu-home.tailXXXX.ts.net:5432/jobtracker?sslmode=disable" \
  -c 'SELECT count(*) FROM jobs;'
```

### 10c. Install the TUI

You need Go ≥ 1.25 on the Mac:

```bash
brew install go
```

From a fresh clone of the repo on your Mac (the TUI does not run in
a container — it's a local binary):

```bash
cd ~/job-tracker
go install ./cmd/jobtracker
# the binary lands at ~/go/bin/jobtracker; make sure that's on $PATH
```

Add the two namespaced env vars to your shell rc — the TUI reads
its config from there because the rc is the only "global namespace"
shared with other tools on the machine (per ADR 0004):

```bash
cat >> ~/.zshrc <<'EOF'
export JOB_TRACKER_KAFKA_BOOTSTRAP=ubuntu-home.tailXXXX.ts.net:9094
export JOB_TRACKER_DATABASE_URL="postgres://jobtracker:jobtracker@ubuntu-home.tailXXXX.ts.net:5432/jobtracker?sslmode=disable"
EOF

source ~/.zshrc
```

Then launch:

```bash
jobtracker
```

Expected: the alt-screen view with the title bar, a `filter: any`
pill, the table of jobs ordered by `last_event_at DESC`, a detail
panel under it, and a help line of hotkeys. If the table is empty,
add a row through the CLI / bot first (§5 or §9).

### 10d. Smoke tests

**Status hotkey + reconcile.** Highlight a row with `↑/↓` and press
`a`. The status pill on that row flips to `applied` immediately
(optimistic), then re-queries Postgres. Confirm the row actually
moved in the canonical store:

```bash
# on the Ubuntu host
podman-compose logs --tail=10 store      # status: <job-id> → applied
```

```sql
SELECT job_id, status FROM jobs WHERE job_id = '<job-id>';
```

Same shape for `i` (interview), `o` (offer), `r` (rejected),
`w` (withdrawn), and shift-`S` (back to saved).

**Snooze.** Press `s` on the highlighted row. No status change;
a fresh `followup_saved` reminder lands 24h out. Verify:

```sql
SELECT id, job_id, kind, due_at FROM reminders
 ORDER BY id DESC LIMIT 3;
```

**New job (`n`).** A modal prompts for URL, then title, then
company. On enter at the company step, the TUI publishes
`job.submitted` and reloads. Verify the same way as a CLI `add`:

```bash
podman-compose logs --tail=10 store      # submitted: <title> @ <company> (saved)
```

**Search (`/`).** Type a substring and hit enter — the list
filters client-side over the current snapshot. `esc` (or `/`
followed by an empty enter) clears it.

**Status filter (`f`).** Cycles the pill through `any → saved →
applied → interview → offer → rejected → withdrawn → any`. Each
step re-queries with the server-side filter on.

**Reload (`shift-R`)** forces a fresh List in case something
changed via the bot or CLI mid-session.

**Quit (`q` / `ctrl+c`).** The Bubble Tea program exits, defers
run, and the Publisher flushes its in-flight produces. Confirm
no `producer close:` errors on stderr.

### 10e. Cross-frontend coherence

While the TUI is open, add a job from the Telegram bot:

```
/add https://example.com/job/tui-cross
```

The TUI won't update automatically (live Kafka updates are v2 —
ADR 0004 Out of scope). Press `shift-R` to reload — the row
appears. Same the other way: mark a job applied in the TUI, then
`/list` in the bot — the new status is there.

### 10f. Troubleshooting

**Connection timed out on launch.** Hostname or listener is wrong.
Re-run the `kcat -L` check in §10b. If `kcat` works but the TUI
doesn't, double-check `JOB_TRACKER_KAFKA_BOOTSTRAP` actually exported
into the shell where you launched (`echo $JOB_TRACKER_KAFKA_BOOTSTRAP`).

**`postgres: dial tcp …: connect: operation timed out`.** Either
Tailscale is down on the Mac, or the home server is. `tailscale
status` should show the home server as `active; direct`.

**Status flip on keypress reverts a moment later.** The Store
consumer rejected the transition (e.g. `offer → saved` isn't a
sensible move, or the consumer logged a constraint failure). Watch
`podman-compose logs store` while pressing the key; the rejected
event will be in the log. The TUI's "reconcile after publish" is
exactly the mechanism that surfaces this.

**Terminal layout looks cramped.** Resize the window — the table
columns rebalance on `WindowSizeMsg`. The TUI is designed for
≥ 100 columns; below that, truncation gets aggressive.

---

## 11. Day-to-day

After the first-time setup above:

```bash
cd ~/job-tracker
git pull
podman-compose build           # only if code changed
podman-compose up -d           # brings up everything (infra + services)
# then use the CLI as needed:
podman-compose run --rm --no-deps cli add --url ... --title ... --company ...
# or just talk to the bot in Telegram.
```

`restart: unless-stopped` on the app services means they survive
reboots once the podman socket is enabled (`systemctl --user enable
--now podman.socket`).
