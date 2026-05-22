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

The build step compiles all four binaries (`cli`, `store`, `scheduler`,
`notifier`) into a single image and takes ~1–2 minutes on first run.
Subsequent builds are cached.

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
podman-compose run --rm cli ensure-topics
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

Start all three as detached containers:

```bash
podman-compose up -d store scheduler notifier
```

Watch the logs (one terminal per service, or combined):

```bash
podman-compose logs -f store
podman-compose logs -f scheduler
podman-compose logs -f notifier

# or all at once:
podman-compose logs -f store scheduler notifier
```

Expected startup lines:

- **store:** `store: consuming job.submitted, job.status.changed, job.note.added, job.interview.recorded (group=store)`
- **scheduler:** `scheduler: group=scheduler, saved=168h0m0s, applied=336h0m0s, poll=30s`
- **notifier:** `notifier: consuming job.reminder (group=notifier)`

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
podman-compose run --rm cli add \
  --url https://example.com/job/1 \
  --title "Senior Backend Engineer" \
  --company "Acme"
```

Expected reactions (watch with `podman-compose logs -f …`):

- **CLI:** `published: Senior Backend Engineer @ Acme (saved)`
- **store:** `submitted: Senior Backend Engineer @ Acme (saved)`
- **scheduler:** `scheduled reminder for https://example.com/job/1 (saved)`
- **notifier:** (~10s later, with smoke-test env)
  `REMINDER followup_saved — Senior Backend Engineer @ Acme (saved, due ...) :: Still interested? ...`

The bare three-flag form above still works. To exercise the richer
metadata path, pass any of the optional flags:

```bash
podman-compose run --rm cli add \
  --url https://example.com/job/2 \
  --title "Staff Engineer" --company "Globex" \
  --work-mode remote --seniority staff --source linkedin \
  --tech-tag go --tech-tag postgres --custom-tag dream_company \
  --comp-min 180000 --comp-max 230000 --comp-currency USD \
  --priority 5
```

Inspect the row to confirm the typed columns + arrays landed:

```sql
SELECT url, work_mode, seniority, tech_tags, custom_tags, priority,
       comp_min, comp_max, comp_currency
  FROM jobs WHERE url = 'https://example.com/job/2';
```

### 5b. Status change

```bash
podman-compose run --rm cli status https://example.com/job/1 applied
```

Expected:

- **store:** `status: https://example.com/job/1 → applied`
- **scheduler:** `reminders updated for https://example.com/job/1 (applied)`
- The old "saved" reminder is cancelled; a new "applied" reminder is scheduled.
- **notifier:** within ~15s, fires the `followup_applied` reminder.

### 5c. Terminal status (no new reminder)

```bash
podman-compose run --rm cli status https://example.com/job/1 rejected
```

Expected:

- store updates the row.
- scheduler cancels pending reminders, schedules nothing new.
- notifier stays quiet.

### 5d. Note + interview pipeline

```bash
podman-compose run --rm cli note add \
  --url https://example.com/job/1 \
  --body "Recruiter said they'll decide by Friday."
```

Expected store log: `note: added for https://example.com/job/1`.
Verify the row:

```sql
SELECT url, body, created_at FROM job_notes
 ORDER BY created_at DESC LIMIT 5;
```

Then schedule and update an interview. **Save the printed `interview_id`** —
`interview update` needs it (the CLI doesn't persist any local state).

```bash
podman-compose run --rm cli interview schedule \
  --url https://example.com/job/1 \
  --round phone_screen \
  --scheduled-at 2026-06-01T15:00:00Z \
  --interviewer "Alex" --interviewer "Sam" \
  --notes "30 min screening"

# copy interview_id from the output, then:
podman-compose run --rm cli interview update \
  --interview-id <id-from-above> \
  --url https://example.com/job/1 \
  --completed-at 2026-06-01T15:35:00Z \
  --outcome passed
```

Verify the upsert merged the two events (round + interviewers from
schedule, completed_at + outcome from update — neither got wiped):

```sql
SELECT interview_id, round, scheduled_at, completed_at, outcome, interviewers
  FROM job_interviews ORDER BY updated_at DESC LIMIT 5;
```

Status transitions also leave a trail now — every `cli status …` writes
to `job_status_history`:

```sql
SELECT url, status, changed_at FROM job_status_history
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
SELECT id, url, kind, due_at FROM reminders
 WHERE fired_at IS NULL AND NOT cancelled
 ORDER BY due_at;

-- transitions over time (source of truth for time-based analytics)
SELECT url, status, changed_at FROM job_status_history
 ORDER BY changed_at DESC LIMIT 20;

-- pipeline state per job
SELECT url, round, scheduled_at, completed_at, outcome
  FROM job_interviews ORDER BY scheduled_at DESC NULLS LAST LIMIT 20;

-- notes timeline for one job
SELECT created_at, body FROM job_notes
 WHERE url = 'https://example.com/job/1'
 ORDER BY created_at;
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
   Its log should show `fired reminder id=... url=...` when it fires.

### "topic already exists" error on `ensure-topics`

That's expected behaviour, printed as `exists: <topic>`. Not an error.

### `reminders` won't create on an old DB volume

The schema now declares `reminders.url REFERENCES jobs(url)`. `CREATE
TABLE IF NOT EXISTS` is a no-op against a pre-existing `reminders`
table that lacks the FK, so the new constraint silently doesn't land.
This is greenfield (per ADR 0001) — wipe and recreate:
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

## 9. Day-to-day

After the first-time setup above:

```bash
cd ~/job-tracker
git pull
podman-compose build           # only if code changed
podman-compose up -d           # brings up everything (infra + services)
# then use the CLI as needed:
podman-compose run --rm cli add --url ... --title ... --company ...
```

`restart: unless-stopped` on the app services means they survive
reboots once the podman socket is enabled (`systemctl --user enable
--now podman.socket`).
