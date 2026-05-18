# Runbook — Testing job-tracker on the Ubuntu server

A copy-pasteable walkthrough for bringing the stack up on a fresh
machine, running the full pipeline, and inspecting state. Aimed at
first-time setup; subsequent runs collapse to "Step 2 + Step 4".

---

## 0. Prerequisites

Check on the Ubuntu host once:

```bash
go version                  # need 1.25 or newer
podman --version            # any recent version
podman-compose --version    # or: docker-compose if you symlink
psql --version              # only needed if you want to query Postgres directly
```

If `go` is missing or old:

```bash
sudo snap install go --classic
# OR: download from https://go.dev/dl/ and untar to /usr/local
```

If `podman-compose` is missing:

```bash
sudo apt install podman-compose          # ubuntu 23.10+
# OR:
pipx install podman-compose
```

Rootless podman is fine; nothing here needs root.

---

## 1. Clone & prepare

```bash
git clone <your-github-url> ~/job-tracker
cd ~/job-tracker
cp .env.example .env       # only needed if you override defaults
```

Build everything once to populate the module cache and catch issues
before any service tries to start:

```bash
go build ./...
```

You should get no output. Binaries are not produced — that's fine,
we use `go run` while iterating.

---

## 2. Start the infra (Kafka + Postgres + Kafka UI)

```bash
podman-compose up -d
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
http://localhost:8080
```

You should see one cluster ("job-tracker"), no topics yet.

---

## 3. Create topics

```bash
go run ./cmd/cli ensure-topics
```

Expected output:

```
created: job.reminder
created: job.status.changed
created: job.submitted
```

Re-running prints `exists: …` for each — the command is idempotent.

In Kafka UI → Topics, you should now see all three.

---

## 4. Run the services

Each in its own terminal (or use `tmux`). Run from `~/job-tracker`.

**Terminal A — Store:**
```bash
go run ./cmd/store
# expect: store: consuming job.submitted, job.status.changed (group=store)
```

**Terminal B — Scheduler:**

For real-world delays:
```bash
go run ./cmd/scheduler
```

For a 10-second smoke test:
```bash
REMINDER_SAVED_SECONDS=10 REMINDER_APPLIED_SECONDS=15 REMINDER_POLL_SECONDS=2 \
  go run ./cmd/scheduler
```

Expect a startup line like:
```
scheduler: group=scheduler, saved=10s, applied=15s, poll=2s
```

**Terminal C — Notifier:**
```bash
go run ./cmd/notifier
# expect: notifier: consuming job.reminder (group=notifier)
```

---

## 5. Smoke tests

Use a 4th terminal for these.

### 5a. Happy path

```bash
go run ./cmd/cli add \
  --url https://example.com/job/1 \
  --title "Senior Backend Engineer" \
  --company "Acme"
```

Expected reactions:

- **CLI:** `published: Senior Backend Engineer @ Acme (saved)`
- **Store (Terminal A):** `submitted: Senior Backend Engineer @ Acme (saved)`
- **Scheduler (Terminal B):** `scheduled reminder for https://example.com/job/1 (saved)`
- **Notifier (Terminal C):** (~10s later, with the smoke-test env)
  `REMINDER followup_saved — Senior Backend Engineer @ Acme (saved, due ...) :: Still interested? ...`

### 5b. Status change

```bash
go run ./cmd/cli status https://example.com/job/1 applied
```

Expected:

- **Store:** `status: https://example.com/job/1 → applied`
- **Scheduler:** `reminders updated for https://example.com/job/1 (applied)`
- The old "saved" reminder is cancelled; a new "applied" reminder is scheduled.
- **Notifier:** within ~15s, fires the `followup_applied` reminder.

### 5c. Terminal status (no new reminder)

```bash
go run ./cmd/cli status https://example.com/job/1 rejected
```

Expected:

- Store updates the row.
- Scheduler cancels pending reminders, schedules nothing new.
- Notifier stays quiet.

### 5d. Idempotency check

Stop the Store (Ctrl-C in Terminal A) and restart it:

```bash
go run ./cmd/store
```

You'll see it consume the old events again. Each one prints
`submitted: dup event_id=... skipped` / `status: dup event_id=... skipped`.
**No duplicate rows or status flips in Postgres** — that's the
`processed_events` ledger doing its job.

---

## 6. Inspecting state

### Postgres

```bash
psql postgres://jobtracker:jobtracker@localhost:5432/jobtracker
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
```

### Kafka UI (http://localhost:8080)

- **Topics → job.submitted → Messages**: the JSON events you produced.
- **Consumer Groups**: should show `store`, `scheduler`, `notifier` each with a current offset and a lag of 0 (or near 0) when caught up.
- **Topics → job.reminder → Messages**: the reminder events the Scheduler published.

The two consumer groups (`store` + `scheduler`) reading the same
two topics is the "fan-out" pattern in action — proof that consumer
groups are independent.

---

## 7. Stopping

Stop the Go services with Ctrl-C in each terminal. They handle SIGINT.

Stop the containers:

```bash
podman-compose down            # keeps data volumes
podman-compose down -v         # ALSO deletes Kafka logs + Postgres data
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

### Go services hang on connect

`localhost:9092` is what they expect by default. If the services run
*on the same machine* as the containers, that works because the
compose file publishes port 9092 to the host.

If you run a service inside a container later (future scope), it must
use `kafka:29092` instead — set `KAFKA_BOOTSTRAP=kafka:29092` in that
container's env.

### "missing go.sum entry"

You forgot to commit `go.sum` after adding a dependency on the dev
machine. Run `go mod tidy` and commit + push.

### Notifier never fires

Three things to check, in order:
1. Is the reminder row actually in the DB?
   `SELECT * FROM reminders WHERE fired_at IS NULL;`
2. Is its `due_at` in the past?
   If not, you're just waiting. Drop `REMINDER_SAVED_SECONDS` for testing.
3. Is the Scheduler ticking?
   Its log should show `fired reminder id=... url=...` when it fires.

### "topic already exists" error on `ensure-topics`

That's expected behaviour, printed as `exists: <topic>`. Not an error.

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
podman-compose up -d        # if not already running
go run ./cmd/store &
go run ./cmd/scheduler &
go run ./cmd/notifier &
# then use the CLI as needed
```

Use `tmux` or systemd units to keep the three services running across
shell sessions; both are out of scope for v1 but trivial to add when
you find yourself wanting it.
