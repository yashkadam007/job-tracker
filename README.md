# Job Tracker

Kafka-based pipeline that tracks job applications. Built primarily to learn
Kafka end-to-end while producing something useful.

See [`../../thinking/job-tracker-plan.md`](../../thinking/job-tracker-plan.md)
for the full design (v1 scope + future scope).

## Stack

- **Kafka 3.8** in KRaft mode (no ZooKeeper) — message bus
- **Postgres 16** — job + reminder storage
- **Kafka UI** — web UI for inspecting topics, messages, consumer groups
- **Go services** (CLI, Store, Reminder Scheduler, Notifier) — coming soon

## Layout

```
job-tracker/
├── compose.yml         # Kafka + Postgres + Kafka UI
├── .env.example        # config template
└── (services to come)
```

## Running the infra (Podman, on Ubuntu host)

```bash
podman-compose up -d
podman-compose ps                  # check all healthy
podman-compose logs -f kafka       # tail kafka logs
```

Then open:

- **Kafka UI:** http://localhost:8080 — inspect topics + messages here while learning.
- **Postgres:** `psql postgres://jobtracker:jobtracker@localhost:5432/jobtracker`

To stop: `podman-compose down`. Add `-v` to also delete data volumes.

## How the containers talk to each other (Kafka beginner notes)

Kafka has a quirk: it doesn't just accept connections, it also *tells*
clients which address to reconnect on after the first handshake. That's
called the **advertised listener**. If we got this wrong, the host CLI
would connect once, then be told "actually, reconnect to `kafka:29092`"
— which is unreachable from outside Podman — and everything would hang.

So Kafka here listens on two ports:

| Listener | Address it advertises | Used by                                     |
|----------|------------------------|---------------------------------------------|
| INTERNAL | `kafka:29092`          | Other containers (Kafka UI, future services)|
| EXTERNAL | `localhost:9092`       | Anything on the host (CLI during dev)       |

When a Go service runs **inside** a container in this compose, it should
connect to `kafka:29092`. When run **on the host**, it should connect to
`localhost:9092`. The `.env.example` covers the host case; container
services will get their own env vars in their compose entries.

## Kafka concepts you'll see

- **Topic** — a named, append-only log. e.g. `job.submitted`.
- **Producer** — writes messages to a topic. (Our CLI.)
- **Consumer** — reads messages from a topic. (Store, Notifier.)
- **Consumer group** — a set of consumers that split the work of one
  topic among themselves. Two *different* groups reading the same topic
  each get the full stream — that's how Store and the Reminder Scheduler
  will independently consume the same events later on.
- **Offset** — a consumer's bookmark into a topic. Kafka stores it for
  you, so on restart the consumer resumes where it left off.
- **KRaft** — the new way Kafka coordinates itself, replacing ZooKeeper.
  No separate process needed.
