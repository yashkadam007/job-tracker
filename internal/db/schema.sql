-- Single source of truth for the Postgres schema. Every service applies
-- this on startup; all statements are idempotent (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS jobs (
    url            text        PRIMARY KEY,
    title          text        NOT NULL,
    company        text        NOT NULL,
    status         text        NOT NULL,
    first_seen_at  timestamptz NOT NULL,
    last_event_at  timestamptz NOT NULL
);

-- Idempotency ledger shared by all consumers. The `consumer` column
-- namespaces by consumer-group so each service has its own view of
-- "events I have already applied" — Store and Scheduler can both
-- consume job.submitted without stepping on each other's ledger.
CREATE TABLE IF NOT EXISTS processed_events (
    consumer     text        NOT NULL,
    event_id     text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- Scheduled future reminders. fired_at IS NULL means "not yet
-- published to job.reminder". cancelled = true means "superseded
-- by a status change before the reminder fired".
CREATE TABLE IF NOT EXISTS reminders (
    id         bigserial   PRIMARY KEY,
    url        text        NOT NULL,
    kind       text        NOT NULL,
    due_at     timestamptz NOT NULL,
    fired_at   timestamptz,
    cancelled  boolean     NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Partial index: only pending reminders matter for the scheduler's
-- "what's due" query, and those are a tiny fraction of the table.
CREATE INDEX IF NOT EXISTS reminders_pending_due_idx
    ON reminders (due_at)
    WHERE fired_at IS NULL AND NOT cancelled;
