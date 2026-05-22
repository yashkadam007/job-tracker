-- Single source of truth for the Postgres schema. Every service applies
-- this on startup; all statements are idempotent (IF NOT EXISTS).
--
-- Greenfield: no production data exists. Re-running on a clean database
-- reproduces the full schema. CHECK constraints are used instead of
-- CREATE TYPE so `schema.sql` stays editable in place.

CREATE TABLE IF NOT EXISTS jobs (
    -- Producer-generated UUID. Stable across the job's lifetime; every
    -- child table and every event keys off this. URL is metadata on
    -- the job, not its identity — postings rename, redirect, and rot,
    -- but the job_id never changes.
    job_id               text        PRIMARY KEY,
    -- url is still required and globally unique, but it's mutable
    -- (rename without losing children, notes, or history).
    url                  text        NOT NULL UNIQUE,
    title                text        NOT NULL,
    company              text        NOT NULL,
    status               text        NOT NULL
        CHECK (status IN ('saved','applied','interview','rejected','offer','withdrawn')),
    first_seen_at        timestamptz NOT NULL,
    last_event_at        timestamptz NOT NULL,

    -- Posting metadata.
    work_mode            text
        CHECK (work_mode IS NULL OR work_mode IN ('onsite','hybrid','remote')),
    location             text,
    seniority            text
        CHECK (seniority IS NULL OR seniority IN ('intern','junior','mid','senior','staff','principal')),
    source               text
        CHECK (source IS NULL OR source IN ('linkedin','indeed','referral','company_site','recruiter','other')),
    tech_tags            text[]      NOT NULL DEFAULT '{}',
    description          text,
    deadline             timestamptz,

    -- Compensation. Flattened (not nested) so event field names == column
    -- names. comp_equity/comp_bonus stay text — real postings express
    -- them as "0.1%", "5000 RSU", "$80k value" — picking one numeric
    -- normalisation would force the CLI to lie.
    comp_min             numeric,
    comp_max             numeric,
    comp_currency        text,
    comp_equity          text,
    comp_bonus           text,

    -- Application details.
    resume_version       text,
    cover_letter_version text,
    referral             text,
    recruiter_name       text,
    recruiter_email      text,
    recruiter_phone      text,

    -- Personal scaffolding.
    priority             int
        CHECK (priority IS NULL OR priority BETWEEN 1 AND 5),
    custom_tags          text[]      NOT NULL DEFAULT '{}'
);

-- Per-company filter on the dashboard ("show me everything at Acme").
CREATE INDEX IF NOT EXISTS jobs_company_idx ON jobs (company);

-- "Applied remote jobs" / "interview onsite jobs" — composite hit.
CREATE INDEX IF NOT EXISTS jobs_status_work_mode_idx ON jobs (status, work_mode);

-- "Saved jobs sorted by when they showed up" — drives the inbox view
-- and the weekly throughput report.
CREATE INDEX IF NOT EXISTS jobs_status_first_seen_idx ON jobs (status, first_seen_at);

-- "Jobs with tag X" — only query against the tag arrays.
CREATE INDEX IF NOT EXISTS jobs_tech_tags_gin   ON jobs USING GIN (tech_tags);
CREATE INDEX IF NOT EXISTS jobs_custom_tags_gin ON jobs USING GIN (custom_tags);

-- Every status transition with the event that caused it. jobs.status is
-- the denormalised cache of MAX(changed_at) on this table; this table
-- is the source of truth for time-based analytics (conversion ratios,
-- weekly counts, median time-in-status).
--
-- event_id UNIQUE is the row-level idempotency key: replays from Kafka
-- insert nothing on conflict. This is deliberately separate from
-- processed_events (which dedupes at the consumer level) — the unique
-- constraint dedupes inside a single insert without an extra round trip.
CREATE TABLE IF NOT EXISTS job_status_history (
    id          bigserial   PRIMARY KEY,
    job_id      text        NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    status      text        NOT NULL,
    changed_at  timestamptz NOT NULL,
    event_id    text        NOT NULL UNIQUE
);

-- "Timeline for this job".
CREATE INDEX IF NOT EXISTS job_status_history_job_id_idx
    ON job_status_history (job_id, status, changed_at);

-- "Conversion ratio applied → interview in last 30d" / "median time in
-- status" — scanned by status across the global window.
CREATE INDEX IF NOT EXISTS job_status_history_status_changed_idx
    ON job_status_history (status, changed_at);

-- One row per interview round. PK is producer-generated interview_id so
-- a later "complete" event upserts the same row as the earlier
-- "schedule" event. interviewers is text[] — display only, no analytics.
CREATE TABLE IF NOT EXISTS job_interviews (
    interview_id  text        PRIMARY KEY,
    job_id        text        NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    round         text        NOT NULL
        CHECK (round IN ('phone_screen','technical','behavioral','system_design','onsite','final','other')),
    scheduled_at  timestamptz,
    completed_at  timestamptz,
    outcome       text
        CHECK (outcome IS NULL OR outcome IN ('passed','failed','no_show','pending','withdrawn')),
    interviewers  text[]      NOT NULL DEFAULT '{}',
    notes         text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- "Upcoming interviews" — calendar view; only pending rows matter.
CREATE INDEX IF NOT EXISTS job_interviews_scheduled_idx
    ON job_interviews (scheduled_at)
    WHERE completed_at IS NULL;

-- Append-only note timeline per job. event_id UNIQUE → idempotent replays.
CREATE TABLE IF NOT EXISTS job_notes (
    id          bigserial   PRIMARY KEY,
    job_id      text        NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    body        text        NOT NULL,
    created_at  timestamptz NOT NULL,
    event_id    text        NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS job_notes_job_id_created_idx
    ON job_notes (job_id, created_at);

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
    job_id     text        NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
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
