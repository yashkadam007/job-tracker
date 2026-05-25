-- ADR 0010 — promote company to a first-class entity. One new table,
-- one column swap on jobs, one index swap. The backfill runs inline so
-- the operator's existing jobs.company strings flow into companies rows
-- before the NOT NULL FK on jobs.company_id becomes enforceable.

CREATE TABLE companies (
    company_id   text        PRIMARY KEY,
    name         text        NOT NULL,
    slug         text        NOT NULL UNIQUE,
    tags         text[]      NOT NULL DEFAULT '{}',
    notes        text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX companies_tags_gin ON companies USING GIN (tags);

ALTER TABLE jobs ADD COLUMN company_id text;

-- Backfill: one companies row per slug. MIN(company) picks the display
-- name deterministically so re-running the migration on the same data
-- yields the same casing.
INSERT INTO companies (company_id, name, slug)
SELECT gen_random_uuid()::text,
       MIN(company),
       lower(regexp_replace(trim(company), '\s+', ' ', 'g'))
  FROM jobs
 GROUP BY lower(regexp_replace(trim(company), '\s+', ' ', 'g'));

UPDATE jobs j
   SET company_id = c.company_id
  FROM companies c
 WHERE c.slug = lower(regexp_replace(trim(j.company), '\s+', ' ', 'g'));

-- Defense in depth: every row must have resolved. If not, the migration
-- aborts here and the operator inspects jobs WHERE company_id IS NULL.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM jobs WHERE company_id IS NULL) THEN
        RAISE EXCEPTION 'companies backfill left orphan jobs; inspect jobs WHERE company_id IS NULL';
    END IF;
END
$$;

ALTER TABLE jobs ALTER COLUMN company_id SET NOT NULL;
ALTER TABLE jobs ADD CONSTRAINT jobs_company_id_fkey
    FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE RESTRICT;

DROP INDEX IF EXISTS jobs_company_idx;
ALTER TABLE jobs DROP COLUMN company;
CREATE INDEX jobs_company_id_idx ON jobs (company_id);
