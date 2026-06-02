-- Revert to the original six-value allowed set. The constraint itself
-- enforces "no row uses assessment/declined" — the ALTER aborts on
-- the first offending row, so the operator must hand-resolve before
-- migrating down.

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;
ALTER TABLE jobs ADD  CONSTRAINT jobs_status_check
    CHECK (status IN ('saved','applied','interview','rejected','offer','withdrawn'));

ALTER TABLE job_status_history DROP CONSTRAINT IF EXISTS job_status_history_status_check;
-- Init didn't have a CHECK on job_status_history.status; the down path
-- intentionally leaves the column unconstrained to match.
