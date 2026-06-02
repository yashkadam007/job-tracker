-- ADR 0013 — widen status enum to admit `assessment` and `declined`.
-- assessment fills the gap between applied and interview (take-home
-- outstanding); declined splits the candidate's "I turned down the
-- offer" from the company's rejection. Transition legality is enforced
-- in the Store consumer, not by the CHECK — this constraint is the
-- floor (membership), not the ceiling (graph).

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;
ALTER TABLE jobs ADD  CONSTRAINT jobs_status_check
    CHECK (status IN ('saved','applied','assessment',
                      'interview','offer','rejected',
                      'declined','withdrawn'));

-- job_status_history.status had no CHECK in the init migration. Add
-- the same allow-list here so the history table can't be written with
-- a status the jobs table would reject — the analytics query layer
-- assumes the two are in lockstep.
ALTER TABLE job_status_history DROP CONSTRAINT IF EXISTS job_status_history_status_check;
ALTER TABLE job_status_history ADD  CONSTRAINT job_status_history_status_check
    CHECK (status IN ('saved','applied','assessment',
                      'interview','offer','rejected',
                      'declined','withdrawn'));
