-- Reverse the init migration. Drop in reverse dependency order so
-- foreign keys don't block the drops.

DROP TABLE IF EXISTS reminders;
DROP TABLE IF EXISTS processed_events;
DROP TABLE IF EXISTS job_notes;
DROP TABLE IF EXISTS job_interviews;
DROP TABLE IF EXISTS job_status_history;
DROP TABLE IF EXISTS jobs;
