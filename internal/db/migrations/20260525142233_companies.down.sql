-- Reverse ADR 0010. Restore jobs.company text from the companies join,
-- drop the FK column, recreate the original index, drop companies.
-- Near-duplicate names that collapsed into a single slug lose their
-- original casing variant on the way down — see ADR 0010 Notes.

ALTER TABLE jobs ADD COLUMN company text;

UPDATE jobs j
   SET company = c.name
  FROM companies c
 WHERE c.company_id = j.company_id;

ALTER TABLE jobs ALTER COLUMN company SET NOT NULL;

DROP INDEX IF EXISTS jobs_company_id_idx;
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_company_id_fkey;
ALTER TABLE jobs DROP COLUMN company_id;

CREATE INDEX IF NOT EXISTS jobs_company_idx ON jobs (company);

DROP INDEX IF EXISTS companies_tags_gin;
DROP TABLE IF EXISTS companies;
