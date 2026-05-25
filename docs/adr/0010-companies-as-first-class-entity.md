# ADR 0010 — Companies as a first-class entity

## Issue

`jobs.company` is a free-text column. That worked while the tracker
held a handful of rows, but three concrete needs now push past it:

1. **Multiple roles at one company.** A real search applies to several
   roles at the same employer over weeks or months. With a string
   column, `"Acme"`, `"Acme Corp"`, and `"acme"` are three different
   employers as far as Postgres is concerned. The dashboard's
   per-company filter (`jobs_company_idx`) bins them into separate
   buckets; `GROUP BY company` lies; and the operator can't ask
   "how many applications at Acme are still pending?" without
   manually reconciling spellings after the fact.
2. **Company-level metadata.** The operator wants to tag companies
   (`dream`, `interested`, `backup`, … — whatever the operator
   reaches for at the moment), keep free-text notes against the
   company itself (not a specific posting), and treat that metadata
   as durable across roles. A string column has nowhere to hang
   any of this.
3. **Typo and case drift on entry.** The TUI new-job modal accepts
   any string. The first time a company is added, a typo silently
   creates a permanent variant; every subsequent role at that
   employer then has to either repeat the typo or fork into a new
   bin.

Autocomplete against `SELECT DISTINCT company FROM jobs` patches (3)
for the second-and-later role at a company, but it doesn't fix (1)
or (2) — both want an entity, not just a deduplicated string.

The tracker is in active use and the local Postgres holds real
data. Schema changes apply via `golang-migrate` (ADR 0009); this
migration must backfill the existing `jobs.company` strings rather
than assume a clean slate. The project is still single-operator
single-tenant (ADR 0001).

## Decision

Promote company to a first-class entity. Three coordinated changes:

**1. New `companies` table.**

```
companies (
    company_id   text        PRIMARY KEY,    -- producer-generated UUID
    name         text        NOT NULL,       -- display form ("Acme Corp")
    slug         text        NOT NULL UNIQUE,-- normalized key ("acme corp")
    tags         text[]      NOT NULL DEFAULT '{}',
    notes        text,
    created_at   timestamptz NOT NULL DEFAULT now()
)
```

`slug` is `lower(trim(regexp_replace(name, '\s+', ' ', 'g')))` —
case-insensitive, whitespace-collapsed. It is the dedup key; `name`
preserves the operator's chosen casing for display.

`tags` is `text[]` + GIN, matching the `jobs.tech_tags` /
`jobs.custom_tags` pattern from ADR 0001: tags carry no metadata,
the only query is "companies with tag X", and the vocabulary is
operator-defined and open-ended (no `CHECK` constraint, no enum
allow-list). The "I really want to work at X" use case is one tag
value (`dream`, say) the operator picks; "backup", "interesting",
"avoid" are equally first-class — the schema does not pick a
vocabulary.

**2. `jobs.company_id` replaces `jobs.company`.**

```
jobs.company_id  text NOT NULL REFERENCES companies(company_id) ON DELETE RESTRICT
```

`ON DELETE RESTRICT` because deleting a company with applications
against it should fail loudly. Drop the existing `jobs.company` text
column and `jobs_company_idx`; replace with `jobs_company_id_idx`.

**3. Events still carry company *name*, not `company_id`.**

`JobSubmitted.Company` stays a string. The Store consumer resolves
it on apply:

```sql
INSERT INTO companies (company_id, name, slug)
VALUES (gen_random_uuid()::text, $1, normalize_slug($1))
ON CONFLICT (slug) DO UPDATE SET name = companies.name  -- no-op, returns existing
RETURNING company_id
```

The consumer then uses the resolved `company_id` when inserting the
job row. Replays are idempotent: same name → same slug → same row.

This mirrors ADR 0001's reasoning for `job_id`: producers must not
do a write-path Postgres read just to learn an id. The producer
knows the user's typed string; the consumer knows the canonical id;
neither side has to coordinate ahead of the event.

**4. Inline create-or-link in the new-job modal.**

The TUI `stepCompany` step gets autocomplete suggestions backed by
`Reader.ListCompanies(ctx)`:

- Typing matches existing companies (case-insensitive substring,
  so `"corp"` surfaces `"Acme Corp"`). The operator picks one with
  Tab/Enter → resolves to its canonical `name`, which goes into
  the event verbatim.
- Typing a brand-new name and pressing Enter → that string flows
  into the event; the Store consumer creates the `companies` row on
  apply.

No second modal, no two-phase "register then submit" flow. Adding a
job stays a three-field modal (URL → title → company), which is the
explicit UX target — fast capture wins over upfront structure.

**5. Tags and notes live outside the new-job flow.**

A separate `c` keybind on the list view opens a company panel:
browse companies, edit tags, edit notes. This is its own
ADR-scoped piece of work and is out of scope for the *schema* part
of this decision — but the columns exist in v1 so the panel can
land without a follow-up migration.

## Status

Accepted.

## Group

Foundational / Schema and contracts.

## Assumptions

- The tracker holds live operator data. The up-migration must
  backfill `jobs.company` strings into `companies` rows before the
  `NOT NULL` FK lands, and the down-migration must restore the
  string column from the resolved id. No data loss on either
  direction.
- The existing `jobs.company` strings are whatever the operator has
  typed so far. They may already contain near-duplicates (case
  variants, "Inc" vs "Inc.") that the slug normalization will or
  won't collapse depending on how aggressive it is — see the slug
  decision below and the open question in Notes.
- Single operator, single producer. No concurrent submit-races to
  reason about across writers; the consumer's `ON CONFLICT (slug)`
  is enough.
- Case-and-whitespace normalization is sufficient for v1 dedup.
  Aliases ("Meta" ↔ "Facebook"), legal-entity vs. brand splits, and
  acquired-company merges are real concerns but uncommon enough on a
  personal tracker that they can wait for a `companies merge` op.
- Company tagging is open-vocabulary and many-to-many. Locking it
  into a `CHECK`-constrained enum now would force a migration every
  time the operator coins a new label (`backup`, `pre-seed`,
  `eu-only`, …). `text[]` + GIN matches the existing tag-shape
  precedent and defers the vocabulary question entirely.
- The new-job modal staying three steps is a non-negotiable UX
  requirement (operator preference, this thread). Anything that
  forces a fourth step is rejected.

## Constraints

- Producers do not read Postgres on the write path. Resolving names
  to ids happens in the consumer (same rule as `job_id`).
- Schema change goes through `golang-migrate` (ADR 0009). No
  `CREATE IF NOT EXISTS` patching. The migration is one file —
  create `companies`, alter `jobs`, drop the old column and index.
- Event field names match Postgres column names — except where a
  field is a resolved foreign key (here, `company_id`). The event
  carries the unresolved upstream value (`company` name), the column
  carries the resolved id. This breaks the literal symmetry of
  ADR 0001 for one field; the principle behind that rule
  ("events describe what happened") is preserved.
- `slug` is generated, never user-typed. The operator never sees it;
  it exists only to make `ON CONFLICT` work.
- `ON DELETE RESTRICT` on `jobs.company_id` — never silently cascade
  a company deletion through applications. A future "merge" op
  re-points jobs explicitly before dropping the obsolete row.
- The new-job modal stays at three steps. Tag-editing is a separate
  flow.

## Positions

Alternatives considered:

1. **Companies table + name-in-events + consumer resolves** (this
   decision).
2. **Keep `jobs.company` as a string; add autocomplete only.**
   Rejected — autocomplete is UX scaffolding, not an entity. It
   solves typos for second-and-later submissions but cannot hang
   `tier` or company-level notes anywhere. The operator's stated
   needs (3, in particular) require an entity.
3. **Companies table; events carry `company_id`; producer resolves
   via a Reader call before publishing.** Rejected — violates the
   ADR 0001 principle that producers don't do a write-path Postgres
   read. The TUI could do it (it already holds a `Reader`); the CLI
   would either have to acquire one or duplicate the logic. The
   asymmetry isn't worth the marginal "events look more normalised"
   win.
4. **Events carry both name and a producer-generated `company_id`
   for new companies; consumer trusts the id for new rows and
   resolves the name to an existing id otherwise.** Rejected —
   doubles the surface area for "what does a new vs. existing
   company look like on the wire" with no payoff. The consumer
   resolve is already a single statement.
5. **Soft-delete (`deleted_at`) on companies instead of
   `ON DELETE RESTRICT`.** Rejected for v1 — single operator, no
   audit/compliance requirement, no UI to surface tombstones.
   `RESTRICT` is the correct safety net; revisit if a "company
   archive" workflow shows up.
6. **`tier text CHECK (tier IN (…))` scalar column for
   categorization.** Rejected — categorization is many-to-many
   (a company can be both `dream` and `eu-only`), the vocabulary
   is operator-defined and grows over time, and forcing a
   migration on every new label is friction the operator
   explicitly pushed back on. `text[]` + GIN is the precedent
   ADR 0001 set for exactly this shape of data.
7. **A separate "company registration" event topic (`CompanyRegistered`).**
   Rejected for v1 — adds a topic and a consumer handler for a thing
   that doesn't have an independent lifecycle yet. Companies are
   currently born from job submissions and never edited outside the
   tier-panel flow. If the tier panel grows write traffic, a
   `CompanyEdited` (or richer `CompanyRecorded`) event becomes
   justified; for v1 the company exists as a side-effect of the
   first job submitted against it.
8. **Two-step "register company → then submit job" modal.**
   Rejected — fails the UX requirement. The whole point of inline
   create-or-link is that adding a job stays a three-prompt flow.
9. **Keep `jobs.company` text *and* add a separate `companies` lookup
   table without an FK.** Rejected — that's the worst of both
   worlds: two sources of truth, drift on the first non-modal
   write path (a CSV import, a Telegram bot edit), and the operator
   re-creates the dedup problem they're trying to escape.

## Argument

- **The schema change is small.** One new table, one new column on
  `jobs`, one dropped column, one index swap. The Store consumer
  gains an UPSERT-then-SELECT-id step; nothing else moves.
- **The event contract barely changes.** `JobSubmitted.Company`
  already exists as a string. The only semantic shift is "this
  string is canonical-name-not-arbitrary"; the wire format is
  identical. Replays of historical events still work.
- **Consumer-side resolve preserves the producer-no-Postgres-read
  rule.** ADR 0001 paid real attention to that property for
  `job_id`; this ADR pays the same attention for `company_id`. The
  cost is a small SQL block in `internal/store`.
- **UX target is met.** Three-step modal stays three steps; the
  textinput suggestions list is a passive overlay, not a new
  modal. The operator types as freely as today; the only new
  behaviour is that picking a suggestion guarantees a clean link.
- **Metadata (`tags`, `notes`) has a home.** The "I really want
  to work at X" requirement lands as a tag on `companies`, not a
  custom tag on every individual job (which is what today's
  schema would force). Tags are open-vocabulary, so the operator
  picks labels at the moment they need them — no schema change
  to coin a new one.
- **Analytics queries become natural.** "Pending applications at
  Acme" is `WHERE company_id = $1 AND status NOT IN ('rejected',
  'withdrawn', 'offer')` — one indexed scan. "How many companies
  am I tracking?" is `SELECT count(*) FROM companies`. Today both
  require operator-side string reconciliation.

## Implications

- **Migration.** One up/down pair under
  `internal/db/migrations/`. The up-migration runs the backfill
  inside the migration itself so the operator's existing
  `jobs.company` strings flow into `companies` rows before the
  `NOT NULL` constraint on `jobs.company_id` becomes enforceable:
  1. `CREATE TABLE companies (...)` with `slug UNIQUE`, plus
     `CREATE INDEX companies_tags_gin ON companies USING GIN (tags)`.
  2. `ALTER TABLE jobs ADD COLUMN company_id text` — nullable for
     the duration of the backfill.
  3. Backfill companies, picking one display `name` per slug
     deterministically (`MIN(company)` so the result is repeatable):
     ```sql
     INSERT INTO companies (company_id, name, slug)
     SELECT gen_random_uuid()::text,
            MIN(company),
            lower(regexp_replace(trim(company), '\s+', ' ', 'g'))
       FROM jobs
      GROUP BY lower(regexp_replace(trim(company), '\s+', ' ', 'g'));
     ```
  4. Backfill `jobs.company_id`:
     ```sql
     UPDATE jobs j
        SET company_id = c.company_id
       FROM companies c
      WHERE c.slug = lower(regexp_replace(trim(j.company), '\s+', ' ', 'g'));
     ```
  5. Assert no orphans: `SELECT count(*) FROM jobs WHERE company_id
     IS NULL` must be 0; otherwise the migration aborts and the
     operator inspects the offending rows.
  6. `ALTER TABLE jobs ALTER COLUMN company_id SET NOT NULL`,
     `ADD CONSTRAINT … FOREIGN KEY … REFERENCES companies(company_id)
     ON DELETE RESTRICT`.
  7. `DROP INDEX jobs_company_idx`,
     `ALTER TABLE jobs DROP COLUMN company`,
     `CREATE INDEX jobs_company_id_idx ON jobs (company_id)`.

  Down-migration reverses by re-adding `jobs.company text`,
  populating it from the join (`UPDATE jobs SET company = c.name
  FROM companies c WHERE c.company_id = jobs.company_id`),
  dropping the FK column, recreating `jobs_company_idx`, and
  dropping the `companies` table. No data is lost in either
  direction — the operator can roll forward and back at least once
  safely (modulo the case where two near-duplicate names slug into
  one, in which case rolling back loses the casing variant; see
  Notes).
- **`internal/store`.** `ApplySubmitted` gains a resolve step:
  upsert the company by slug, then insert/update `jobs` with the
  resulting `company_id`. The handler stays in one transaction.
  No change to `ApplyStatusChanged` / `ApplyNoteAdded` /
  `ApplyInterviewRecorded` — none of them touch the company.
- **`internal/jobclient` (Reader).** New methods:
  - `ListCompanies(ctx) ([]Company, error)` — drives the TUI
    autocomplete and a future companies panel. Returns
    `{CompanyID, Name, Tags, Notes}`.
  - `Reader.List` / `Reader.Get` join `companies` into the
    existing `Job` projection so `Job.Company` (display name) is
    populated from `companies.name`. `Job.CompanyTags` is added
    so list/detail views can render badges without a second
    round-trip. The string-shaped `Job.Company` field is
    preserved to minimise TUI churn.
- **`internal/jobclient` (Publisher).** No change — the event
  payload still carries a `Company` string.
- **`internal/tui`.**
  - New-job modal `stepCompany` gets `textinput.ShowSuggestions =
    true`; suggestions are loaded once when entering `modeNew`
    (one Reader call per modal-open, cheap).
  - A future `c` keybind opens a companies panel (tag-edit,
    notes-edit, rename). Out of scope for this ADR's commits; the
    columns exist so the panel can land independently.
- **`cmd/cli`.** No flag change. The existing `--company` string
  flag continues to mean "company display name"; the store
  consumer resolves it.
- **`internal/bot`.** Same as CLI — the Telegram surface accepts
  a company string and that flows through.
- **No new Kafka topic.** Confirms the ADR 0001 budget ("at most
  two new event topics" was about the schema redesign; this one
  adds zero).
- **No new drift-detection entry.** `companies.tags` is open
  vocabulary — there is nothing to assert against `CHECK`. The
  existing `AllowedCompTags` / `AllowedTechTags` precedent (no
  drift test) carries over.

## Related decisions

- **ADR 0001** — Richer schema and event contracts. This ADR
  extends that schema; the producer-no-Postgres-read principle for
  `job_id` is reused verbatim for `company_id`.
- **ADR 0002** — Shared `jobclient` library. Gains
  `ListCompanies` and a `Company` projection.
- **ADR 0004** — Desktop TUI. The new-job modal is the surface
  this ADR is optimising for.
- **ADR 0005** — Producer-side input validation. The new
  `company` validator stays string-shaped (non-empty after trim);
  the consumer is what makes the string a real entity.
- **ADR 0009** — Schema migrations via `golang-migrate`. This
  ADR's schema change is the second real migration on top of
  `0001-init`.

## Related requirements

- Tracking multiple roles at one company without manual string
  reconciliation.
- Per-company aggregates ("how many applications at X are still
  pending?") in a single SQL statement.
- Per-company metadata (tags, notes) with a real home.
- Inline create-or-link at submit time. Three-prompt modal stays
  three prompts.
- Producers don't read from Postgres on the write path.

## Related artifacts

- `internal/db/migrations/<ts>_companies.up.sql` (new)
- `internal/db/migrations/<ts>_companies.down.sql` (new)
- `internal/store/store.go` — `ApplySubmitted` resolve step.
- `internal/jobclient/reader.go` — `ListCompanies`, join
  `companies` into `Job` projection.
- `internal/jobclient/jobclient.go` — `Job.CompanyTier` field (or
  a small `Company` struct).
- `internal/tui/tui.go` — suggestions wiring on `stepCompany`,
  load on `modeNew` entry.
- `internal/tui/cmds.go` — `listCompaniesCmd`.
- `internal/events/allowed_values.go` — unchanged. `companies.tags`
  is open vocabulary; no allow-list to drift-detect.

## Related principles

- **Producers don't read on the write path.** Consumer-side
  resolve preserves this for the new `company_id` foreign key.
- **Events describe what happened.** `JobSubmitted` continues to
  describe what the operator submitted (a name); resolution is
  the consumer's job, not the event's.
- **Typed columns over schemaless escape hatches.** `tier` is a
  `CHECK`-constrained column, not a custom tag.
- **No service writes to another's tables.** `companies` is owned
  by `internal/store`; frontends mutate it only via events.
- **UX latency over schema purity.** Inline create-or-link beats a
  cleaner two-step "register company then submit" flow.

## Notes

- **Slug algorithm is deliberately conservative.** v1 is
  `lower(trim(collapse-whitespace))`. It will not collapse
  `"Acme, Inc."` and `"Acme Inc"` into one slug — a `companies
  merge` op handles that. Aggressive normalization (stripping
  legal suffixes, punctuation) is easy to add and hard to undo,
  so v1 leans conservative.
- **Backfill collisions on real data.** The operator's existing
  `jobs.company` strings will slug-collide where case or
  whitespace differs (`"Acme"`, `"acme"`, `" Acme "` → one
  `companies` row); they will *not* collide where punctuation or
  legal-suffix variants differ (`"Acme"`, `"Acme Inc"` → two
  rows). Before running the migration the operator should
  `SELECT DISTINCT company FROM jobs ORDER BY company` to scan
  for unintended near-duplicates and either fix them in-place
  (string `UPDATE`) or accept that the companies panel will need
  a merge afterwards. This pre-flight scan belongs in the
  migration's commit message, not in the SQL.
- **No `CompanyEdited` / `CompanyRegistered` events yet.** Tag
  and notes edits happen as direct Postgres writes from the
  companies panel for v1. This is a deliberate exception to "no
  service writes to another's tables" — the panel is a TUI
  affordance over the same Reader-and-Publisher boundary, and
  the panel's writes are bounded to columns no consumer projects
  from events. If a second frontend (bot, web) needs to edit
  company state, that's the trigger to introduce a
  `CompanyEdited` event and the matching topic. The operator has
  signalled they want to edit companies but accepted deferring
  the event-shaped version for now.
- **`gen_random_uuid()` requires `pgcrypto` or PG13+.** The
  init migration already assumes a modern Postgres; the
  resolver could equally do `INSERT … RETURNING company_id`
  with a producer-supplied id if we ever want to remove the
  built-in dependency. Out of scope.
- **Open question:** should `companies.name` be UNIQUE in
  addition to `slug`? Today two different display names that
  slug-collide would both insert successfully with the first
  one's `name` winning (the `ON CONFLICT DO UPDATE SET
  name = companies.name` is a no-op that leaves the existing
  name in place). That's the right v1 behaviour — first writer
  picks the casing — but it does mean the operator can't
  retroactively re-case a company from the new-job modal. The
  companies panel handles that explicitly via an `UPDATE` on
  `name`.
- **Resolved (per operator review):** tag badges in the
  suggestion list are deferred — punted out of v1.
- **Resolved (per operator review):** suggestion match is
  case-insensitive *substring*, not prefix-only.
- **Resolved (per operator review):** `companies.name` is not
  UNIQUE — first writer picks the casing; the companies panel
  re-cases via `UPDATE`.
- **Resolved (per operator review):** company categorization is
  many-to-many via `companies.tags text[]`, not a scalar
  `tier`-enum column. Open-vocabulary, GIN-indexed.
- **Resolved (per operator review):** the companies panel writes
  tags/notes directly to Postgres for v1; `CompanyEdited` event
  is deferred until a second frontend needs it.
