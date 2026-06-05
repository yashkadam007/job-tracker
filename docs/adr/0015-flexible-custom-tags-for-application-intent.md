# ADR 0015 — Flexible custom tags for application intent

## Issue

The tracker already has two tag lanes on every job:

- `tech_tags` for technology / stack labels.
- `custom_tags` for the operator's own labels.

`custom_tags` exists in the schema (`jobs.custom_tags`), the submit
event (`JobSubmitted.CustomTags`), the sparse edit event
(`JobEdited.CustomTags`), the Store consumer, the Reader projection,
and the TUI edit modal (ADR 0011). It is already the open-ended
metadata escape hatch.

A new workflow need has emerged: some saved jobs are not just "one
more job to volume apply to". They need a visible signal that the
operator wants to apply with extra effort: ask employees for a
referral, cold-email the application, write a custom resume, add a
cover letter, read more about the company, or treat it as a
dream-company / high-priority target.

The first instinct is to add an "intent" field, but that field would
be misleadingly narrow. The operator's examples are not one fixed
state machine:

- **Company type:** `startup`, `scaleup`, `enterprise`,
  `product-company`, `consulting`, `fintech`, `healthtech`.
- **Application source / route:** `linkedin`, `wellfound`,
  `company-website`, `referral`, `recruiter`, `cold-outreach`.
- **Application characteristics:** `high-priority`,
  `dream-company`, `quick-apply`, `custom-resume`, `cover-letter`,
  `take-home-assignment`.

Those labels will grow. Some are about company classification, some
are about sourcing, some are about the amount of application effort,
and some are temporary action hints. A fixed enum like
`application_intent = referral|email|research|standard` would force
the operator to either collapse different meanings into one value or
keep asking for schema changes whenever the vocabulary changes.

The tracker holds live data. Schema additions go through
`golang-migrate` (ADR 0009); write-path changes go through Kafka
events (ADR 0001); validation runs producer-side (ADR 0005); the TUI
is the primary frontend (ADR 0004); the edit surface is `JobEdited`
(ADR 0011). Any solution must respect those boundaries and avoid a
new backend concept if the existing model already captures the intent.

## Decision

Use `custom_tags` as the canonical model for application intent and
other operator-defined job labels. **No new schema column. No new
event topic. No fixed allowed-value set. No new status.**

This is a frontend ergonomics change around an existing backend model:

**1. Keep tags free-form and user-created.**

`custom_tags` remains an open `text[]`. The operator may create any
trimmed, non-empty tag at submit or edit time:

```
startup
referral
cold-outreach
high-priority
dream-company
custom-resume
cover-letter
```

The app should not enforce a taxonomy. The example groups above are
useful conventions, not a validation list. Unknown tags are valid by
definition.

**2. Make `custom_tags` easy to add when creating a job in the TUI.**

The TUI's new-job modal gains an optional tags step after company:

```
URL → title → company → custom tags
```

The step is optional. Pressing Enter on an empty value submits the job
without custom tags. Typing a comma-separated list submits those tags
on `JobSubmitted.CustomTags`.

This is the narrow exception to ADR 0010's three-field submit modal:
the new step is optional metadata, does not create a second modal, and
exists because the operator specifically wants to mark high-effort jobs
at save time instead of saving first and immediately reopening edit.

**3. Reuse the existing edit modal for post-submit tag changes.**

The existing `custom tags` field in `modeEdit` remains the post-submit
entry point. It continues to publish `JobEdited.CustomTags` through the
existing `job.edited` topic.

**4. Add TUI tag suggestions from existing data.**

The TUI derives a suggestion list from the current Reader snapshot's
`CustomTags`. When the operator is editing a comma-separated tag field,
suggestions match the token currently being typed. Selecting a
suggestion fills that token; typing a brand-new tag remains allowed.

The suggestion source is read-side only. Producers still do not read
Postgres on the write path (ADR 0001), and no tag registry table is
introduced.

**5. Include tags in TUI search.**

The TUI's local search should match title, company, URL, `tech_tags`,
and `custom_tags`. Searching for `referral`, `custom-resume`, or
`startup` should surface matching jobs without adding a separate tag
filter mode in v1.

**6. Show `custom_tags` in the TUI table as colored badges.**

The list table should expose the effort signal without requiring the
operator to move the cursor to each row's detail panel. Add a compact
tags column that renders `custom_tags` as terminal badges:

```
[referral] [custom-resume] [startup]
```

Badges are visual treatment only; the underlying values stay plain
strings in `custom_tags`. Badge colors should be deterministic from the
tag text so a tag keeps the same color across rows and reloads. The
table still has to obey fixed column widths: if a row has more tags
than fit, render the leading badges and truncate with a compact
overflow marker rather than letting ANSI styling bleed into adjacent
columns.

## Status

Implemented.

## Group

Frontend / Tagging.

## Assumptions

- `custom_tags` is the right persistence model. The operator wants
  flexible labels that can grow, not a fixed backend enum.
- Tags are not mutually exclusive. A job can be both `startup` and
  `custom-resume`, or both `dream-company` and `referral`.
- The TUI is the only frontend that needs this ergonomic improvement
  in v1. CLI and bot can continue using the existing submit/edit
  capabilities.
- Tag spelling is operator-owned. The app trims whitespace and drops
  empty values, but does not force lowercase or replace spaces with
  hyphens.
- Colored badges are a TUI presentation detail. They do not change the
  stored tag value, event payload, or Reader projection.
- Existing custom tags remain valid. No backfill, rename, or migration
  is needed.

## Constraints

- **No schema change.** `jobs.custom_tags text[] NOT NULL DEFAULT '{}'`
  already exists. A new migration would add another model for data the
  app can already store.
- **No new event topic.** `JobSubmitted.CustomTags` and
  `JobEdited.CustomTags` already carry the write path. Adding
  `job.tag.added` or `job.intent.changed` would duplicate
  `job.edited` semantics.
- **No allowed-value drift test.** `custom_tags` is deliberately open.
  ADR 0005's allowed-set pattern applies to constrained fields like
  status, work mode, source, and interview round, not operator labels.
- **Read-side suggestions only.** Suggestions come from the TUI's
  loaded job snapshot. They are a convenience, not a registry, and
  stale suggestions are harmless because any typed tag is accepted.
- **ANSI-safe table rendering.** Badge styling must not break
  `bubbles/table` width calculations. The implementation should build
  a fixed-width rendered cell, truncate by visible width, and only then
  apply lipgloss styling to badge spans.
- **Existing tag replacement semantics stay.** Editing `custom_tags`
  replaces the array, matching ADR 0011's sparse edit convention. This
  ADR does not add append/remove patch operations.
- **Push-based reconciliation is unchanged.** Store still writes the
  job row and emits `NOTIFY jobs_changed`; the TUI reloads through the
  existing ADR 0012 path.

## Positions

Alternatives considered:

1. **Use existing `custom_tags`, improve TUI add/search/suggestion
   ergonomics** (this decision).
2. **Add a new `application_intent` enum column.** Rejected — the
   desired labels are not one axis. `referral`, `startup`,
   `custom-resume`, and `dream-company` can all be true for the same
   job, and the vocabulary is expected to grow.
3. **Use status for intent, for example a new `intent_to_apply`
   status.** Rejected — status tracks pipeline position (`saved`,
   `applied`, `assessment`, etc.). "Needs referral outreach" is
   orthogonal to whether a job is saved or applied.
4. **Add a tag registry table.** Rejected for v1 — suggestions can be
   derived from existing job rows. A registry would add CRUD, merge,
   delete, and casing policy before the operator has asked for global
   tag management.
5. **Use notes instead of tags.** Rejected — notes are append-only
   timeline entries. They preserve narrative context, but they are not
   ergonomic for search, grouping, or repeated labels.
6. **Keep behavior as-is and rely on the existing edit modal only.**
   Rejected — the operator wants to save some jobs *with* the effort
   signal already attached. Saving a job and immediately reopening edit
   is extra friction for the core workflow.
7. **Show tags only in the detail panel.** Rejected — detail rendering
   helps after selecting a row, but the operator needs to scan a list of
   saved jobs and immediately see which ones deserve referral outreach,
   custom-resume work, or other high-effort treatment.

## Argument

- **The existing model already represents the need.** `custom_tags`
  was created for operator-defined labels. Application intent is an
  operator-defined label, not a lifecycle state.
- **Flexibility is more valuable than type safety here.** A fixed
  intent enum would make the first two or three labels neat and the
  fourth one painful. The operator has already named enough categories
  to show the vocabulary will not stay closed.
- **No backend churn is the correct cost profile.** The write path,
  storage, idempotency, and read projection already exist. The missing
  piece is TUI affordance: add at creation time, discover existing
  tags, show tags in the list, and search by tag.
- **Tags compose.** A single job can carry company type, source, and
  application-effort labels simultaneously. A scalar intent field
  cannot do that without becoming another tag list under a different
  name.
- **Badges make effort visible at scan time.** The list table is where
  the operator decides what to work next. Colored badges make
  `referral`, `custom-resume`, and `dream-company` visible before the
  detail panel is opened.
- **This preserves future options.** If tag usage later needs reports,
  server-side filtering, saved views, or tag cleanup tools, those can
  build on `custom_tags` without migrating away from this decision.
