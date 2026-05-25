-- ADR 0011 — add jobs.expected_comp. Distinct from comp_min/comp_max:
-- the posting's advertised range vs. the operator's quoted number.
-- Currency is shared with comp_currency. No CHECK constraint —
-- positivity is enforced producer-side (ErrInvalidExpectedComp).

ALTER TABLE jobs ADD COLUMN expected_comp numeric;
