package jobclient

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"job-tracker/internal/events"
)

// Validators mirror the schema's CHECK constraints and required-field
// rules. They run synchronously inside Publisher methods so a bad
// event never reaches Kafka. Allowed-set values come from
// internal/events/allowed_values.go — a drift-detection test asserts
// they match schema.sql.

func validateSubmitted(ev events.JobSubmitted) error {
	if ev.JobID == "" {
		return fmt.Errorf("%w: job_id is required", ErrMissingJobID)
	}
	if strings.TrimSpace(ev.Title) == "" {
		return fmt.Errorf("%w: title is required", ErrMissingTitle)
	}
	if strings.TrimSpace(ev.Company) == "" {
		return fmt.Errorf("%w: company is required", ErrMissingCompany)
	}
	if strings.TrimSpace(ev.URL) == "" {
		return fmt.Errorf("%w: url is required", ErrMissingURL)
	}
	if err := validateURL(ev.URL); err != nil {
		return err
	}
	if !slices.Contains(events.AllowedStatuses, ev.Status) {
		return fmt.Errorf("%w: %q; allowed: %s",
			ErrInvalidStatus, ev.Status, joinStatuses(events.AllowedStatuses))
	}
	if ev.WorkMode != "" && !slices.Contains(events.AllowedWorkModes, ev.WorkMode) {
		return fmt.Errorf("%w: %q; allowed: %s",
			ErrInvalidWorkMode, ev.WorkMode, joinWorkModes(events.AllowedWorkModes))
	}
	if ev.Seniority != "" && !slices.Contains(events.AllowedSeniorities, ev.Seniority) {
		return fmt.Errorf("%w: %q; allowed: %s",
			ErrInvalidSeniority, ev.Seniority, joinSeniorities(events.AllowedSeniorities))
	}
	if ev.Source != "" && !slices.Contains(events.AllowedSources, ev.Source) {
		return fmt.Errorf("%w: %q; allowed: %s",
			ErrInvalidSource, ev.Source, joinSources(events.AllowedSources))
	}
	if err := validateCompensation(ev.CompMin, ev.CompMax, ev.CompCurrency); err != nil {
		return err
	}
	if ev.Deadline != nil {
		if err := validateDeadline(*ev.Deadline); err != nil {
			return err
		}
	}
	if err := validateTags("tech_tag", ev.TechTags); err != nil {
		return err
	}
	if err := validateTags("custom_tag", ev.CustomTags); err != nil {
		return err
	}
	return nil
}

func validateStatusChanged(ev events.JobStatusChanged) error {
	if ev.JobID == "" {
		return fmt.Errorf("%w: job_id is required", ErrMissingJobID)
	}
	if !slices.Contains(events.AllowedStatuses, ev.Status) {
		return fmt.Errorf("%w: %q; allowed: %s",
			ErrInvalidStatus, ev.Status, joinStatuses(events.AllowedStatuses))
	}
	return nil
}

func validateNoteAdded(ev events.JobNoteAdded) error {
	if ev.JobID == "" {
		return fmt.Errorf("%w: job_id is required", ErrMissingJobID)
	}
	if strings.TrimSpace(ev.Body) == "" {
		return fmt.Errorf("%w: body is required", ErrMissingNoteBody)
	}
	return nil
}

// validateInterviewRecorded validates both the "schedule" and "update"
// shapes of JobInterviewRecorded. Round is optional at this layer —
// a partial update may omit it — but when provided must be in the
// allowed set. The schema's NOT NULL on round catches a malformed
// first-insert as defense in depth.
func validateInterviewRecorded(ev events.JobInterviewRecorded) error {
	if ev.JobID == "" {
		return fmt.Errorf("%w: job_id is required", ErrMissingJobID)
	}
	if ev.InterviewID == "" {
		return fmt.Errorf("%w: interview_id is required", ErrMissingInterviewID)
	}
	if ev.Round != "" && !slices.Contains(events.AllowedInterviewRounds, ev.Round) {
		return fmt.Errorf("%w: %q; allowed: %s",
			ErrInvalidInterviewRound, ev.Round, joinRounds(events.AllowedInterviewRounds))
	}
	return nil
}

// validateURL requires a parseable URL with scheme in {http, https}
// and a non-empty Host. Stricter checks (DNS, reachability) are
// network calls that don't belong in input validation.
func validateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: %q is not a parseable URL", ErrInvalidURL, raw)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("%w: %q; scheme must be http or https", ErrInvalidURL, raw)
	}
	if u.Host == "" {
		return fmt.Errorf("%w: %q; missing host", ErrInvalidURL, raw)
	}
	return nil
}

// validateCompensation validates the comp_{min,max,currency} group.
// All three are optional; when provided they're checked together. A
// single sentinel covers the three failure modes — the message names
// the specific problem so frontends can render verbatim.
func validateCompensation(minV, maxV *float64, currency string) error {
	if minV != nil && *minV < 0 {
		return fmt.Errorf("%w: comp_min %v is negative", ErrInvalidCompensation, *minV)
	}
	if maxV != nil && *maxV < 0 {
		return fmt.Errorf("%w: comp_max %v is negative", ErrInvalidCompensation, *maxV)
	}
	if minV != nil && maxV != nil && *minV > *maxV {
		return fmt.Errorf("%w: comp_min %v > comp_max %v", ErrInvalidCompensation, *minV, *maxV)
	}
	if currency != "" && !slices.Contains(events.AllowedCurrencies, currency) {
		return fmt.Errorf("%w: comp_currency %q; allowed: %s",
			ErrInvalidCompensation, currency, strings.Join(events.AllowedCurrencies, ", "))
	}
	return nil
}

// validateDeadline rejects deadlines strictly before today (UTC).
// A deadline equal to today passes — postings often list the closing
// day itself. Most postings carry no deadline, so timezone precision
// isn't worth the cross-frontend coordination cost.
func validateDeadline(d time.Time) error {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	if d.UTC().Before(today) {
		return fmt.Errorf("%w: %s is before today (%s)",
			ErrInvalidDeadline, d.UTC().Format("2006-01-02"), today.Format("2006-01-02"))
	}
	return nil
}

func validateTags(kind string, tags []string) error {
	for _, t := range tags {
		if strings.TrimSpace(t) == "" {
			return fmt.Errorf("%w: empty %s", ErrInvalidTag, kind)
		}
	}
	return nil
}

// Small typed-slice joiners — keep validator messages tidy without
// reaching for generics or reflect.
func joinStatuses(xs []events.JobStatus) string {
	ss := make([]string, len(xs))
	for i, x := range xs {
		ss[i] = string(x)
	}
	return strings.Join(ss, ", ")
}

func joinWorkModes(xs []events.WorkMode) string {
	ss := make([]string, len(xs))
	for i, x := range xs {
		ss[i] = string(x)
	}
	return strings.Join(ss, ", ")
}

func joinSeniorities(xs []events.Seniority) string {
	ss := make([]string, len(xs))
	for i, x := range xs {
		ss[i] = string(x)
	}
	return strings.Join(ss, ", ")
}

func joinSources(xs []events.Source) string {
	ss := make([]string, len(xs))
	for i, x := range xs {
		ss[i] = string(x)
	}
	return strings.Join(ss, ", ")
}

func joinRounds(xs []events.InterviewRound) string {
	ss := make([]string, len(xs))
	for i, x := range xs {
		ss[i] = string(x)
	}
	return strings.Join(ss, ", ")
}
