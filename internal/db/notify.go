package db

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
)

// JobsChangedChannel is the Postgres LISTEN/NOTIFY channel the Store
// consumer uses to wake readers after a projection commit (ADR 0012).
const JobsChangedChannel = "jobs_changed"

// NotifyJobsChanged fires pg_notify inside the apply transaction so the
// wakeup is delivered if and only if the commit succeeds. kind is one of
// "submitted", "status_changed", "edited", "note_added",
// "interview_recorded".
func NotifyJobsChanged(ctx context.Context, tx pgx.Tx, jobID, kind string) error {
	payload, err := json.Marshal(map[string]string{"job_id": jobID, "event": kind})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify($1, $2)`, JobsChangedChannel, string(payload))
	return err
}
