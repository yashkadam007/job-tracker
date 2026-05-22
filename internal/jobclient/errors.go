package jobclient

import "errors"

// ErrNotFound is returned by Reader.Get when the requested job is not
// in the table. Callers branch with errors.Is.
var ErrNotFound = errors.New("jobclient: job not found")
