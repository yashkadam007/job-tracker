package jobclient

import "errors"

// ErrNotFound is returned by Reader.Get when the requested job is not
// in the table. Callers branch with errors.Is.
var ErrNotFound = errors.New("jobclient: job not found")

// Producer-side validation sentinels. The Publisher returns these
// (wrapped with fmt.Errorf("...: %w", sentinel, …)) when an event
// would fail the schema's CHECK constraints or violates a required-
// field rule. Frontends branch with errors.Is and render the wrapped
// message verbatim.
var (
	ErrInvalidStatus         = errors.New("invalid status")
	ErrInvalidWorkMode       = errors.New("invalid work mode")
	ErrInvalidSeniority      = errors.New("invalid seniority")
	ErrInvalidSource         = errors.New("invalid source")
	ErrInvalidInterviewRound = errors.New("invalid interview round")
	ErrInvalidURL            = errors.New("invalid url")
	ErrMissingTitle          = errors.New("missing title")
	ErrMissingCompany        = errors.New("missing company")
	ErrMissingURL            = errors.New("missing url")
	ErrMissingJobID          = errors.New("missing job id")
	ErrMissingInterviewID    = errors.New("missing interview id")
	ErrMissingNoteBody       = errors.New("missing note body")
	ErrInvalidCompensation   = errors.New("invalid compensation")
	ErrInvalidDeadline       = errors.New("invalid deadline")
	ErrInvalidTag            = errors.New("invalid tag")
	ErrInvalidPriority       = errors.New("invalid priority")
	ErrInvalidExpectedComp   = errors.New("invalid expected compensation")
	ErrEmptyEdit             = errors.New("empty edit")
)

// validationSentinels lists every error a validator may return.
// IsValidationError checks against this set — frontends use it to
// distinguish "user typed something bad" from "Kafka is unreachable".
var validationSentinels = []error{
	ErrInvalidStatus,
	ErrInvalidWorkMode,
	ErrInvalidSeniority,
	ErrInvalidSource,
	ErrInvalidInterviewRound,
	ErrInvalidURL,
	ErrMissingTitle,
	ErrMissingCompany,
	ErrMissingURL,
	ErrMissingJobID,
	ErrMissingInterviewID,
	ErrMissingNoteBody,
	ErrInvalidCompensation,
	ErrInvalidDeadline,
	ErrInvalidTag,
	ErrInvalidPriority,
	ErrInvalidExpectedComp,
	ErrEmptyEdit,
}

// IsValidationError reports whether err wraps any producer-side
// validation sentinel. A true result means the cause was user input
// the frontend could have prevented; render the message verbatim and
// do not retry.
func IsValidationError(err error) bool {
	for _, s := range validationSentinels {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}
