package events

// Allowed-set slices mirror the CHECK constraints in internal/db/schema.sql.
// They are the single source of truth on the Go side, consumed by the
// producer-side validator in jobclient and by any future input-helper
// (TUI dropdown, bot keyboard). A drift-detection test in jobclient
// parses schema.sql and asserts these match.

var AllowedStatuses = []JobStatus{
	StatusSaved,
	StatusApplied,
	StatusInterview,
	StatusRejected,
	StatusOffer,
	StatusWithdrawn,
}

var AllowedWorkModes = []WorkMode{
	WorkModeOnsite,
	WorkModeHybrid,
	WorkModeRemote,
}

var AllowedSeniorities = []Seniority{
	SeniorityIntern,
	SeniorityJunior,
	SeniorityMid,
	SenioritySenior,
	SeniorityStaff,
	SeniorityPrincipal,
}

var AllowedSources = []Source{
	SourceLinkedIn,
	SourceIndeed,
	SourceReferral,
	SourceCompanySite,
	SourceRecruiter,
	SourceOther,
}

var AllowedInterviewRounds = []InterviewRound{
	RoundPhoneScreen,
	RoundTechnical,
	RoundBehavioral,
	RoundSystemDesign,
	RoundOnsite,
	RoundFinal,
	RoundOther,
}

// AllowedCurrencies is the producer-side allowed set for comp_currency.
// Postgres has no CHECK on this column (it's free text in the schema),
// so this slice is purely a producer convention — not enforced by the
// drift-detection test.
var AllowedCurrencies = []string{"INR", "USD", "EUR", "AUD"}
