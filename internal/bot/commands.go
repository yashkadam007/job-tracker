package bot

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"job-tracker/internal/events"
	"job-tracker/internal/jobclient"
	"job-tracker/internal/telegram"
)

// dispatchCommand parses the leading token (/foo) and routes. Unknown
// commands get a usage reply instead of being silently dropped — they
// usually mean a typo.
func (b *Bot) dispatchCommand(ctx context.Context, m *telegram.Message, text string) {
	cmd, rest := splitCommand(text)
	// Any new command implicitly cancels a half-finished /add — the
	// user has moved on.
	b.state.clearPending(m.Chat.ID)
	switch cmd {
	case "/start", "/help":
		b.reply(ctx, helpText)
	case "/add":
		b.cmdAdd(ctx, m.Chat.ID, rest)
	case "/list":
		b.cmdList(ctx, m.Chat.ID, rest)
	case "/applied":
		b.cmdStatusByIndex(ctx, m.Chat.ID, rest, events.StatusApplied)
	case "/rejected":
		b.cmdStatusByIndex(ctx, m.Chat.ID, rest, events.StatusRejected)
	case "/offer":
		b.cmdStatusByIndex(ctx, m.Chat.ID, rest, events.StatusOffer)
	default:
		b.reply(ctx, "Unknown command. "+helpText)
	}
}

const helpText = `Commands:
/add <url> — capture a job (will prompt for title/company if missing)
/list [saved] — show recent jobs (numbered)
/applied <n> — mark job N from last /list as applied
/rejected <n> — mark job N from last /list as rejected
/offer <n> — mark job N from last /list as offer`

// splitCommand returns ("/cmd", "rest of text"). Bot-mention suffixes
// like "/list@MyBot" are stripped to keep the dispatch tidy.
func splitCommand(text string) (cmd, rest string) {
	parts := strings.SplitN(text, " ", 2)
	cmd = parts[0]
	if at := strings.Index(cmd, "@"); at >= 0 {
		cmd = cmd[:at]
	}
	if len(parts) == 2 {
		rest = strings.TrimSpace(parts[1])
	}
	return cmd, rest
}

func (b *Bot) cmdAdd(ctx context.Context, chatID int64, rest string) {
	if rest == "" {
		b.reply(ctx, "usage: /add <url>")
		return
	}
	url := strings.Fields(rest)[0]
	b.reply(ctx, "Fetching "+url+" …")

	title, company := fetchTitleCompany(ctx, url)
	p := &pendingJob{url: url, title: title, company: company}
	b.continueAddFlow(ctx, chatID, p)
}

// continueAdd is reached when the user replies to an outstanding /add
// prompt. The current question's answer is the entire user text — no
// re-parsing, no field names.
func (b *Bot) continueAdd(ctx context.Context, chatID int64, p *pendingJob, text string) {
	switch p.awaiting {
	case "title":
		p.title = text
	case "company":
		p.company = text
	}
	p.awaiting = ""
	b.continueAddFlow(ctx, chatID, p)
}

// continueAddFlow advances the /add state machine: ask for the next
// missing field, or — once everything is filled in — publish.
func (b *Bot) continueAddFlow(ctx context.Context, chatID int64, p *pendingJob) {
	switch {
	case p.title == "":
		p.awaiting = "title"
		b.state.setPending(chatID, p)
		b.reply(ctx, "What's the job title?")
	case p.company == "":
		p.awaiting = "company"
		b.state.setPending(chatID, p)
		b.reply(ctx, "Which company?")
	default:
		b.state.clearPending(chatID)
		b.publishAdd(ctx, p)
	}
}

func (b *Bot) publishAdd(ctx context.Context, p *pendingJob) {
	ev := events.JobSubmitted{
		EventID:     uuid.NewString(),
		JobID:       uuid.NewString(),
		URL:         p.url,
		Title:       p.title,
		Company:     p.company,
		Status:      events.StatusSaved,
		SubmittedAt: time.Now().UTC(),
	}
	pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := b.cfg.Publisher.Submit(pubCtx, ev); err != nil {
		if jobclient.IsValidationError(err) {
			b.reply(ctx, err.Error())
			return
		}
		log.Printf("bot: publish JobSubmitted: %v", err)
		b.reply(ctx, "Failed to save: "+err.Error())
		return
	}
	b.reply(ctx, fmt.Sprintf("Saved: %s @ %s\n%s\njob_id: %s",
		ev.Title, ev.Company, ev.URL, ev.JobID))
}

func (b *Bot) cmdList(ctx context.Context, chatID int64, rest string) {
	filter := jobclient.ListFilter{Limit: 20}
	if rest != "" {
		// Only "saved" is a supported argument today; anything else is
		// likely a typo and worth surfacing rather than silently
		// returning the whole list.
		status := events.JobStatus(strings.ToLower(rest))
		if !isKnownStatus(status) {
			b.reply(ctx, "Unknown status filter. Try /list or /list saved.")
			return
		}
		filter.Status = &status
	}
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	jobs, err := b.cfg.Reader.List(readCtx, filter)
	if err != nil {
		log.Printf("bot: list: %v", err)
		b.reply(ctx, "Failed to list: "+err.Error())
		return
	}
	if len(jobs) == 0 {
		b.state.setList(chatID, nil)
		b.reply(ctx, "No jobs.")
		return
	}
	b.state.setList(chatID, jobs)

	var buf strings.Builder
	for i, j := range jobs {
		fmt.Fprintf(&buf, "%d. %s @ %s (%s)\n   %s\n", i+1, j.Title, j.Company, j.Status, j.URL)
	}
	b.reply(ctx, buf.String())
}

// cmdStatusByIndex implements /applied <n>, /rejected <n>, /offer <n>.
// N is 1-based against the last /list result for the same chat — if
// the user hasn't /list'd yet, there's no index to resolve.
func (b *Bot) cmdStatusByIndex(ctx context.Context, chatID int64, rest string, status events.JobStatus) {
	if rest == "" {
		b.reply(ctx, "usage: /"+string(status)+" <n>  (run /list first)")
		return
	}
	n, err := strconv.Atoi(strings.Fields(rest)[0])
	if err != nil {
		b.reply(ctx, "expected a number after /"+string(status))
		return
	}
	job, ok := b.state.listJob(chatID, n)
	if !ok {
		b.reply(ctx, fmt.Sprintf("No job #%d in the last /list (or list is empty).", n))
		return
	}
	b.publishStatus(ctx, job.JobID, status)
	b.reply(ctx, fmt.Sprintf("%s @ %s → %s", job.Title, job.Company, status))
}

func (b *Bot) publishStatus(ctx context.Context, jobID string, status events.JobStatus) {
	ev := events.JobStatusChanged{
		EventID:   uuid.NewString(),
		JobID:     jobID,
		Status:    status,
		ChangedAt: time.Now().UTC(),
	}
	pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := b.cfg.Publisher.ChangeStatus(pubCtx, ev); err != nil {
		if jobclient.IsValidationError(err) {
			b.reply(ctx, err.Error())
			return
		}
		log.Printf("bot: publish JobStatusChanged: %v", err)
		b.reply(ctx, "Failed to update status: "+err.Error())
	}
}

// isKnownStatus guards against typos in `/list <status>`. The list is
// hard-coded rather than reflected from the events package so a new
// status appearing in events doesn't accidentally become a bot-exposed
// filter without a deliberate edit here.
func isKnownStatus(s events.JobStatus) bool {
	switch s {
	case events.StatusSaved, events.StatusApplied, events.StatusInterview,
		events.StatusRejected, events.StatusOffer, events.StatusWithdrawn:
		return true
	}
	return false
}
