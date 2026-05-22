package bot

import (
	"context"
	"fmt"
	"log"
	"time"

	"job-tracker/internal/events"
	"job-tracker/internal/telegram"
)

// handleCallback dispatches an inline-keyboard tap. The two actions
// today: status change (Applied / Rejected) and snooze. The visible
// answerCallbackQuery is best-effort cosmetic; the side effect — Kafka
// publish or reminders insert — is the source of truth.
func (b *Bot) handleCallback(ctx context.Context, q *telegram.CallbackQuery) {
	cb, err := telegram.ParseCallback(q.Data)
	if err != nil {
		log.Printf("bot: callback parse: %v", err)
		b.answer(ctx, q.ID, "bad callback")
		return
	}
	switch cb.Action {
	case telegram.CallbackActionStatus:
		if len(cb.Args) != 2 {
			b.answer(ctx, q.ID, "bad status callback")
			return
		}
		status, jobID := events.JobStatus(cb.Args[0]), cb.Args[1]
		b.publishStatus(ctx, jobID, status)
		b.answer(ctx, q.ID, "Marked "+string(status))
		b.reply(ctx, fmt.Sprintf("✓ job %s → %s", jobID, status))
	case telegram.CallbackActionSnooze:
		if len(cb.Args) != 1 {
			b.answer(ctx, q.ID, "bad snooze callback")
			return
		}
		jobID := cb.Args[0]
		if err := b.snooze(ctx, jobID); err != nil {
			log.Printf("bot: snooze job_id=%s: %v", jobID, err)
			b.answer(ctx, q.ID, "snooze failed")
			return
		}
		b.answer(ctx, q.ID, "Snoozed 1d")
		b.reply(ctx, fmt.Sprintf("💤 job %s — snoozed 1d", jobID))
	default:
		b.answer(ctx, q.ID, "unknown action")
	}
}

// snooze inserts a fresh reminder one day out. We deliberately reuse
// ReminderFollowupSaved (per ADR 0003 Notes) rather than introducing a
// new kind — the wording on the next reminder is the same, "still
// interested?", which fits a snoozed item.
//
// This is the bot's one direct DB write. event_id-keyed idempotency
// already guards against double-publish at the update level, so a
// raw INSERT (no event_id column on reminders) is fine.
func (b *Bot) snooze(ctx context.Context, jobID string) error {
	due := time.Now().UTC().Add(24 * time.Hour)
	_, err := b.cfg.Pool.Exec(ctx,
		`INSERT INTO reminders (job_id, kind, due_at) VALUES ($1, $2, $3)`,
		jobID, string(events.ReminderFollowupSaved), due)
	return err
}

func (b *Bot) answer(ctx context.Context, queryID, text string) {
	sendCtx, cancel := contextWithSendTimeout(ctx)
	defer cancel()
	if err := b.tg.AnswerCallbackQuery(sendCtx, queryID, text); err != nil {
		log.Printf("bot: answerCallbackQuery: %v", err)
	}
}

