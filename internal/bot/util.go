package bot

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// parseChatID accepts the env var as a decimal int64 (Telegram chat
// IDs are signed — groups are negative). Returned as int64 so we can
// compare directly with Update.Message.Chat.ID without an alloc per
// update.
func parseChatID(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bot: TELEGRAM_CHAT_ID must be an integer, got %q: %w", s, err)
	}
	return n, nil
}

// sleepCtx blocks for `seconds` or until ctx is cancelled. Returns
// true if the full sleep elapsed, false if the context cancelled.
func sleepCtx(ctx context.Context, seconds int) bool {
	if seconds <= 0 {
		return true
	}
	t := time.NewTimer(time.Duration(seconds) * time.Second)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// contextWithSendTimeout caps an individual outbound Telegram call so
// a hung TCP socket can't stall the long-poll loop indefinitely.
func contextWithSendTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, 10*time.Second)
}

// eventIDForUpdate namespaces the Telegram update_id inside
// processed_events so it can't collide with a Kafka event_id.
func eventIDForUpdate(updateID int64) string {
	return fmt.Sprintf("tg-update-%d", updateID)
}
