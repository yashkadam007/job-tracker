// Package bot is the Telegram two-way interface — long-poll loop,
// command dispatch, callback handling, conversation state. The bot is
// a Kafka producer (via jobclient.Publisher) and a read-only Postgres
// reader (via jobclient.Reader). The one direct DB write it performs
// is a reminders insert for the "Snooze 1d" callback — see ADR 0003.
package bot

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"job-tracker/internal/jobclient"
	"job-tracker/internal/telegram"
)

// Consumer is the namespace used in processed_events for Telegram
// update_id dedup.
const Consumer = "bot"

// Config bundles construction args. All required.
type Config struct {
	Token          string // Telegram bot token (BotFather)
	ChatID         string // authoritative single chat; all others are dropped
	PollTimeout    int    // long-poll timeout in seconds (Telegram caps at 50)
	Publisher      *jobclient.Publisher
	Reader         *jobclient.Reader
	Pool           *pgxpool.Pool
}

type Bot struct {
	cfg    Config
	tg     *telegram.Client
	state  *stateStore
	chatID int64 // parsed once from cfg.ChatID
}

func New(cfg Config) (*Bot, error) {
	if cfg.Token == "" || cfg.ChatID == "" {
		return nil, errors.New("bot: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required")
	}
	if cfg.Publisher == nil || cfg.Reader == nil || cfg.Pool == nil {
		return nil, errors.New("bot: Publisher, Reader, and Pool are required")
	}
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = 25
	}
	cid, err := parseChatID(cfg.ChatID)
	if err != nil {
		return nil, err
	}
	return &Bot{
		cfg:    cfg,
		tg:     telegram.New(cfg.Token),
		state:  newStateStore(),
		chatID: cid,
	}, nil
}

// Run is the long-poll loop. Returns when ctx is cancelled. The offset
// is in-memory only — on restart we start from 0 and rely on
// processed_events to drop anything we've already handled.
func (b *Bot) Run(ctx context.Context) error {
	var offset int64
	log.Printf("bot: long-polling Telegram (chat_id=%d, timeout=%ds)", b.chatID, b.cfg.PollTimeout)
	for {
		if ctx.Err() != nil {
			return nil
		}
		updates, err := b.tg.GetUpdates(ctx, offset, b.cfg.PollTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("bot: getUpdates: %v", err)
			// Sleep-with-context so we don't hot-loop on a flapping
			// endpoint, but stay responsive to SIGTERM.
			if !sleepCtx(ctx, b.cfg.PollTimeout) {
				return nil
			}
			continue
		}
		for _, u := range updates {
			if u.UpdateID >= offset {
				offset = u.UpdateID + 1
			}
			b.handleUpdate(ctx, u)
		}
	}
}

func (b *Bot) handleUpdate(ctx context.Context, u telegram.Update) {
	// Chat-ID allowlist is the only authentication. Drop anything from
	// a chat that isn't ours, *before* the dedup write — we don't even
	// want a ledger row for unrelated chats.
	if !b.fromOurChat(u) {
		return
	}
	claimed, err := b.claimUpdate(ctx, u.UpdateID)
	if err != nil {
		log.Printf("bot: claim update_id=%d: %v", u.UpdateID, err)
		return
	}
	if !claimed {
		// Telegram redelivered after a crash between handle and the
		// next getUpdates ack. Cosmetic ack only — the side effect
		// already happened on the first run.
		if u.CallbackQuery != nil {
			ackCtx, cancel := contextWithSendTimeout(ctx)
			_ = b.tg.AnswerCallbackQuery(ackCtx, u.CallbackQuery.ID, "")
			cancel()
		}
		return
	}
	switch {
	case u.CallbackQuery != nil:
		b.handleCallback(ctx, u.CallbackQuery)
	case u.Message != nil && u.Message.Text != "":
		b.handleMessage(ctx, u.Message)
	}
}

func (b *Bot) fromOurChat(u telegram.Update) bool {
	switch {
	case u.Message != nil:
		return u.Message.Chat.ID == b.chatID
	case u.CallbackQuery != nil && u.CallbackQuery.Message != nil:
		return u.CallbackQuery.Message.Chat.ID == b.chatID
	}
	return false
}

func (b *Bot) handleMessage(ctx context.Context, m *telegram.Message) {
	text := strings.TrimSpace(m.Text)
	// Commands begin with `/`; everything else is either a reply to a
	// pending /add prompt or unhandled chatter.
	if strings.HasPrefix(text, "/") {
		b.dispatchCommand(ctx, m, text)
		return
	}
	if pending := b.state.pending(m.Chat.ID); pending != nil {
		b.continueAdd(ctx, m.Chat.ID, pending, text)
		return
	}
	b.reply(ctx, "Unrecognised input. Try /list or /add <url>.")
}

// reply is the common-case sender: text-only, no buttons, to the
// configured chat. Logs but does not return errors — there's nothing
// useful for the long-poll loop to do with one.
func (b *Bot) reply(ctx context.Context, text string) {
	sendCtx, cancel := contextWithSendTimeout(ctx)
	defer cancel()
	if err := b.tg.SendMessage(sendCtx, b.cfg.ChatID, text, telegram.SendMessageOptions{}); err != nil {
		log.Printf("bot: sendMessage: %v", err)
	}
}

// claimUpdate inserts into processed_events. Returns (true, nil) the
// first time it sees update_id, (false, nil) on duplicate. The event
// ID is namespaced so a Kafka event_id and an update_id can't collide
// even if both happen to be small integers.
func (b *Bot) claimUpdate(ctx context.Context, updateID int64) (bool, error) {
	ct, err := b.cfg.Pool.Exec(ctx,
		`INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		Consumer, eventIDForUpdate(updateID))
	if err != nil {
		return false, err
	}
	return ct.RowsAffected() > 0, nil
}

// stateStore is the per-chat in-memory scratch space: last /list result
// (for numeric shortcuts) and any in-flight /add follow-up. Single
// operator, but the mutex is cheap insurance.
type stateStore struct {
	mu     sync.Mutex
	chats  map[int64]*chatState
}

func newStateStore() *stateStore {
	return &stateStore{chats: make(map[int64]*chatState)}
}

func (s *stateStore) get(chatID int64) *chatState {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.chats[chatID]
	if !ok {
		c = &chatState{}
		s.chats[chatID] = c
	}
	return c
}

func (s *stateStore) setList(chatID int64, jobs []jobclient.Job) {
	c := s.get(chatID)
	s.mu.Lock()
	defer s.mu.Unlock()
	c.list = jobs
}

func (s *stateStore) listJob(chatID int64, n int) (jobclient.Job, bool) {
	c := s.get(chatID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if n < 1 || n > len(c.list) {
		return jobclient.Job{}, false
	}
	return c.list[n-1], true
}

func (s *stateStore) setPending(chatID int64, p *pendingJob) {
	c := s.get(chatID)
	s.mu.Lock()
	defer s.mu.Unlock()
	c.pending = p
}

func (s *stateStore) pending(chatID int64) *pendingJob {
	c := s.get(chatID)
	s.mu.Lock()
	defer s.mu.Unlock()
	return c.pending
}

func (s *stateStore) clearPending(chatID int64) {
	c := s.get(chatID)
	s.mu.Lock()
	defer s.mu.Unlock()
	c.pending = nil
}

type chatState struct {
	list    []jobclient.Job
	pending *pendingJob
}

type pendingJob struct {
	url      string
	title    string
	company  string
	awaiting string // "title" or "company"
}
