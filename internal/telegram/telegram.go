// Package telegram is a thin wrapper around the Telegram Bot HTTP API.
//
// Surface: the three endpoints the notifier + bot need (sendMessage,
// getUpdates, answerCallbackQuery). Hand-rolled with net/http +
// encoding/json — pulling in a full Bot API library for three calls
// would be heavier than the wrapper itself.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is safe for concurrent use.
type Client struct {
	token string
	http  *http.Client
}

func New(token string) *Client {
	return &Client{
		token: token,
		// getUpdates uses long polling with its own per-call timeout
		// budget; this is the safety net for connection-level hangs.
		http: &http.Client{Timeout: 60 * time.Second},
	}
}

// InlineKeyboardButton mirrors Telegram's wire type. Only the two fields
// we care about (text label + callback_data payload) are modelled —
// URL/login_url/etc. aren't used.
type InlineKeyboardButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data,omitempty"`
}

// InlineKeyboardMarkup is what gets serialised into reply_markup. The
// outer slice is rows; the inner slice is buttons within a row.
type InlineKeyboardMarkup struct {
	InlineKeyboard [][]InlineKeyboardButton `json:"inline_keyboard"`
}

// SendMessageOptions are the optional knobs on sendMessage. ReplyMarkup
// is a pointer so the JSON omits it cleanly when no buttons are wanted.
type SendMessageOptions struct {
	ReplyMarkup *InlineKeyboardMarkup
	ParseMode   string
}

// SendMessage posts a text message to the chat. Errors include
// 4xx/5xx responses with the raw body for diagnosis.
func (c *Client) SendMessage(ctx context.Context, chatID, text string, opts SendMessageOptions) error {
	body := map[string]any{
		"chat_id": chatID,
		"text":    text,
	}
	if opts.ReplyMarkup != nil {
		body["reply_markup"] = opts.ReplyMarkup
	}
	if opts.ParseMode != "" {
		body["parse_mode"] = opts.ParseMode
	}
	return c.call(ctx, "sendMessage", body, nil)
}

// AnswerCallbackQuery clears the spinner on the user's tap. Text is
// optional toast; empty string is fine.
func (c *Client) AnswerCallbackQuery(ctx context.Context, callbackQueryID, text string) error {
	body := map[string]any{"callback_query_id": callbackQueryID}
	if text != "" {
		body["text"] = text
	}
	return c.call(ctx, "answerCallbackQuery", body, nil)
}

// Update is the wire shape Telegram sends in getUpdates responses.
// Only message + callback_query are populated for our use; the long
// list of other fields (edited_message, channel_post, …) is ignored.
type Update struct {
	UpdateID      int64          `json:"update_id"`
	Message       *Message       `json:"message,omitempty"`
	CallbackQuery *CallbackQuery `json:"callback_query,omitempty"`
}

type Message struct {
	MessageID int64  `json:"message_id"`
	From      *User  `json:"from,omitempty"`
	Chat      Chat   `json:"chat"`
	Date      int64  `json:"date"`
	Text      string `json:"text,omitempty"`
}

type CallbackQuery struct {
	ID      string   `json:"id"`
	From    User     `json:"from"`
	Message *Message `json:"message,omitempty"`
	Data    string   `json:"data,omitempty"`
}

type User struct {
	ID        int64  `json:"id"`
	IsBot     bool   `json:"is_bot"`
	Username  string `json:"username,omitempty"`
	FirstName string `json:"first_name,omitempty"`
}

type Chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

// GetUpdates long-polls. Telegram blocks up to `timeout` seconds before
// returning an empty list if nothing arrives. Pass offset = (lastSeen
// + 1) to acknowledge prior updates; pass 0 on the very first call.
func (c *Client) GetUpdates(ctx context.Context, offset int64, timeout int) ([]Update, error) {
	q := url.Values{}
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	if timeout > 0 {
		q.Set("timeout", strconv.Itoa(timeout))
	}
	endpoint := fmt.Sprintf("https://api.telegram.org/bot%s/getUpdates?%s", c.token, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	// Per-call deadline = long-poll timeout + a generous network grace.
	// The client.Timeout above is a backstop for hangs that bypass ctx.
	cli := &http.Client{Timeout: time.Duration(timeout+15) * time.Second}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("telegram getUpdates status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var env struct {
		OK     bool     `json:"ok"`
		Result []Update `json:"result"`
		Desc   string   `json:"description,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, err
	}
	if !env.OK {
		return nil, fmt.Errorf("telegram getUpdates: %s", env.Desc)
	}
	return env.Result, nil
}

func (c *Client) call(ctx context.Context, method string, body any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("https://api.telegram.org/bot%s/%s", c.token, method)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram %s status=%d body=%s", method, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
