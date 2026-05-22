package telegram

import (
	"fmt"
	"strings"
)

// Callback data is capped at 64 bytes by Telegram. Encoding is
// `<action>:<arg1>[:<arg2>]` with single-letter actions to leave room
// for UUIDs. Decoded with ParseCallback below.
const (
	CallbackActionStatus = "st" // st:<status>:<job_id>
	CallbackActionSnooze = "sn" // sn:<job_id>
)

// EncodeStatusCallback builds "st:<status>:<job_id>".
func EncodeStatusCallback(status, jobID string) string {
	return fmt.Sprintf("%s:%s:%s", CallbackActionStatus, status, jobID)
}

// EncodeSnoozeCallback builds "sn:<job_id>".
func EncodeSnoozeCallback(jobID string) string {
	return fmt.Sprintf("%s:%s", CallbackActionSnooze, jobID)
}

// Callback is the parsed shape of inline-keyboard callback data.
type Callback struct {
	Action string
	Args   []string
}

func ParseCallback(data string) (Callback, error) {
	parts := strings.Split(data, ":")
	if len(parts) < 2 {
		return Callback{}, fmt.Errorf("telegram: malformed callback data %q", data)
	}
	return Callback{Action: parts[0], Args: parts[1:]}, nil
}

// ReminderKeyboard is the three-button row attached to reminder
// messages. Shared between the notifier (which sends the message) and
// the bot (which receives the callback) so the wire format stays in
// lockstep — see ADR 0003.
func ReminderKeyboard(jobID string) *InlineKeyboardMarkup {
	return &InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{{
			{Text: "✅ Applied", CallbackData: EncodeStatusCallback("applied", jobID)},
			{Text: "❌ Rejected", CallbackData: EncodeStatusCallback("rejected", jobID)},
			{Text: "💤 Snooze 1d", CallbackData: EncodeSnoozeCallback(jobID)},
		}},
	}
}
