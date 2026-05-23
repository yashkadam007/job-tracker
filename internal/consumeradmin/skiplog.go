package consumeradmin

import (
	"encoding/base64"
	"encoding/json"
	"log"
)

// LogSkip emits the structured JSON line described in ADR 0006 Notes
// ("Structured log format") to the standard logger. payload_b64 keeps
// binary-safe in log aggregators; `base64 -d | jq` recovers it for
// manual replay.
func LogSkip(topic string, partition int32, offset int64, class string, err error, payload []byte) {
	rec := map[string]any{
		"level":       "error",
		"event":       "consumer_skip",
		"topic":       topic,
		"partition":   partition,
		"offset":      offset,
		"class":       class,
		"error":       err.Error(),
		"payload_b64": base64.StdEncoding.EncodeToString(payload),
	}
	body, mErr := json.Marshal(rec)
	if mErr != nil {
		log.Printf("skip-log marshal failed (class=%s offset=%d): %v", class, offset, mErr)
		return
	}
	log.Print(string(body))
}
