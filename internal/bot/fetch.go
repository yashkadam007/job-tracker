package bot

import (
	"context"
	"html"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// fetchTitleCompany pulls a job posting and returns its best-effort
// title and company name. Either or both may be empty, in which case
// the caller asks the user. This is intentionally lightweight: a
// 5-second GET, three regexes, and string normalisation — no full
// HTML parser, no JS execution. ADR 0003 calls out URL parsing as
// best-effort: when in doubt, prompt.
func fetchTitleCompany(ctx context.Context, rawURL string) (title, company string) {
	body, err := fetchPage(ctx, rawURL)
	if err != nil {
		log.Printf("bot: fetch %s: %v", rawURL, err)
		return "", ""
	}
	title = firstMatch(body, ogTitleRE)
	company = firstMatch(body, ogSiteRE)
	if title == "" {
		// Fall back to <title>, with boilerplate trimmed. Many job
		// boards format the tag as "Title - Company | Board"; pull
		// company out of the same string when we don't have og:site.
		raw := firstMatch(body, titleRE)
		title, company = splitTitleBoilerplate(raw, company)
	}
	return strings.TrimSpace(html.UnescapeString(title)),
		strings.TrimSpace(html.UnescapeString(company))
}

// Two regex variants per tag to handle attribute order ("property"
// first vs "content" first). Case-insensitive (?i) — some sites
// capitalise META.
var (
	ogTitleRE = regexp.MustCompile(`(?is)<meta[^>]*(?:property|name)\s*=\s*["']og:title["'][^>]*content\s*=\s*["']([^"']*)["']` +
		`|<meta[^>]*content\s*=\s*["']([^"']*)["'][^>]*(?:property|name)\s*=\s*["']og:title["']`)
	ogSiteRE = regexp.MustCompile(`(?is)<meta[^>]*(?:property|name)\s*=\s*["']og:site_name["'][^>]*content\s*=\s*["']([^"']*)["']` +
		`|<meta[^>]*content\s*=\s*["']([^"']*)["'][^>]*(?:property|name)\s*=\s*["']og:site_name["']`)
	titleRE = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
)

func firstMatch(body string, re *regexp.Regexp) string {
	m := re.FindStringSubmatch(body)
	if m == nil {
		return ""
	}
	// Either capture group 1 or 2 holds the value depending on which
	// attribute-order branch matched. The other is empty.
	for _, g := range m[1:] {
		if g != "" {
			return g
		}
	}
	return ""
}

// splitTitleBoilerplate teases a job title and a company name out of
// the common `<title>Senior Engineer - Acme | Wellfound</title>`
// pattern. If we already have a company from og:site_name we leave it
// alone; otherwise we take everything after the first separator and
// drop the rightmost site-name segment.
func splitTitleBoilerplate(raw, existingCompany string) (title, company string) {
	if raw == "" {
		return "", existingCompany
	}
	// Split on common separators: ` - `, ` | `, ` – ` (en dash),
	// ` — ` (em dash), ` @ `. First piece is the title.
	segs := splitOnAny(raw, []string{" - ", " | ", " – ", " — ", " @ "})
	if len(segs) == 1 {
		return strings.TrimSpace(raw), existingCompany
	}
	title = strings.TrimSpace(segs[0])
	if existingCompany != "" {
		return title, existingCompany
	}
	// Heuristic: if there are >= 3 segments the last one is usually
	// the site name ("… | LinkedIn"); take the second.
	if len(segs) >= 3 {
		return title, strings.TrimSpace(segs[1])
	}
	return title, strings.TrimSpace(segs[1])
}

func splitOnAny(s string, seps []string) []string {
	// Lazy implementation: find the *earliest* separator and split on
	// it, recurse. Recursion depth bounded by len(s)/min(sep).
	for _, sep := range seps {
		if strings.Contains(s, sep) {
			parts := strings.Split(s, sep)
			var out []string
			for _, p := range parts {
				out = append(out, splitOnAny(p, seps)...)
			}
			return out
		}
	}
	return []string{s}
}

func fetchPage(ctx context.Context, rawURL string) (string, error) {
	cli := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	// A vanilla Go UA gets blocked by some job boards. Identify
	// honestly but mimic a real browser enough to pass cheap filters.
	req.Header.Set("User-Agent", "job-tracker-bot/0.1 (+https://github.com/) Mozilla/5.0")
	resp, err := cli.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	// Cap the read so a 50MB page can't OOM the bot. 1MB is plenty
	// for finding <meta> / <title> in the document head.
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	return string(b), nil
}
