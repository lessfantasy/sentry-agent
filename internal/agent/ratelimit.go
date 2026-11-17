package agent

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

func rateLimitUntil(h http.Header, now time.Time) time.Time {
	var until time.Time
	for _, raw := range h.Values("X-Sentry-Rate-Limits") {
		for _, part := range strings.Split(raw, ",") {
			fields := strings.Split(strings.TrimSpace(part), ":")
			if len(fields) < 2 {
				continue
			}
			seconds, err := strconv.ParseFloat(strings.TrimSpace(fields[0]), 64)
			if err != nil || seconds <= 0 || seconds > 86400*365 {
				continue
			}
			candidate := now.Add(time.Duration(seconds * float64(time.Second)))
			if candidate.After(until) {
				until = candidate
			}
		}
	}
	for _, raw := range h.Values("Retry-After") {
		raw = strings.TrimSpace(raw)
		var candidate time.Time
		if seconds, err := strconv.ParseFloat(raw, 64); err == nil && seconds > 0 && seconds <= 86400*365 {
			candidate = now.Add(time.Duration(seconds * float64(time.Second)))
		} else if date, err := http.ParseTime(raw); err == nil {
			candidate = date
		}
		if candidate.After(until) {
			until = candidate
		}
	}
	return until
}
