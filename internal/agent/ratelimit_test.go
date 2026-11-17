package agent

import (
	"net/http"
	"testing"
	"time"
)

func TestRateLimitUntil(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		headers http.Header
		want    time.Duration
	}{{"sentry multiple", http.Header{"X-Sentry-Rate-Limits": []string{"2:error:project:, 5::organization:"}}, 5 * time.Second}, {"retry seconds", http.Header{"Retry-After": []string{"7"}}, 7 * time.Second}, {"retry date", http.Header{"Retry-After": []string{"Thu, 01 Jan 2026 00:00:09 GMT"}}, 9 * time.Second}, {"longer wins", http.Header{"Retry-After": []string{"8"}, "X-Sentry-Rate-Limits": []string{"2:error:project:"}}, 8 * time.Second}, {"malformed", http.Header{"Retry-After": []string{"bogus"}, "X-Sentry-Rate-Limits": []string{"bogus:error:"}}, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			got := rateLimitUntil(tc.headers, now)
			if tc.want == 0 {
				if !got.IsZero() {
					t.Fatalf("got %v", got)
				}
				return
			}
			if got.Sub(now) != tc.want {
				t.Fatalf("got %v, want %v", got.Sub(now), tc.want)
			}
		})
	}
}
