package agent

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sentry-agent/internal/config"
)

func testConfig() config.Config {
	c, _ := config.Parse(nil, func(string) string { return "" })
	c.AllowedHosts = "127.0.0.1"
	c.RetryInterval = 30 * time.Millisecond
	c.Timeout = 200 * time.Millisecond
	return c
}
func startAgent(t *testing.T, c config.Config) (*Agent, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	a := New(c, slog.New(slog.NewTextHandler(io.Discard, nil)))
	a.Start(ctx)
	t.Cleanup(func() { cancel(); a.Close(); a.Wait() })
	return a, cancel
}
func envelope(upstream string, project string) []byte {
	u, _ := url.Parse(upstream)
	return []byte(fmt.Sprintf("{\"dsn\":\"%s://public@%s/%s\"}\n{\"type\":\"event\"}\n{}\n", u.Scheme, u.Host, project))
}
func post(t *testing.T, h http.Handler, upstream, project string, body []byte, encoding string) int {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/api/"+project+"/envelope/", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/x-sentry-envelope")
	if encoding != "" {
		r.Header.Set("Content-Encoding", encoding)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code
}
func received(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("delivery timeout")
		return ""
	}
}

func TestAcceptAsyncMultipleDestinationsAndOpaqueBody(t *testing.T) {
	ch := make(chan string, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if r.Header.Get("X-Sentry-Auth") != "Sentry sentry_version=7, sentry_key=public" {
			t.Error("missing auth")
		}
		ch <- r.URL.Path + "|" + string(b)
		w.WriteHeader(200)
	}))
	defer upstream.Close()
	c := testConfig()
	a, _ := startAgent(t, c)
	h := a.Handler()
	for _, project := range []string{"1", "2"} {
		body := envelope(upstream.URL, project)
		if got := post(t, h, upstream.URL, project, body, ""); got != 200 {
			t.Fatalf("status %d", got)
		}
	}
	got1, got2 := received(t, ch), received(t, ch)
	if !strings.Contains(got1, "/api/1/envelope/") && !strings.Contains(got2, "/api/1/envelope/") {
		t.Fatal("project 1 missing")
	}
	if !strings.Contains(got1, "/api/2/envelope/") && !strings.Contains(got2, "/api/2/envelope/") {
		t.Fatal("project 2 missing")
	}
}

func TestRetryAndNoHeadOfLineBlocking(t *testing.T) {
	var attempts atomic.Int32
	ch := make(chan string, 4)
	aServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		ch <- fmt.Sprintf("A%d", n)
		if n == 1 {
			w.WriteHeader(503)
		}
	}))
	defer aServer.Close()
	bServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ch <- "B" }))
	defer bServer.Close()
	c := testConfig()
	c.Concurrency = 1
	c.RetryInterval = 150 * time.Millisecond
	a, _ := startAgent(t, c)
	if got := post(t, a.Handler(), aServer.URL, "1", envelope(aServer.URL, "1"), ""); got != 200 {
		t.Fatal(got)
	}
	if got := received(t, ch); got != "A1" {
		t.Fatal(got)
	}
	if got := post(t, a.Handler(), bServer.URL, "2", envelope(bServer.URL, "2"), ""); got != 200 {
		t.Fatal(got)
	}
	if got := received(t, ch); got != "B" {
		t.Fatalf("head of line blocked: %s", got)
	}
	if got := received(t, ch); got != "A2" {
		t.Fatal(got)
	}
}

func TestRateLimitsAndRetryAfter(t *testing.T) {
	for _, tc := range []struct{ name, header, value string }{{"sentry", "X-Sentry-Rate-Limits", "0.15:error:project:"}, {"retry-after", "Retry-After", "0.15"}} {
		t.Run(tc.name, func(t *testing.T) {
			ch := make(chan time.Time, 3)
			var count atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ch <- time.Now()
				if count.Add(1) == 1 {
					w.Header().Set(tc.header, tc.value)
					w.WriteHeader(429)
				}
			}))
			defer up.Close()
			c := testConfig()
			c.RetryInterval = 20 * time.Millisecond
			a, _ := startAgent(t, c)
			if got := post(t, a.Handler(), up.URL, "1", envelope(up.URL, "1"), ""); got != 200 {
				t.Fatal(got)
			}
			first := <-ch
			time.Sleep(10 * time.Millisecond)
			if got := post(t, a.Handler(), up.URL, "1", envelope(up.URL, "1"), ""); got != 200 {
				t.Fatal(got)
			}
			second := receivedTime(t, ch)
			if second.Sub(first) < 120*time.Millisecond {
				t.Fatalf("rate limit ignored: %v", second.Sub(first))
			}
			receivedTime(t, ch)
		})
	}
}
func receivedTime(t *testing.T, ch <-chan time.Time) time.Time {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
		return time.Time{}
	}
}

func TestPermanent4xxAndMaxSizeAndMalformed(t *testing.T) {
	var count atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1); w.WriteHeader(400) }))
	defer up.Close()
	c := testConfig()
	c.MaxRequestSize = 200
	a, _ := startAgent(t, c)
	body := envelope(up.URL, "1")
	if got := post(t, a.Handler(), up.URL, "1", body, ""); got != 200 {
		t.Fatal(got)
	}
	deadline := time.Now().Add(time.Second)
	for a.QueueDepth() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if a.QueueDepth() != 0 || count.Load() != 1 {
		t.Fatalf("queue %d, attempts %d", a.QueueDepth(), count.Load())
	}
	if got := post(t, a.Handler(), up.URL, "1", bytes.Repeat([]byte("x"), 201), ""); got != 413 {
		t.Fatal(got)
	}
	if got := post(t, a.Handler(), up.URL, "2", body, ""); got != 400 {
		t.Fatal(got)
	}
	bad := []byte("{\"dsn\":\"http://public@127.0.0.1:1/1\"}\n")
	if got := post(t, a.Handler(), up.URL, "1", bad, ""); got != 200 {
		t.Fatal(got)
	}
}

func TestGzipPreservedAndAuthValidation(t *testing.T) {
	ch := make(chan []byte, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") != "gzip" {
			t.Error("encoding lost")
		}
		b, _ := io.ReadAll(r.Body)
		ch <- b
	}))
	defer up.Close()
	c := testConfig()
	a, _ := startAgent(t, c)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(envelope(up.URL, "1"))
	_ = zw.Close()
	if got := post(t, a.Handler(), up.URL, "1", buf.Bytes(), "gzip"); got != 200 {
		t.Fatal(got)
	}
	if !bytes.Equal(receivedBytes(t, ch), buf.Bytes()) {
		t.Fatal("body changed")
	}
	r := httptest.NewRequest("POST", "/api/1/envelope/", bytes.NewReader(envelope(up.URL, "1")))
	r.Header.Set("X-Sentry-Auth", "Sentry sentry_key=wrong")
	w := httptest.NewRecorder()
	a.Handler().ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}

func TestAdmissionIsAsyncAndRejectNew(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { entered <- struct{}{}; <-release }))
	defer up.Close()
	defer close(release)
	c := testConfig()
	c.QueueLimit = 1
	c.Overflow = "reject-new"
	a, _ := startAgent(t, c)
	start := time.Now()
	if got := post(t, a.Handler(), up.URL, "1", envelope(up.URL, "1"), ""); got != 200 {
		t.Fatal(got)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("ingress waited for upstream")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	if got := post(t, a.Handler(), up.URL, "1", envelope(up.URL, "1"), ""); got != 503 {
		t.Fatalf("queue full returned %d", got)
	}
}

func TestWorkersStopOnCancel(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer up.Close()
	c := testConfig()
	c.Timeout = time.Second
	a, cancel := startAgent(t, c)
	if got := post(t, a.Handler(), up.URL, "1", envelope(up.URL, "1"), ""); got != 200 {
		t.Fatal(got)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	cancel()
	done := make(chan struct{})
	go func() { a.Close(); a.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not stop")
	}
	close(release)
}

func TestMalformedDestinations(t *testing.T) {
	for _, tc := range []struct{ name, dsn, path, hosts string }{{"disallowed", "http://public@evil.test/1", "/api/1/envelope/", "127.0.0.1"}, {"project", "http://public@127.0.0.1/2", "/api/1/envelope/", "127.0.0.1"}, {"scheme", "file://public@127.0.0.1/1", "/api/1/envelope/", "127.0.0.1"}, {"secret", "http://public:secret@127.0.0.1/1", "/api/1/envelope/", "127.0.0.1"}, {"saas-http", "http://public@sentry.io/1", "/api/1/envelope/", "sentry.io"}} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(fmt.Sprintf("{\"dsn\":%q}\n{\"type\":\"event\"}\n{}\n", tc.dsn))
			if _, err := parseDestination(body, "", tc.path, "", tc.hosts); err == nil {
				t.Fatal("accepted invalid DSN")
			}
		})
	}
}

func TestTimeoutAnd408Retry(t *testing.T) {
	for _, mode := range []string{"timeout", "408"} {
		t.Run(mode, func(t *testing.T) {
			var attempts atomic.Int32
			ch := make(chan time.Time, 3)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := attempts.Add(1)
				ch <- time.Now()
				if n == 1 {
					if mode == "408" {
						w.WriteHeader(http.StatusRequestTimeout)
					} else {
						time.Sleep(80 * time.Millisecond)
					}
				}
			}))
			defer up.Close()
			c := testConfig()
			c.Timeout = 30 * time.Millisecond
			c.RetryInterval = 40 * time.Millisecond
			a, _ := startAgent(t, c)
			if got := post(t, a.Handler(), up.URL, "1", envelope(up.URL, "1"), ""); got != 200 {
				t.Fatal(got)
			}
			first := receivedTime(t, ch)
			second := receivedTime(t, ch)
			if second.Sub(first) < 35*time.Millisecond {
				t.Fatalf("retried too soon: %v", second.Sub(first))
			}
		})
	}
}

func TestConcurrentWorkers(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { entered <- struct{}{}; <-release }))
	defer up.Close()
	c := testConfig()
	c.Concurrency = 2
	a, _ := startAgent(t, c)
	for i := 0; i < 2; i++ {
		if got := post(t, a.Handler(), up.URL, "1", envelope(up.URL, "1"), ""); got != 200 {
			t.Fatal(got)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("second worker did not start")
		}
	}
	close(release)
}
func receivedBytes(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(time.Second):
		t.Fatal("timeout")
		return nil
	}
}
