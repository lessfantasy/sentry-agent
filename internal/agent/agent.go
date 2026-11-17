package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"sentry-agent/internal/config"
	"sentry-agent/internal/queue"
)

// Queue is the delivery lifecycle needed by ingress and workers. A future
// backend can implement it without changing either HTTP or sender logic.
type Queue interface {
	Enqueue(*queue.Item) (int, error)
	Acquire(context.Context) (*queue.Item, error)
	Complete(*queue.Item, time.Time)
	DelayEndpoint(string, time.Time)
	Depth() int
	Close()
}

type Agent struct {
	cfg    config.Config
	q      Queue
	client *http.Client
	log    *slog.Logger
	wg     sync.WaitGroup
}

func New(cfg config.Config, log *slog.Logger) *Agent {
	return NewWithQueue(cfg, log, queue.New(cfg.QueueLimit, cfg.QueueBytes, cfg.Overflow))
}

func NewWithQueue(cfg config.Config, log *slog.Logger, q Queue) *Agent {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = http.ProxyFromEnvironment
	transport.MaxIdleConns = cfg.Concurrency * 2
	transport.MaxIdleConnsPerHost = cfg.Concurrency
	transport.MaxConnsPerHost = cfg.Concurrency
	transport.IdleConnTimeout = 90 * time.Second
	client := &http.Client{Transport: transport, Timeout: cfg.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Agent{cfg: cfg, q: q, client: client, log: log}
}

func (a *Agent) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", a.ingest)
	return mux
}

func (a *Agent) ingest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if !projectPath.MatchString(r.URL.Path) {
		http.Error(w, "invalid envelope path", http.StatusBadRequest)
		return
	}
	if r.ContentLength > a.cfg.MaxRequestSize {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.EqualFold(strings.TrimSpace(strings.Split(ct, ";")[0]), "application/x-sentry-envelope") {
		http.Error(w, "invalid content type", http.StatusUnsupportedMediaType)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, a.cfg.MaxRequestSize+1))
	if err != nil {
		http.Error(w, "could not read body", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > a.cfg.MaxRequestSize {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	dest, err := parseDestination(body, r.Header.Get("Content-Encoding"), r.URL.Path, r.Header.Get("X-Sentry-Auth"), a.cfg.AllowedHosts)
	if err != nil {
		a.log.Warn("envelope rejected", "reason", err.Error())
		http.Error(w, "invalid envelope destination", http.StatusBadRequest)
		return
	}
	item := &queue.Item{Endpoint: dest.endpoint, Body: body, ContentEncoding: r.Header.Get("Content-Encoding"), Auth: "Sentry sentry_version=7, sentry_key=" + dest.key, UserAgent: safeUserAgent(r.Header.Get("User-Agent")), Due: time.Now()}
	dropped, err := a.q.Enqueue(item)
	if dropped > 0 {
		a.log.Warn("queue overflow dropped oldest", "count", dropped)
	}
	if err != nil {
		a.log.Warn("queue admission rejected", "reason", err.Error())
		http.Error(w, "queue full", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func safeUserAgent(v string) string {
	if len(v) > 256 {
		return "sentry-agent/1"
	}
	for _, r := range v {
		if r < 32 || r > 126 {
			return "sentry-agent/1"
		}
	}
	if v == "" {
		return "sentry-agent/1"
	}
	return v
}

func (a *Agent) Start(ctx context.Context) {
	for i := 0; i < a.cfg.Concurrency; i++ {
		a.wg.Add(1)
		go a.worker(ctx)
	}
}
func (a *Agent) Wait()           { a.wg.Wait(); a.client.CloseIdleConnections() }
func (a *Agent) Close()          { a.q.Close() }
func (a *Agent) QueueDepth() int { return a.q.Depth() }

func (a *Agent) worker(ctx context.Context) {
	defer a.wg.Done()
	for {
		item, err := a.q.Acquire(ctx)
		if err != nil {
			return
		}
		retry, limit, status, err := a.send(ctx, item)
		if err != nil {
			a.log.Warn("upstream delivery failed", "error", err.Error())
		} else if retry.IsZero() {
			if status >= 400 {
				a.log.Warn("permanent upstream rejection", "status", status)
			} else {
				a.log.Debug("upstream delivery succeeded", "status", status)
			}
		} else {
			a.log.Warn("upstream delivery scheduled for retry", "status", status)
		}
		if !limit.IsZero() {
			a.q.DelayEndpoint(item.Endpoint, limit)
		}
		a.q.Complete(item, retry)
	}
}

func (a *Agent) send(ctx context.Context, it *queue.Item) (retry, limit time.Time, status int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, it.Endpoint, bytes.NewReader(it.Body))
	if err != nil {
		return time.Time{}, time.Time{}, 0, err
	}
	req.Header.Set("Content-Type", "application/x-sentry-envelope")
	req.Header.Set("X-Sentry-Auth", it.Auth)
	req.Header.Set("User-Agent", it.UserAgent)
	if it.ContentEncoding != "" {
		req.Header.Set("Content-Encoding", it.ContentEncoding)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return time.Time{}, time.Time{}, 0, ctx.Err()
		}
		return time.Now().Add(a.cfg.RetryInterval), time.Time{}, 0, errors.New("transport error")
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	now := time.Now()
	status = resp.StatusCode
	limit = rateLimitUntil(resp.Header, now)
	if status == http.StatusTooManyRequests && limit.IsZero() {
		limit = now.Add(60 * time.Second)
	}
	if status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500 {
		retry = now.Add(a.cfg.RetryInterval)
		if limit.After(retry) {
			retry = limit
		}
		return retry, limit, status, nil
	}
	if status >= 300 {
		if status < 400 {
			return time.Time{}, limit, status, fmt.Errorf("upstream redirect rejected: %s", strconv.Itoa(status))
		}
		return time.Time{}, limit, status, nil
	}
	return time.Time{}, limit, status, nil
}
