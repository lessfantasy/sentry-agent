package config

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Listen         string
	AllowedHosts   string
	Concurrency    int
	Timeout        time.Duration
	RetryInterval  time.Duration
	QueueLimit     int
	QueueBytes     int64
	Overflow       string
	MaxRequestSize int64
	LogLevel       string
}

func Parse(args []string, getenv func(string) string) (Config, error) {
	return ParseWithLookup(args, func(k string) (string, bool) { v := getenv(k); return v, v != "" })
}

func ParseWithLookup(args []string, lookup func(string) (string, bool)) (Config, error) {
	c := Config{Listen: "127.0.0.1:5148", AllowedHosts: "sentry.io,*.sentry.io", Concurrency: 10, Timeout: 5 * time.Second, RetryInterval: 5 * time.Second, QueueLimit: 1000, QueueBytes: 64 << 20, Overflow: "drop-oldest", MaxRequestSize: 20 << 20, LogLevel: "info"}
	fs := flag.NewFlagSet("sentry-agent", flag.ContinueOnError)
	str := func(name, env, def, help string) *string {
		v, ok := lookup(env)
		if !ok {
			v = def
		}
		return fs.String(name, v, help+" (env: "+env+")")
	}
	listen := str("listen", "SENTRY_AGENT_LISTEN", c.Listen, "HTTP listen address")
	hosts := str("allowed-hosts", "SENTRY_AGENT_ALLOWED_HOSTS", c.AllowedHosts, "Comma-separated permitted DSN hosts")
	concurrency := str("upstream-concurrency", "SENTRY_AGENT_UPSTREAM_CONCURRENCY", strconv.Itoa(c.Concurrency), "Delivery workers")
	timeout := str("upstream-timeout", "SENTRY_AGENT_UPSTREAM_TIMEOUT", c.Timeout.String(), "Upstream request timeout")
	retry := str("retry-interval", "SENTRY_AGENT_RETRY_INTERVAL", c.RetryInterval.String(), "Fixed retry interval")
	limit := str("queue-limit", "SENTRY_AGENT_QUEUE_LIMIT", strconv.Itoa(c.QueueLimit), "Maximum queued and in-flight envelopes")
	bytes := str("queue-bytes", "SENTRY_AGENT_QUEUE_BYTES", "64MB", "Maximum queued and in-flight payload bytes")
	overflow := str("overflow-policy", "SENTRY_AGENT_OVERFLOW_POLICY", c.Overflow, "drop-oldest or reject-new")
	maxSize := str("max-request-size", "SENTRY_AGENT_MAX_REQUEST_SIZE", "20MB", "Maximum incoming body size")
	level := str("log-level", "SENTRY_AGENT_LOG_LEVEL", c.LogLevel, "debug, info, warn, or error")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	c.Listen = *listen
	c.AllowedHosts = *hosts
	c.Overflow = *overflow
	c.LogLevel = *level
	var err error
	if c.Concurrency, err = strconv.Atoi(*concurrency); err != nil {
		return c, fmt.Errorf("upstream-concurrency: %w", err)
	}
	if c.Timeout, err = time.ParseDuration(*timeout); err != nil {
		return c, fmt.Errorf("upstream-timeout: %w", err)
	}
	if c.RetryInterval, err = time.ParseDuration(*retry); err != nil {
		return c, fmt.Errorf("retry-interval: %w", err)
	}
	if c.QueueLimit, err = strconv.Atoi(*limit); err != nil {
		return c, fmt.Errorf("queue-limit: %w", err)
	}
	if c.QueueBytes, err = ParseBytes(*bytes); err != nil {
		return c, fmt.Errorf("queue-bytes: %w", err)
	}
	if c.MaxRequestSize, err = ParseBytes(*maxSize); err != nil {
		return c, fmt.Errorf("max-request-size: %w", err)
	}
	if err = c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := int64(1)
	for _, unit := range []struct {
		s string
		m int64
	}{{"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}, {"B", 1}} {
		if strings.HasSuffix(s, unit.s) {
			mult = unit.m
			s = strings.TrimSpace(strings.TrimSuffix(s, unit.s))
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n > (1<<62)/mult {
		return 0, errors.New("expected a positive byte count (B, KB, MB, GB)")
	}
	return n * mult, nil
}

func (c Config) Validate() error {
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	if c.Concurrency < 1 || c.Concurrency > 1024 {
		return errors.New("upstream-concurrency must be 1..1024")
	}
	if c.Timeout <= 0 || c.RetryInterval <= 0 {
		return errors.New("upstream-timeout and retry-interval must be positive")
	}
	if c.QueueLimit < 1 || c.QueueBytes < 1 || c.MaxRequestSize < 1 {
		return errors.New("queue-limit, queue-bytes, and max-request-size must be positive")
	}
	if c.Overflow != "drop-oldest" && c.Overflow != "reject-new" {
		return errors.New("overflow-policy must be drop-oldest or reject-new")
	}
	if c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" && c.LogLevel != "error" {
		return errors.New("log-level must be debug, info, warn, or error")
	}
	if strings.TrimSpace(c.AllowedHosts) == "" {
		return errors.New("allowed-hosts must not be empty")
	}
	for _, h := range strings.Split(c.AllowedHosts, ",") {
		h = strings.TrimSpace(h)
		if h == "" || strings.ContainsAny(h, "/ :@") || strings.Count(h, "*") > 1 || strings.Contains(h, "*") && !strings.HasPrefix(h, "*.") {
			return errors.New("allowed-hosts contains invalid host pattern")
		}
	}
	return nil
}
