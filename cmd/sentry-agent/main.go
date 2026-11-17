package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"sentry-agent/internal/agent"
	"sentry-agent/internal/config"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "probe" {
		probe(os.Args[2:])
		return
	}
	cfg, err := config.ParseWithLookup(os.Args[1:], os.LookupEnv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "configuration:", err)
		os.Exit(2)
	}
	var level slog.Level
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		log.Error("listen failed", "error", err)
		os.Exit(1)
	}
	a := agent.New(cfg, log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	a.Start(ctx)
	server := &http.Server{Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	log.Info("sentry-agent started", "listen", listener.Addr().String(), "workers", cfg.Concurrency, "queue_limit", cfg.QueueLimit, "queue_bytes", cfg.QueueBytes, "max_request_size", cfg.MaxRequestSize, "allowed_hosts", strings.Count(cfg.AllowedHosts, ",")+1)
	select {
	case <-ctx.Done():
		log.Info("shutdown requested")
	case err := <-served:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server stopped", "error", err)
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Warn("HTTP shutdown timed out", "error", err)
		_ = server.Close()
	}
	stop()
	a.Close()
	a.Wait()
	log.Info("sentry-agent stopped", "queued_lost", a.QueueDepth())
}

func probe(args []string) {
	if len(args) != 1 || (args[0] != "healthz" && args[0] != "readyz") {
		fmt.Fprintln(os.Stderr, "usage: sentry-agent probe healthz|readyz")
		os.Exit(2)
	}
	cfg, err := config.ParseWithLookup(nil, os.LookupEnv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration:", err)
		os.Exit(2)
	}
	_, port, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		os.Exit(1)
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + net.JoinHostPort("127.0.0.1", port) + "/" + args[0])
	if err != nil {
		os.Exit(1)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
