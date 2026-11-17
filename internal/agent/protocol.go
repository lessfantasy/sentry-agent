package agent

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"regexp"
	"strings"
)

var projectPath = regexp.MustCompile(`^/api/([1-9][0-9]*)/envelope/$`)
var keyPattern = regexp.MustCompile(`^[A-Za-z0-9]+$`)

type destination struct{ endpoint, key string }

func parseDestination(body []byte, encoding, requestPath, auth, allowedHosts string) (destination, error) {
	var out destination
	match := projectPath.FindStringSubmatch(requestPath)
	if match == nil {
		return out, errors.New("expected /api/{project-id}/envelope/")
	}
	var reader io.Reader = bytes.NewReader(body)
	if encoding != "" && encoding != "gzip" {
		return out, errors.New("unsupported content encoding")
	}
	if encoding == "gzip" {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return out, errors.New("invalid gzip body")
		}
		defer gz.Close()
		reader = gz
	}
	line, err := bufio.NewReader(io.LimitReader(reader, 16*1024+1)).ReadBytes('\n')
	if err != nil || len(line) > 16*1024 {
		return out, errors.New("missing or oversized envelope header")
	}
	var header struct {
		DSN string `json:"dsn"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(line), &header); err != nil || header.DSN == "" {
		return out, errors.New("envelope header requires a DSN")
	}
	u, err := url.Parse(header.DSN)
	if err != nil || u.User == nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return out, errors.New("malformed DSN")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return out, errors.New("DSN must use HTTP or HTTPS")
	}
	key := u.User.Username()
	if !keyPattern.MatchString(key) {
		return out, errors.New("invalid DSN public key")
	}
	if password, ok := u.User.Password(); ok && password != "" {
		return out, errors.New("DSN secret is unsupported")
	}
	host := strings.ToLower(u.Hostname())
	if u.Scheme == "http" && (host == "sentry.io" || strings.HasSuffix(host, ".sentry.io")) {
		return out, errors.New("Sentry SaaS requires HTTPS")
	}
	if !hostAllowed(host, allowedHosts) {
		return out, errors.New("DSN host is not allowed")
	}
	if strings.Contains(u.Host, "@") || strings.ContainsAny(u.Host, "\\#?") {
		return out, errors.New("invalid DSN host")
	}
	segments := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
	if len(segments) == 0 || segments[len(segments)-1] != match[1] {
		return out, errors.New("DSN project does not match request path")
	}
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." || strings.Contains(strings.ToLower(segment), "%2f") {
			return out, errors.New("invalid DSN path")
		}
	}
	for i, segment := range segments {
		decoded, err := url.PathUnescape(segment)
		if err != nil || decoded == "." || decoded == ".." || strings.Contains(decoded, "/") {
			return out, errors.New("invalid DSN path")
		}
		segments[i] = decoded
	}
	if auth != "" {
		got, err := authKey(auth)
		if err != nil || got != key {
			return out, errors.New("X-Sentry-Auth key does not match DSN")
		}
	}
	base := strings.Join(segments[:len(segments)-1], "/")
	u.User = nil
	u.RawPath = ""
	u.Path = path.Join("/", base, "api", match[1], "envelope") + "/"
	out.endpoint = u.String()
	out.key = key
	return out, nil
}

func hostAllowed(host, patterns string) bool {
	for _, entry := range strings.Split(patterns, ",") {
		entry = strings.ToLower(strings.TrimSpace(entry))
		if entry == host || strings.HasPrefix(entry, "*.") && strings.HasSuffix(host, entry[1:]) && host != entry[2:] {
			return true
		}
	}
	return false
}
func authKey(s string) (string, error) {
	if !strings.HasPrefix(s, "Sentry ") {
		return "", errors.New("invalid auth scheme")
	}
	for _, field := range strings.Split(strings.TrimPrefix(s, "Sentry "), ",") {
		parts := strings.SplitN(strings.TrimSpace(field), "=", 2)
		if len(parts) == 2 && parts[0] == "sentry_key" && keyPattern.MatchString(parts[1]) {
			return parts[1], nil
		}
	}
	return "", fmt.Errorf("missing sentry_key")
}
