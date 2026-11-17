package config

import (
	"strings"
	"testing"
)

func TestPrecedence(t *testing.T) {
	env := func(k string) string {
		if k == "SENTRY_AGENT_QUEUE_LIMIT" {
			return "3"
		}
		return ""
	}
	c, err := Parse(nil, env)
	if err != nil || c.QueueLimit != 3 {
		t.Fatalf("env: %+v %v", c, err)
	}
	c, err = Parse([]string{"--queue-limit=7"}, env)
	if err != nil || c.QueueLimit != 7 {
		t.Fatalf("flag: %+v %v", c, err)
	}
}
func TestInvalid(t *testing.T) {
	for _, args := range [][]string{{"--retry-interval=0s"}, {"--queue-limit=x"}, {"--overflow-policy=ignore"}, {"--max-request-size=0"}, {"--allowed-hosts=*"}} {
		if _, err := Parse(args, func(string) string { return "" }); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
}
func TestExplicitEmptyEnvironmentRejected(t *testing.T) {
	_, err := ParseWithLookup(nil, func(k string) (string, bool) {
		if k == "SENTRY_AGENT_LISTEN" {
			return "", true
		}
		return "", false
	})
	if err == nil {
		t.Fatal("empty explicit listen was accepted")
	}
}
func TestBytes(t *testing.T) {
	for _, tc := range []struct {
		s    string
		want int64
	}{{"512KB", 512 << 10}, {"20MB", 20 << 20}, {"1GB", 1 << 30}, {"42", 42}} {
		got, err := ParseBytes(tc.s)
		if err != nil || got != tc.want {
			t.Fatalf("%s: %d %v", tc.s, got, err)
		}
	}
	if _, err := ParseBytes(strings.Repeat("9", 100)); err == nil {
		t.Fatal("accepted overflow")
	}
}
