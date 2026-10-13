package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	outboxrelay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
)

// TestOutboxMaxAttempts pins OUTBOX_MAX_ATTEMPTS (gap fix D12): unset means
// the fleet default of 20, 0 means unlimited, and an invalid or negative
// value falls back to the default with a warning instead of silently
// disabling the cap.
func TestOutboxMaxAttempts(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
		warn bool
	}{
		{name: "unset uses the default", raw: "", want: 20},
		{name: "explicit value", raw: "5", want: 5},
		{name: "zero means unlimited", raw: "0", want: 0},
		{name: "negative falls back", raw: "-1", want: 20, warn: true},
		{name: "garbage falls back", raw: "many", want: 20, warn: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			if got := outboxMaxAttempts(tt.raw, logger); got != tt.want {
				t.Fatalf("outboxMaxAttempts(%q) = %d, want %d", tt.raw, got, tt.want)
			}
			if warned := strings.Contains(buf.String(), "level=WARN"); warned != tt.warn {
				t.Fatalf("warned = %v, want %v (log: %s)", warned, tt.warn, buf.String())
			}
		})
	}
}

// TestDefaultOutboxMaxAttemptsIsTwenty keeps the fleet default in one place:
// the relay package constant cmd/api falls back to.
func TestDefaultOutboxMaxAttemptsIsTwenty(t *testing.T) {
	if outboxrelay.DefaultMaxAttempts != 20 {
		t.Fatalf("DefaultMaxAttempts = %d, want 20 (fleet convention)", outboxrelay.DefaultMaxAttempts)
	}
}
