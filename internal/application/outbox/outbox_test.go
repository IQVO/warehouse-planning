package outbox_test

import (
	"errors"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

// TestSendFailure_ErrorAndUnwrap pins the SendFailure wording the runbook
// greps for ("outbox row <id> (<type>): <cause>") and that the send error
// stays reachable through errors.Is (gap fix D12, ADR 0013).
func TestSendFailure_ErrorAndUnwrap(t *testing.T) {
	cause := errors.New("message too large")
	f := &outbox.SendFailure{RowID: 7, Message: outbox.Message{EventType: "com.example.Poison"}, Attempts: 3, Err: cause}
	if got, want := f.Error(), "outbox row 7 (com.example.Poison): message too large"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(f, cause) || !errors.Is(error(f), cause) {
		t.Fatal("errors.Is must reach the send error through Unwrap")
	}
}
