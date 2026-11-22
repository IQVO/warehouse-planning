package outbox_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	relay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

// crlfFailingStore fails every drain on a row whose event type, key and send
// error carry CR/LF, already at the attempt cap.
type crlfFailingStore struct{ err error }

func (s crlfFailingStore) Drain(context.Context, int, int, func(context.Context, outbox.Message) error) (int, error) {
	return 0, s.err
}

// TestRelayLogsNoEventDerivedLineBreaks: the attempt-cap and pass-failed logs
// of the relay must carry no line break from the outbox row or the send error.
func TestRelayLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	store := crlfFailingStore{err: &outbox.SendFailureError{
		RowID: 1, Attempts: 3, Err: errors.New("broker refused" + crlfForged),
		Message: outbox.Message{EventID: "id" + crlfForged, Topic: "logsafe.topic", EventType: "logsafe.Evil" + crlfForged, Key: []byte("key" + crlfForged)},
	}}
	r := relay.NewRelay(store, nil, slog.New(rec), relay.WithMaxAttempts(3), relay.WithInterval(time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_ = r.Run(ctx)
	rec.assertNoLineBreaks(t, 2)
}
