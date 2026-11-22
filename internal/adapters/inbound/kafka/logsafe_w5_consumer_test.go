package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

// TestOrderDemandConsumerLogsNoEventDerivedLineBreaks feeds the order demand
// consumer a payload that is not a CloudEvent and an OrderAllocated whose id,
// subject and order id carry CR/LF (and disagree, so it is skipped and
// logged): no log attribute may carry the line break.
func TestOrderDemandConsumerLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	c := &OrderDemandConsumer{Logger: slog.New(rec)}
	ctx := context.Background()
	_ = c.HandleMessage(ctx, []byte("not a CloudEvent"+crlfForged))
	b, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": "id" + crlfForged, "type": typeOrderAllocated, "source": "urn:logsafe",
		"subject": "subject" + crlfForged, "datacontenttype": "application/json",
		"data": map[string]any{"order_id": "order" + crlfForged},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.HandleMessage(ctx, b)
	rec.assertNoLineBreaks(t, 2)
}
