package kafka

import (
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

// syncWriterBatchTimeout is the relay writer's BatchTimeout. kafka-go's
// default is 1s: a synchronous WriteMessages waits up to a full second for
// a batch to fill, which capped a relay that sends one row per call at ~1
// event/s (seen live in the warehouse-day simulation). 10ms keeps writes
// batched under load while flushing a lone event almost immediately.
const syncWriterBatchTimeout = 10 * time.Millisecond

// syncWriterRequiredAcks makes every write wait for the broker's
// acknowledgement. kafka-go's default (RequireNone) returns nil without
// waiting, so the relay would mark rows published that the broker never
// stored -- silently at-most-once, the opposite of the outbox's guarantee.
const syncWriterRequiredAcks = segmentio.RequireAll
