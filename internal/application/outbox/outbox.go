// Package outbox defines the already-encoded message the transactional
// outbox stores: the exact Kafka wire form of one integration event, minted
// ONCE when the use case runs so a relay retry republishes identical bytes
// (same CloudEvents id). It lives in the application layer because both
// ports (OutboxRepository, EventEncoder) and the relay adapter speak it;
// it carries no Kafka or Postgres types.
package outbox

// Header is one Kafka message header.
type Header struct {
	Key   string
	Value string
}

// Message is one outbox row's payload.
type Message struct {
	// EventID is the CloudEvents `id`, a UUID minted once at encode time
	// and persisted so redelivery never changes it.
	EventID string
	// Topic is the destination topic; the relay's writer is topic-less and
	// routes each message by this field.
	Topic string
	// EventType is the FULL CloudEvents `type`
	// (com.warehouse.wes.warehouse-planning.capacityplan.<EventName>).
	EventType string
	// Subject is the CloudEvents `subject` (the aggregate id).
	Subject string
	// Key is the Kafka message key (the aggregate id), so one plan's events
	// stay ordered on one partition.
	Key []byte
	// DataSchema is the CloudEvents `dataschema` URN.
	DataSchema string
	// Value is the structured-mode CloudEvents JSON, ready to write.
	Value []byte
	// Headers are the Kafka headers (always includes content-type).
	Headers []Header
}
