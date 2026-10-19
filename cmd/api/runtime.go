package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	outboxrelay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// Environment variables the context cannot run without (ADR 0016). Each boot
// error below names its variable.
const (
	envDatabaseURL         = "DATABASE_URL"
	envKafkaBrokers        = "KAFKA_BROKERS"
	envEventPublisher      = "EVENT_PUBLISHER"
	envLaborGroup          = "LABOR_CAPACITY_CONSUMER_GROUP"
	envStorageGroup        = "STORAGE_CAPACITY_CONSUMER_GROUP"
	envDemandConsumerGrp   = "DEMAND_CONSUMER_GROUP"
	envDemandSiteID        = "DEMAND_SITE_ID"
	envOutboxRelayInterval = "OUTBOX_RELAY_INTERVAL"
	envOutboxMaxAttempts   = "OUTBOX_MAX_ATTEMPTS"
)

var (
	errMissingDatabaseURL = errors.New("DATABASE_URL is required: this context runs on real Postgres, there is no in-memory mode")
	errMissingBrokers     = errors.New("KAFKA_BROKERS is required: this context runs on real Kafka (outbox relay, consumers, /readyz)")
)

// runtimeEnv is the validated part of the environment that selects the real
// runtime: the brokers, the consumer groups and the demand site.
type runtimeEnv struct {
	brokers []string
	// groups maps a consumer's group variable (LABOR_CAPACITY_CONSUMER_GROUP,
	// ...) to its value; every consumer always runs, so every entry is
	// present.
	groups map[string]string
	// demandSiteID is the ONE site every consumed order is attributed to
	// (DEMAND_SITE_ID): order-management's events carry no fulfillment site.
	demandSiteID string
}

// consumerGroupEnvs are the three Kafka consumers of cmd/api, each with the
// env var that carries its STABLE group id (never a literal, never a default:
// a default would let a locally run process join the live group).
var consumerGroupEnvs = []string{envLaborGroup, envStorageGroup, envDemandConsumerGrp}

// loadRuntime validates the environment before anything is opened or bound:
// DATABASE_URL, KAFKA_BROKERS, every consumer group and DEMAND_SITE_ID are
// mandatory, and EVENT_PUBLISHER may only be `kafka`. Every error names the
// variable (ADR 0016).
func loadRuntime(env func(string) string) (runtimeEnv, error) {
	if env(envDatabaseURL) == "" {
		return runtimeEnv{}, errMissingDatabaseURL
	}
	rt := runtimeEnv{brokers: splitBrokers(env(envKafkaBrokers)), groups: map[string]string{}}
	if len(rt.brokers) == 0 {
		return runtimeEnv{}, errMissingBrokers
	}
	if err := parsePublisherMode(env(envEventPublisher)); err != nil {
		return runtimeEnv{}, err
	}
	for _, name := range consumerGroupEnvs {
		group := env(name)
		if group == "" {
			return runtimeEnv{}, fmt.Errorf("%s is required: the consumer always runs on real Kafka under a stable group id", name)
		}
		rt.groups[name] = group
	}
	rt.demandSiteID = env(envDemandSiteID)
	if rt.demandSiteID == "" {
		return runtimeEnv{}, fmt.Errorf("%s is required: order-management orders carry no site, so the site the demand is attributed to must be configured", envDemandSiteID)
	}
	return rt, nil
}

// splitBrokers splits KAFKA_BROKERS on commas, trimming blanks.
func splitBrokers(raw string) []string {
	var out []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// parsePublisherMode validates EVENT_PUBLISHER: unset or "kafka". The log sink
// is gone (ADR 0016); the variable stays because the chart and
// warehouse-infra set it.
func parsePublisherMode(raw string) error {
	switch raw {
	case "", "kafka":
		return nil
	case "log":
		return errors.New("EVENT_PUBLISHER=log is not supported: this context publishes to real Kafka")
	default:
		return fmt.Errorf("EVENT_PUBLISHER: unknown value %q (want kafka)", raw)
	}
}

// newReadiness builds the /readyz gate: the drain gate plus a Postgres ping
// and a Kafka broker answering a topic-less metadata request (never topics:
// the broker auto-creates them on first write), cached 3s, bounded 2s. The
// ping is a parameter so the gate is testable without a pool.
func newReadiness(ping func(context.Context) error, brokers []string) *inboundhttp.Readiness {
	return inboundhttp.NewReadiness(inboundhttp.DefaultReadinessTTL, inboundhttp.DefaultReadinessTimeout,
		inboundhttp.Dependency{Name: "postgres", Check: ping},
		inboundhttp.Dependency{Name: "kafka", Check: outboundkafka.NewBrokerCheck(brokers).Check},
	)
}

// worker is a started background loop and the channel closed once it
// returned.
type worker struct {
	name string
	done chan struct{}
}

// runner is the part of a Kafka consumer the composition root drives.
type runner interface {
	Run(ctx context.Context) error
	Close() error
}

// consumerSpec is one Kafka consumer: its log name, topic, the env var that
// carries its group id and how to build it once the group is resolved.
type consumerSpec struct {
	name, topic, groupEnv string
	build                 func(brokers []string, group string) runner
}

// consumerSpecs lists the three consumers; they are always on.
func consumerSpecs(ad adapters, rt runtimeEnv, register *usecases.RegisterProcessCapacityConstraint, logger *slog.Logger) []consumerSpec {
	return []consumerSpec{
		{"labor-capacity", inboundkafka.LaborTopic, envLaborGroup, func(brokers []string, group string) runner {
			return inboundkafka.NewLaborCapacityConsumer(brokers, group, register, ad.processedEvents, ad.uow, logger)
		}},
		{"storage-capacity", inboundkafka.FacilityTopic, envStorageGroup, func(brokers []string, group string) runner {
			return inboundkafka.NewStorageCapacityConsumer(brokers, group, ad.storageTally, ad.processedEvents, ad.uow, logger)
		}},
		{"order-demand", inboundkafka.OrderTopic, envDemandConsumerGrp, func(brokers []string, group string) runner {
			record := &usecases.RecordOrderDemand{UoW: ad.uow, ProcessedEvents: ad.processedEvents, Demand: ad.orderDemand}
			return inboundkafka.NewOrderDemandConsumer(brokers, group, rt.demandSiteID, record, logger)
		}},
	}
}

// startConsumers starts every consumer; they are always on. The readers dial
// lazily inside Run, so a broker outage never blocks boot. Run only ever
// returns on error (including the plain context.Canceled of an orderly
// shutdown), never nil: a message that fails transiently does NOT end Run, it
// is retried with backoff and its offset is committed only after it succeeds.
func startConsumers(specs []consumerSpec, rt runtimeEnv, logger *slog.Logger) ([]*worker, func()) {
	ctx, stop := context.WithCancel(context.Background())
	var (
		workers []*worker
		closers []func() error
	)
	for _, spec := range specs {
		group := rt.groups[spec.groupEnv]
		consumer := spec.build(rt.brokers, group)
		w := &worker{name: spec.name + " consumer", done: make(chan struct{})}
		workers = append(workers, w)
		closers = append(closers, consumer.Close)
		go func() {
			defer close(w.done)
			logger.Info("consumer running", "consumer", spec.name, "topic", spec.topic, "group_id", group, "brokers", rt.brokers)
			if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
				logger.Error("consumer stopped", "consumer", spec.name, "error", err)
			}
		}()
	}
	return workers, func() {
		stop()
		for _, closeFn := range closers {
			_ = closeFn()
		}
	}
}

// parseRelayInterval reads OUTBOX_RELAY_INTERVAL (a Go duration such as
// "500ms" or "2s", or plain seconds), defaulting to 1s when unset,
// malformed or non-positive -- the interval is a tuning knob, never a
// reason to refuse to boot.
func parseRelayInterval(raw string, logger *slog.Logger) time.Duration {
	if raw == "" {
		return outboxrelay.DefaultInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		if secs, serr := strconv.ParseFloat(raw, 64); serr == nil {
			d, err = time.Duration(secs*float64(time.Second)), nil
		}
	}
	if err != nil || d <= 0 {
		logger.Warn("invalid OUTBOX_RELAY_INTERVAL; using the default", "value", raw, "default", outboxrelay.DefaultInterval)
		return outboxrelay.DefaultInterval
	}
	return d
}

// outboxMaxAttempts reads OUTBOX_MAX_ATTEMPTS (gap fix D12, ADR 0013): how
// many times an outbox row may fail before the relay excludes it from
// claims. Unset = the fleet default (20); 0 = unlimited; negative or
// unparsable values log a warning and use the default, so a typo never
// silently removes the cap.
func outboxMaxAttempts(raw string, logger *slog.Logger) int {
	if raw == "" {
		return outboxrelay.DefaultMaxAttempts
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		logger.Warn("invalid OUTBOX_MAX_ATTEMPTS; using the default", "value", raw, "default", outboxrelay.DefaultMaxAttempts)
		return outboxrelay.DefaultMaxAttempts
	}
	return n
}

// startOutboxRelay starts the always-on relay goroutine over the Kafka sink.
// Kafka is dialled lazily on the first send, never here, so a broker outage
// cannot crash the process at boot.
func startOutboxRelay(store outboxrelay.Store, brokers []string, logger *slog.Logger) (*worker, func(), error) {
	if len(brokers) == 0 {
		return nil, nil, errMissingBrokers
	}
	kafkaSink := outboundkafka.NewRelaySink(brokers)
	closeSink := func() {
		if err := kafkaSink.Close(); err != nil {
			logger.Error("kafka relay sink close failed", "error", err)
		}
	}
	interval := parseRelayInterval(os.Getenv(envOutboxRelayInterval), logger)
	maxAttempts := outboxMaxAttempts(os.Getenv(envOutboxMaxAttempts), logger)
	relay := outboxrelay.NewRelay(store, kafkaSink, logger, outboxrelay.WithInterval(interval), outboxrelay.WithMaxAttempts(maxAttempts))

	ctx, stop := context.WithCancel(context.Background())
	w := &worker{name: "outbox-relay", done: make(chan struct{})}
	go func() {
		defer close(w.done)
		defer closeSink()
		logger.Info("outbox relay running", "publisher", "kafka", "interval", interval, "max_attempts", maxAttempts, "topic", outboundkafka.Topic)
		if err := relay.Run(ctx); !errors.Is(err, context.Canceled) {
			logger.Error("outbox relay stopped", "error", err)
		}
	}()
	return w, stop, nil
}
