package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	inboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// Environment of the order-demand consumer (docs/adr/0004-demand-ingestion-
// from-order-management.md). DEMAND_CONSUMER_GROUP has NO default and is the
// feature's off switch: unset means no consumer, no Kafka reader, no extra
// query -- the service behaves exactly as it did before the read model
// existed. A default would let a locally run process join the live group.
const (
	envDemandConsumerGroup = "DEMAND_CONSUMER_GROUP"
	envDemandSiteID        = "DEMAND_SITE_ID"
)

// demandConfig is the resolved configuration of the order-demand consumer.
type demandConfig struct {
	// group is the Kafka consumer group id (DEMAND_CONSUMER_GROUP).
	group string
	// siteID is the ONE site every consumed order is attributed to
	// (DEMAND_SITE_ID): order-management's events carry no fulfillment site.
	siteID string
}

// demandConfigFromEnv reads the consumer's configuration. enabled is false
// when DEMAND_CONSUMER_GROUP is unset. A group without a site is a
// configuration error (refuse to boot rather than attribute demand to an
// empty site).
func demandConfigFromEnv() (cfg demandConfig, enabled bool, err error) {
	group := os.Getenv(envDemandConsumerGroup)
	if group == "" {
		return demandConfig{}, false, nil
	}
	site := os.Getenv(envDemandSiteID)
	if site == "" {
		return demandConfig{}, false, fmt.Errorf("%s is set but %s is not: order-management orders carry no site, so the site demand is attributed to must be configured", envDemandConsumerGroup, envDemandSiteID)
	}
	return demandConfig{group: group, siteID: site}, true, nil
}

// joinClosers returns a func that runs every given closer in order.
func joinClosers(closers ...func()) func() {
	return func() {
		for _, c := range closers {
			c()
		}
	}
}

// startDemandConsumer starts the order-demand consumer as a background
// goroutine when it is enabled (DEMAND_CONSUMER_GROUP set) and KAFKA_BROKERS
// is configured. It returns nothing to run otherwise. Like the other
// consumers it never dials Kafka at construction: the connection happens
// inside Run's first FetchMessage.
func startDemandConsumer(ad adapters, logger *slog.Logger) ([]runningConsumer, func(), error) {
	cfg, enabled, err := demandConfigFromEnv()
	if err != nil {
		return nil, func() {}, err
	}
	if !enabled {
		logger.Info("order demand consumption is disabled", "enable_with", envDemandConsumerGroup)
		return nil, func() {}, nil
	}
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		logger.Warn("KAFKA_BROKERS not configured; order demand Kafka ingestion is disabled")
		return nil, func() {}, nil
	}
	brokers := strings.Split(raw, ",")

	record := &usecases.RecordOrderDemand{UoW: ad.uow, ProcessedEvents: ad.processedEvents, Demand: ad.orderDemand}
	consumer := inboundkafka.NewOrderDemandConsumer(brokers, cfg.group, cfg.siteID, record, logger)

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		logger.Info("order demand consumer running", "topic", inboundkafka.OrderTopic, "group_id", cfg.group, "site_id", cfg.siteID, "brokers", brokers)
		if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
			logger.Error("order demand consumer stopped", "error", err)
		}
	}()
	closeFn := func() {
		stop()
		_ = consumer.Close()
	}
	return []runningConsumer{{name: "order-demand", done: done}}, closeFn, nil
}
