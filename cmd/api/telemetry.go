package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/telemetry"
)

// serviceVersion is this binary's OTel service.version resource
// attribute. SERVICE_VERSION is set by the chart from the image tag; a
// local run without it reports "dev".
func serviceVersion() string {
	return getenv("SERVICE_VERSION", "dev")
}

// setupTelemetry installs the standard-metrics adapter (docs/adr/0011):
// a TracerProvider + MeterProvider exporting OTLP to an optional
// Collector (Setup never blocks on the Collector being reachable -- a
// missing one degrades to "telemetry dropped", never to "service won't
// start"), and registers the Tier-2 plan-created counter. The returned
// closer flushes both providers on a bounded timeout and logs (never
// returns) a shutdown failure; call it via defer.
func setupTelemetry(logger *slog.Logger) (closer func(), metrics *telemetry.PlanMetrics, err error) {
	otelCtx, otelCancel := context.WithTimeout(context.Background(), 10*time.Second)
	otelShutdown, err := telemetry.Setup(otelCtx, inboundhttp.DefaultServiceName, serviceVersion(), getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint))
	otelCancel()
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry setup: %w", err)
	}
	closer = func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(shutdownCtx); err != nil {
			logger.Error("telemetry shutdown failed", "error", err)
		}
	}
	metrics, err = telemetry.NewPlanMetrics()
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry: register plan metrics: %w", err)
	}
	return closer, metrics, nil
}

// newLogger builds the process-wide structured logger. LOG_LEVEL maps
// debug|info|warn|warning|error (case-insensitive; "warning" is accepted
// like "warn" fleet-wide, gap fix D22) to the matching slog.Level,
// defaulting to Info for unset/unrecognized values.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
