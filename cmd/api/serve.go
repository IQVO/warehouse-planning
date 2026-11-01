package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
)

// serveUntilSignal runs httpServer until SIGINT/SIGTERM (or a listen
// error), then performs the fleet's graceful-shutdown sequence (docs/adr
// on standard metrics / readiness, mirroring order-management's
// drainUnderShutdown and fulfillment-execution's waitForShutdown, the
// fleet references for this phase):
//
//  1. readiness.SetNotReady() -- /readyz starts answering 503 so a
//     Kubernetes readinessProbe stops routing NEW traffic here;
//  2. wait shutdownDrainDelay() -- gives the probe/endpoint controller a
//     window to observe the flip BEFORE the listener closes;
//  3. httpServer.Shutdown -- drain in-flight HTTP requests (one outbox
//     writer);
//  4. stop and await the Kafka consumers (the other outbox writer),
//     including the commit of any message they are mid-handling;
//  5. stop and await the outbox relay LAST -- only after every writer
//     above has stopped, so an event committed by the HTTP drain or a
//     consumer's last message is not stranded until the next pod boots;
//  6. the caller's deferred closeAdapters() (pool.Close) runs after this
//     function returns, i.e. after every step above.
//
// Steps 3-5 share shutdownTimeout/consumerDrainTimeout; the drain delay is
// additional (terminationGracePeriodSeconds must cover both).
//
// The process also exits non-zero when the relay or a consumer returns on its
// own (a lost broker, a failed reader): the service cannot run without them
// and must not sit "alive" with a dead worker, so the orchestrator restarts
// it.
func serveUntilSignal(
	parent context.Context,
	logger *slog.Logger,
	httpServer *http.Server,
	readiness *inboundhttp.Readiness,
	consumers []*worker,
	closeConsumers func(),
	relay *worker,
	closeRelay func(),
) error {
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	died := watchWorkers(append([]*worker{relay}, consumers...)...)

	select {
	case err := <-errCh:
		closeConsumers()
		closeRelay()
		return err
	case name := <-died:
		closeConsumers()
		closeRelay()
		_ = httpServer.Close()
		return fmt.Errorf("%s stopped unexpectedly: exiting so the orchestrator restarts the service", name)
	case <-ctx.Done():
	}

	readiness.SetNotReady()
	if delay := shutdownDrainDelay(logger); delay > 0 {
		logger.Info("shutdown: readiness flipped to not-ready; waiting for traffic to drain", "drain_delay", delay.String())
		time.Sleep(delay)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	err := httpServer.Shutdown(shutdownCtx)

	closeConsumers()
	for _, c := range consumers {
		await(logger, c)
	}

	// Every outbox writer has now stopped: let the relay finish its
	// in-flight pass so an event committed by the last HTTP request or a
	// consumer's last message is not stranded until the next pod boots.
	closeRelay()
	await(logger, relay)

	return err
}

// watchWorkers returns a channel that receives the name of every worker that
// returns. Workers only return when stopped or on an unrecoverable failure, so
// a value read while the service is serving means a worker died.
func watchWorkers(workers ...*worker) <-chan string {
	died := make(chan string, len(workers))
	for _, w := range workers {
		if w == nil {
			continue
		}
		go func() {
			<-w.done
			died <- w.name
		}()
	}
	return died
}

// await waits (bounded) for w to return; a nil w is a no-op.
func await(logger *slog.Logger, w *worker) {
	if w == nil {
		return
	}
	select {
	case <-w.done:
	case <-time.After(consumerDrainTimeout):
		logger.Warn("worker did not stop before the shutdown drain deadline", "worker", w.name)
	}
}

// DefaultShutdownDrainDelay is how long shutdown waits, after flipping
// /readyz to not-ready and before closing the HTTP listener, for
// Kubernetes' readinessProbe and the endpoint controller to observe the
// flip and stop routing NEW traffic to this pod. SHUTDOWN_DRAIN_DELAY
// overrides it; "0" disables the wait (tests, local dev).
const DefaultShutdownDrainDelay = 5 * time.Second

// shutdownDrainDelay reads SHUTDOWN_DRAIN_DELAY. Unlike getenv's fallback
// convention, "0" is a legal value (disable the delay); an unset,
// negative or unparsable value falls back to DefaultShutdownDrainDelay.
func shutdownDrainDelay(logger *slog.Logger) time.Duration {
	raw := os.Getenv("SHUTDOWN_DRAIN_DELAY")
	if raw == "" {
		return DefaultShutdownDrainDelay
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		logger.Warn("ignoring invalid SHUTDOWN_DRAIN_DELAY", "value", raw, "default", DefaultShutdownDrainDelay.String())
		return DefaultShutdownDrainDelay
	}
	return d
}
