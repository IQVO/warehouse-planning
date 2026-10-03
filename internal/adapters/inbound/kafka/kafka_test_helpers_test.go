package kafka_test

import (
	"context"
	"io"
	"log/slog"

	"github.com/claudioed/warehouse-planning/internal/application/tally"
)

// testLogger returns a discard-everything logger so tests' expected WARN
// logs (malformed/unknown/untracked messages) don't spam `go test -v`
// output.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeProcessedEvents lets a test force
// ports.ProcessedEventRepository.Claim to fail, proving a consumer
// propagates the error instead of masking it.
type fakeProcessedEvents struct {
	err error
}

func (f fakeProcessedEvents) Claim(context.Context, string, string) (bool, error) {
	return false, f.err
}

// fakeFailingTally lets a test force ports.StorageTallyRepository to
// fail, proving StorageCapacityConsumer propagates the error instead of
// masking it.
type fakeFailingTally struct {
	err error
}

func (f fakeFailingTally) RegisterSlot(context.Context, string, string, string, []string) ([]tally.Update, error) {
	return nil, f.err
}

func (f fakeFailingTally) DecommissionSlot(context.Context, string) ([]tally.Update, bool, error) {
	return nil, false, f.err
}
