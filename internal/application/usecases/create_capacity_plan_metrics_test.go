package usecases

import (
	"context"
	"errors"
	"testing"
)

// fakePlanMetrics records every outcome passed to CapacityPlanCreated.
type fakePlanMetrics struct{ outcomes []string }

func (m *fakePlanMetrics) CapacityPlanCreated(_ context.Context, outcome string) {
	m.outcomes = append(m.outcomes, outcome)
}

func TestCreateCapacityPlan_RecordsMetricsOutcome(t *testing.T) {
	t.Run("success -> created", func(t *testing.T) {
		f := newPlanFixture(t)
		metrics := &fakePlanMetrics{}
		f.create.Metrics = metrics
		if _, err := f.create.Handle(context.Background(), planCommand(12000)); err != nil {
			t.Fatalf("Handle: %v", err)
		}
		if len(metrics.outcomes) != 1 || metrics.outcomes[0] != "created" {
			t.Fatalf("outcomes = %v, want [created]", metrics.outcomes)
		}
	})

	t.Run("rejection -> rejected", func(t *testing.T) {
		f := newPlanFixture(t)
		metrics := &fakePlanMetrics{}
		f.create.Metrics = metrics
		cmd := planCommand(12000)
		cmd.WarehouseID = "" // deterministic domain validation rejection
		if _, err := f.create.Handle(context.Background(), cmd); err == nil {
			t.Fatal("want a rejection error")
		}
		if len(metrics.outcomes) != 1 || metrics.outcomes[0] != "rejected" {
			t.Fatalf("outcomes = %v, want [rejected]", metrics.outcomes)
		}
	})

	t.Run("outbox failure (transient-shaped) -> rejected", func(t *testing.T) {
		f := newPlanFixture(t)
		metrics := &fakePlanMetrics{}
		f.create.Metrics = metrics
		f.create.Outbox = failingOutbox{err: errors.New("boom")}
		if _, err := f.create.Handle(context.Background(), planCommand(12000)); err == nil {
			t.Fatal("want an error")
		}
		if len(metrics.outcomes) != 1 || metrics.outcomes[0] != "rejected" {
			t.Fatalf("outcomes = %v, want [rejected]", metrics.outcomes)
		}
	})

	t.Run("nil Metrics (every pre-existing caller) never panics", func(t *testing.T) {
		f := newPlanFixture(t)
		if f.create.Metrics != nil {
			t.Fatal("fixture must leave Metrics nil for this case")
		}
		if _, err := f.create.Handle(context.Background(), planCommand(12000)); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	})
}
