package usecases

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

// RecordProcessFlow writes one process-path-management ProcessFlowDefined,
// ProcessFlowRevised or ProcessFlowRetired event into the local copy of the
// flows (docs/adr/0025 decision 1). The processed-event claim and the write
// are ONE ports.UnitOfWork.Do, so a failure after the claim rolls the claim
// back and the redelivery is processed instead of skipped. The copy raises no
// event of its own.
type RecordProcessFlow struct {
	UoW             ports.UnitOfWork
	ProcessedEvents ports.ProcessedEventRepository
	Flows           ports.ProcessFlowRepository
}

// Handle claims (consumer, eventID) and records f atomically. OutcomeApplied
// means the flow was stored (its version is greater than the stored one),
// OutcomeStale that the stored version is equal or higher (nothing changed),
// OutcomeDuplicate that the CloudEvents id was already processed. It returns a
// non-nil error ONLY for infrastructure failures; everything deterministic was
// rejected before the flow was built.
func (uc *RecordProcessFlow) Handle(ctx context.Context, consumer, eventID string, f processflow.Flow) (RecordOutcome, error) {
	outcome := OutcomeDuplicate
	err := uc.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := uc.ProcessedEvents.Claim(ctx, consumer, eventID)
		if err != nil || !claimed {
			return err
		}
		applied, err := uc.Flows.Record(ctx, f)
		if err != nil {
			return err
		}
		outcome = OutcomeStale
		if applied {
			outcome = OutcomeApplied
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}
