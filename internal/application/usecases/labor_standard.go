package usecases

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

// RecordLaborStandard writes one labor-performance LaborStandardDefined or
// LaborStandardRevised event into the local copy of the labour standards
// (docs/adr/0022 decisions 1 and 3). The processed-event claim and the write
// are ONE ports.UnitOfWork.Do, so a failure after the claim rolls the claim
// back and the redelivery is processed instead of skipped. The copy raises no
// event of its own.
type RecordLaborStandard struct {
	UoW             ports.UnitOfWork
	ProcessedEvents ports.ProcessedEventRepository
	Standards       ports.LaborStandardRepository
}

// Handle claims (consumer, eventID) and records s atomically. OutcomeApplied
// means the record was stored, OutcomeStale that its standard id was already
// stored (the copy keeps one row per id, so nothing changed), OutcomeDuplicate
// that the CloudEvents id was already processed. It returns a non-nil error
// ONLY for infrastructure failures; everything deterministic was rejected
// before the standard was built.
func (uc *RecordLaborStandard) Handle(ctx context.Context, consumer, eventID string, s laborstandard.Standard) (RecordOutcome, error) {
	outcome := OutcomeDuplicate
	err := uc.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := uc.ProcessedEvents.Claim(ctx, consumer, eventID)
		if err != nil || !claimed {
			return err
		}
		recorded, err := uc.Standards.Record(ctx, s)
		if err != nil {
			return err
		}
		outcome = OutcomeStale
		if recorded {
			outcome = OutcomeApplied
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}
