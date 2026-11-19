package repocontract

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

// commitment is the committed heads of (siteA, shift1, pathP) observed at
// T0 + v minutes, with heads heads.
func commitment(heads, v int) labourplan.Commitment {
	return labourplan.Commitment{
		SiteID: siteA, ShiftID: shift1, PathID: pathP,
		Heads: heads, ObservedAt: T0.Add(time.Duration(v) * time.Minute),
	}
}

// getCommitment reads (siteA, shift1, pathP) and fails when it is absent.
func getCommitment(t *testing.T, fx Fixture) labourplan.Commitment {
	t.Helper()
	got, found, err := fx.Commitments.Get(ctx, siteA, shift1, pathP)
	noErr(t, err)
	eq(t, "found", found, true)
	return got
}

// committedHeadsCases is the ports.CommittedHeadsRepository contract
// (docs/adr/0023 decision 3): the upsert keeps the last recorded drift, the
// drift record needs a commitment, the keys are independent and a failed
// unit of work leaves nothing behind.
var committedHeadsCases = []named{
	{"an absent key is not found", func(t *testing.T, fx Fixture) {
		got, found, err := fx.Commitments.Get(ctx, siteA, shift1, pathP)
		noErr(t, err)
		eq(t, "found", found, false)
		eq(t, "commitment", got, labourplan.Commitment{})
	}},
	{"a first commitment round-trips with no recorded drift", func(t *testing.T, fx Fixture) {
		noErr(t, fx.Commitments.SaveCommitment(ctx, commitment(12, 0)))
		got := getCommitment(t, fx)
		eq(t, "heads", got.Heads, 12)
		eq(t, "observed_at", got.ObservedAt.Equal(T0), true)
		eq(t, "has last drift", got.HasLastDrift, false)
		eq(t, "key", got.SiteID+"/"+got.ShiftID+"/"+got.PathID, siteA+"/"+shift1+"/"+pathP)
	}},
	{"a recorded drift survives a new commitment", func(t *testing.T, fx Fixture) {
		noErr(t, fx.Commitments.SaveCommitment(ctx, commitment(12, 0)))
		noErr(t, fx.Commitments.RecordDrift(ctx, siteA, shift1, pathP, -3))
		noErr(t, fx.Commitments.SaveCommitment(ctx, commitment(14, 5)))
		got := getCommitment(t, fx)
		eq(t, "heads", got.Heads, 14)
		eq(t, "observed_at", got.ObservedAt.Equal(T0.Add(5*time.Minute)), true)
		eq(t, "has last drift", got.HasLastDrift, true)
		eq(t, "last drift", got.LastDriftHeads, -3)
	}},
	{"a zero drift is recorded as a drift", func(t *testing.T, fx Fixture) {
		noErr(t, fx.Commitments.SaveCommitment(ctx, commitment(12, 0)))
		noErr(t, fx.Commitments.RecordDrift(ctx, siteA, shift1, pathP, 4))
		noErr(t, fx.Commitments.RecordDrift(ctx, siteA, shift1, pathP, 0))
		got := getCommitment(t, fx)
		eq(t, "has last drift", got.HasLastDrift, true)
		eq(t, "last drift", got.LastDriftHeads, 0)
	}},
	{"a drift without a commitment is ErrCommittedHeadsNotFound", func(t *testing.T, fx Fixture) {
		wantErr(t, fx.Commitments.RecordDrift(ctx, siteA, shift1, pathP, 2), ports.ErrCommittedHeadsNotFound)
		_, found, err := fx.Commitments.Get(ctx, siteA, shift1, pathP)
		noErr(t, err)
		eq(t, "found", found, false)
	}},
	{"keys are independent", func(t *testing.T, fx Fixture) {
		noErr(t, fx.Commitments.SaveCommitment(ctx, commitment(12, 0)))
		for _, k := range [][3]string{{siteB, shift1, pathP}, {siteA, shift2, pathP}, {siteA, shift1, pathQ}} {
			_, found, err := fx.Commitments.Get(ctx, k[0], k[1], k[2])
			noErr(t, err)
			eq(t, "found "+k[0]+"/"+k[1]+"/"+k[2], found, false)
			wantErr(t, fx.Commitments.RecordDrift(ctx, k[0], k[1], k[2], 1), ports.ErrCommittedHeadsNotFound)
		}
		eq(t, "has last drift", getCommitment(t, fx).HasLastDrift, false)
	}},
	{"a failed unit of work leaves no commitment and no drift behind", func(t *testing.T, fx Fixture) {
		noErr(t, fx.Commitments.SaveCommitment(ctx, commitment(12, 0)))
		err := fx.UoW.Do(ctx, func(ctx context.Context) error {
			if err := fx.Commitments.SaveCommitment(ctx, commitment(20, 9)); err != nil {
				return err
			}
			if err := fx.Commitments.RecordDrift(ctx, siteA, shift1, pathP, 8); err != nil {
				return err
			}
			return errInjected
		})
		wantErr(t, err, errInjected)
		got := getCommitment(t, fx)
		eq(t, "heads", got.Heads, 12)
		eq(t, "has last drift", got.HasLastDrift, false)
	}},
}
