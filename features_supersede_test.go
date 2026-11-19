package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cucumber/godog"
)

// Steps of features/plan_supersede.feature (docs/adr/0024 decision 3). Plans
// are named by the scenario label they were created with.

func (w *world) labelledID(label string) (string, error) {
	id, ok := w.scenarioIDs[label]
	if !ok {
		return "", fmt.Errorf("no capacity plan was created labelled %q", label)
	}
	return id, nil
}

func (w *world) iPublishTheLabelledPlan(ctx context.Context, label string) error {
	id, err := w.labelledID(label)
	if err != nil {
		return err
	}
	return w.record(ctx, http.MethodPost, "/capacity-plans/"+id+"/publications", nil)
}

// storedPlan GETs the labelled plan without disturbing the last response.
func (w *world) storedPlan(ctx context.Context, label string) (map[string]any, error) {
	id, err := w.labelledID(label)
	if err != nil {
		return nil, err
	}
	status0, body0 := w.status, w.body
	defer func() { w.status, w.body = status0, body0 }()
	if err := w.record(ctx, http.MethodGet, "/capacity-plans/"+id, nil); err != nil {
		return nil, err
	}
	if w.status != http.StatusOK {
		return nil, fmt.Errorf("GET /capacity-plans/%s: %d %s", id, w.status, string(w.body))
	}
	var plan map[string]any
	return plan, json.Unmarshal(w.body, &plan)
}

func (w *world) theLabelledPlanIs(ctx context.Context, label, status string) error {
	plan, err := w.storedPlan(ctx, label)
	if err != nil {
		return err
	}
	if plan["status"] != status {
		return fmt.Errorf("plan %q is %v, want %s", label, plan["status"], status)
	}
	if by, present := plan["superseded_by"]; present {
		return fmt.Errorf("plan %q carries superseded_by %v, want none", label, by)
	}
	return nil
}

func (w *world) theLabelledPlanIsSupersededBy(ctx context.Context, label, newer string) error {
	plan, err := w.storedPlan(ctx, label)
	if err != nil {
		return err
	}
	newerID, err := w.labelledID(newer)
	if err != nil {
		return err
	}
	if plan["status"] != "SUPERSEDED" || plan["superseded_by"] != newerID {
		return fmt.Errorf("plan %q is %v by %v, want SUPERSEDED by %s", label, plan["status"], plan["superseded_by"], newerID)
	}
	return nil
}

// theSupersededEventNames checks the one queued CapacityPlanSuperseded: the
// subject and key are the OLDER plan, and the data names both plans plus the
// older plan's warehouse, site and location.
func (w *world) theSupersededEventNames(label, newer, warehouse, site, location string) error {
	olderID, err := w.labelledID(label)
	if err != nil {
		return err
	}
	newerID, err := w.labelledID(newer)
	if err != nil {
		return err
	}
	want := map[string]any{"plan_id": olderID, "superseded_by": newerID, "warehouse_id": warehouse, "site_id": site, "location": location}
	for _, m := range w.outbox.Messages() {
		if m.EventType != outboxEventTypePrefix+"CapacityPlanSuperseded" {
			continue
		}
		var envelope struct {
			Subject string         `json:"subject"`
			Data    map[string]any `json:"data"`
		}
		if err := json.Unmarshal(m.Value, &envelope); err != nil {
			return err
		}
		if envelope.Subject != olderID || string(m.Key) != olderID {
			return fmt.Errorf("subject %q / key %q, want the older plan %s", envelope.Subject, m.Key, olderID)
		}
		for field, value := range want {
			if envelope.Data[field] != value {
				return fmt.Errorf("data.%s = %v, want %v (data %v)", field, envelope.Data[field], value, envelope.Data)
			}
		}
		return nil
	}
	return fmt.Errorf("no CapacityPlanSuperseded event in the outbox")
}

func registerSupersedeSteps(sc *godog.ScenarioContext, w *world) {
	const ts = `"([^"]*)"`
	sc.Step(`^I publish the capacity plan labelled `+ts+`$`, w.iPublishTheLabelledPlan)
	sc.Step(`^the capacity plan labelled `+ts+` is (DRAFT|PUBLISHED)$`, w.theLabelledPlanIs)
	sc.Step(`^the capacity plan labelled `+ts+` is SUPERSEDED by the plan labelled `+ts+`$`, w.theLabelledPlanIsSupersededBy)
	sc.Step(`^the CapacityPlanSuperseded event names the plan labelled `+ts+` superseded by the plan labelled `+ts+` of warehouse `+ts+` at site `+ts+` in location `+ts+`$`, w.theSupersededEventNames)
}
