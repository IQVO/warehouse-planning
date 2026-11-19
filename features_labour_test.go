package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cucumber/godog"

	inboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// labourState is the ADR 0022 slice of the BDD world: the REAL
// labour-standard consumer over the in-memory copy CreateCapacityPlan prices
// each step with. Standards reach the copy only through the consumer's
// HandleMessage, exactly the bytes the Kafka loop would hand it.
type labourState struct {
	standards *memory.LaborStandardRepo
	consumer  *inboundkafka.LaborStandardConsumer
	seq       int
	last      []byte
}

func newLabourState() labourState {
	standards, processed := memory.NewLaborStandardRepo(), memory.NewProcessedEventRepo()
	return labourState{
		standards: standards,
		consumer: &inboundkafka.LaborStandardConsumer{
			Record: &usecases.RecordLaborStandard{
				UoW: memory.NewUnitOfWork(standards, processed), ProcessedEvents: processed, Standards: standards,
			},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
}

// laborPerformancePublished feeds one LaborStandardDefined/Revised CloudEvent
// (the producer's pinned shape, docs/adr/0022 decision 2) to the consumer.
func (w *world) laborPerformancePublished(eventName, standardID, taskType string, seconds int, site, effectiveFrom string) error {
	at, err := time.Parse(time.RFC3339, effectiveFrom)
	if err != nil {
		return err
	}
	data := map[string]any{
		"standard_id": standardID, "task_type": taskType, "expected_seconds": seconds, "effective_from": effectiveFrom,
	}
	if site != "" {
		data["site_code"] = site
	}
	w.ls.seq++
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(fmt.Sprintf("bdd-std-evt-%d", w.ls.seq))
	e.SetSource("/warehouse/labor-performance")
	e.SetType("com.warehouse.wes.labor-performance.standard." + eventName)
	e.SetSubject(standardID)
	e.SetTime(at)
	e.SetDataSchema("urn:warehouse:labor-performance:events:" + eventName + ":v1")
	if err := e.SetData("application/json", data); err != nil {
		return err
	}
	value, err := json.Marshal(e)
	if err != nil {
		return err
	}
	w.ls.last = value
	return w.ls.consumer.HandleMessage(context.Background(), value)
}

func (w *world) laborPerformancePublishedFleetWide(eventName, standardID, taskType string, seconds int, effectiveFrom string) error {
	return w.laborPerformancePublished(eventName, standardID, taskType, seconds, "", effectiveFrom)
}

func (w *world) laborPerformanceRedelivers() error {
	return w.ls.consumer.HandleMessage(context.Background(), w.ls.last)
}

// parseTaskFactors reads "PICK=3,PACK=1".
func parseTaskFactors(raw string) (map[string]any, error) {
	out := map[string]any{}
	for _, pair := range strings.Split(raw, ",") {
		step, value, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok {
			return nil, fmt.Errorf("tasks_per_order %q: want STEP=number pairs", raw)
		}
		factor, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, err
		}
		out[step] = factor
	}
	return out, nil
}

func (w *world) iCreateAPlanWithTaskFactors(ctx context.Context, warehouse, site, location, pathID, start, end string, demand, units, packages float64, factors string) error {
	tasks, err := parseTaskFactors(factors)
	if err != nil {
		return err
	}
	if err := w.record(ctx, http.MethodPost, "/capacity-plans", map[string]any{
		"warehouse_id": warehouse, "site_id": site, "location": location, "path_id": pathID,
		"window_start": start, "window_end": end, "assigned_demand": demand,
		"units_per_order": units, "packages_per_order": packages, "tasks_per_order": tasks,
	}); err != nil {
		return err
	}
	if w.status == http.StatusCreated {
		var plan struct {
			ID string `json:"id"`
		}
		if err := w.decode(&plan); err != nil {
			return err
		}
		w.planID = plan.ID
	}
	return nil
}

type labourItem struct {
	ProcessType          string  `json:"process_type"`
	StandardSeconds      float64 `json:"standard_seconds"`
	StandardSiteScoped   bool    `json:"standard_site_scoped"`
	Tasks                float64 `json:"tasks"`
	RequiredLabourHours  float64 `json:"required_labour_hours"`
	RequiredAverageHeads float64 `json:"required_average_heads"`
	Known                bool    `json:"known"`
	Reason               string  `json:"reason"`
}

func (w *world) labourRequirement() ([]labourItem, error) {
	var plan struct {
		LabourRequirement *[]labourItem `json:"labour_requirement"`
	}
	if err := w.decode(&plan); err != nil {
		return nil, err
	}
	if plan.LabourRequirement == nil {
		return nil, fmt.Errorf("labour_requirement is absent or null: %s", w.body)
	}
	return *plan.LabourRequirement, nil
}

func (w *world) labourOf(step string) (labourItem, error) {
	items, err := w.labourRequirement()
	if err != nil {
		return labourItem{}, err
	}
	for _, item := range items {
		if item.ProcessType == step {
			return item, nil
		}
	}
	return labourItem{}, fmt.Errorf("no labour requirement for %s in %+v", step, items)
}

func (w *world) theLabourRequirementIs(step string, tasks, seconds, hours, heads float64, siteScoped string) error {
	item, err := w.labourOf(step)
	if err != nil {
		return err
	}
	want := labourItem{
		ProcessType: step, StandardSeconds: seconds, StandardSiteScoped: siteScoped != "",
		Tasks: tasks, RequiredLabourHours: hours, RequiredAverageHeads: heads, Known: true,
	}
	if item != want {
		return fmt.Errorf("labour requirement of %s = %+v, want %+v", step, item, want)
	}
	return nil
}

func (w *world) theLabourRequirementIsUnknown(step, reason string) error {
	item, err := w.labourOf(step)
	if err != nil {
		return err
	}
	if item.Known || item.Reason != reason || item.Tasks != 0 || item.RequiredLabourHours != 0 || item.RequiredAverageHeads != 0 {
		return fmt.Errorf("labour requirement of %s = %+v, want unknown because %s with zero figures", step, item, reason)
	}
	return nil
}

func (w *world) everyStepIsUnknown(reason string) error {
	items, err := w.labourRequirement()
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return fmt.Errorf("no labour requirement items: %s", w.body)
	}
	for _, item := range items {
		if err := w.theLabourRequirementIsUnknown(item.ProcessType, reason); err != nil {
			return err
		}
	}
	return nil
}

// theStoredPlanStatesTheSameLabour proves GET /capacity-plans/{id} returns
// the requirement computed at creation.
func (w *world) theStoredPlanStatesTheSameLabour(ctx context.Context) error {
	created, err := w.labourRequirement()
	if err != nil {
		return err
	}
	if err := w.record(ctx, http.MethodGet, "/capacity-plans/"+w.planID, nil); err != nil {
		return err
	}
	stored, err := w.labourRequirement()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(stored, created) {
		return fmt.Errorf("GET labour_requirement = %+v, want %+v", stored, created)
	}
	return nil
}

func registerLabourSteps(sc *godog.ScenarioContext, w *world) {
	const ts = `\"([^\"]*)\"`
	const num = `(\d+(?:\.\d+)?)`
	sc.Step(`^labor-performance published (LaborStandardDefined|LaborStandardRevised) `+ts+` for task type `+ts+` of (\d+) seconds at site `+ts+` effective from `+ts+`$`, w.laborPerformancePublished)
	sc.Step(`^labor-performance published (LaborStandardDefined|LaborStandardRevised) `+ts+` for task type `+ts+` of (\d+) seconds effective from `+ts+`$`, w.laborPerformancePublishedFleetWide)
	sc.Step(`^labor-performance redelivers its last standard$`, w.laborPerformanceRedelivers)
	sc.Step(`^I create a capacity plan for warehouse `+ts+` at site `+ts+` in location `+ts+` on path `+ts+` for the window `+ts+` to `+ts+` with assigned demand `+num+`, units_per_order `+num+`, packages_per_order `+num+` and tasks_per_order `+ts+`$`, w.iCreateAPlanWithTaskFactors)
	sc.Step(`^the labour requirement of ([A-Z]+) is `+num+` tasks at `+num+` seconds, `+num+` labour hours and `+num+` average heads( from the site's own standard)?$`, w.theLabourRequirementIs)
	sc.Step(`^the labour requirement of ([A-Z]+) is unknown because `+ts+`$`, w.theLabourRequirementIsUnknown)
	sc.Step(`^every step's labour requirement is unknown because `+ts+`$`, w.everyStepIsUnknown)
	sc.Step(`^the stored capacity plan states the same labour requirement$`, w.theStoredPlanStatesTheSameLabour)
}
