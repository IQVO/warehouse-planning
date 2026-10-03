// Package main_test hosts the godog (Cucumber for Go) acceptance suite. It
// drives the REAL chi router over HTTP -- the same wiring the service uses
// in production, but with the in-memory outbound adapter -- so every
// scenario in features/*.feature is a true black-box test of the REST API.
// Mirrors inventory-storage's root-level features_test.go shape.
package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// TestFeatures runs every Gherkin feature under features/ against a
// freshly wired HTTP server.
func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			Strict:   true,
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run feature tests")
	}
}

// world is the per-scenario state: a running server over a fresh in-memory
// repository, plus whatever the last HTTP call returned.
type world struct {
	server *httptest.Server

	status int
	body   []byte
}

// start builds the composition root the way cmd/ would, but with the
// in-memory repo.
func (w *world) start() {
	repo := memory.NewProcessCapacityRepo()
	pathRepo := memory.NewProcessPathRepo()
	s := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: repo},
		ProcessCapacities:                 repo,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: pathRepo},
		GetProcessPathCapacity:            &usecases.GetProcessPathCapacity{ProcessPaths: pathRepo, ProcessCapacities: repo},
	}
	w.server = httptest.NewServer(inboundhttp.NewRouter(s))
	w.status = 0
	w.body = nil
}

func (w *world) stop() {
	if w.server != nil {
		w.server.Close()
		w.server = nil
	}
}

// record issues a request and remembers the response the Then steps assert
// against.
func (w *world) record(ctx context.Context, method, path string, payload any) error {
	var reader io.Reader = http.NoBody
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		reader = strings.NewReader(string(encoded))
	}

	req, err := http.NewRequestWithContext(ctx, method, w.server.URL+path, reader)
	if err != nil {
		return err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := w.server.Client().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.status, w.body = resp.StatusCode, body
	return nil
}

func (w *world) decode(dest any) error {
	if err := json.Unmarshal(w.body, dest); err != nil {
		return fmt.Errorf("response body is not valid JSON (%w): %s", err, string(w.body))
	}
	return nil
}

// ----------------------------------------------------------------- When ----

// unitFor maps a Gherkin unit word ("UNIT", "PACKAGE", ...) straight
// through -- the domain's CapacityUnit values are already upper-case
// words, so no translation table is needed.
func (w *world) iRegisterAConstraint(ctx context.Context, constraintType string, quantity float64, unit, processType, location, windowStart, windowEnd string) error {
	return w.record(ctx, http.MethodPost, "/process-capacities", map[string]any{
		"process_type":    processType,
		"location":        location,
		"window_start":    windowStart,
		"window_end":      windowEnd,
		"constraint_type": constraintType,
		"quantity":        quantity,
		"unit":            unit,
		"period_seconds":  3600,
	})
}

func (w *world) iLookUpTheEffectiveCapacity(ctx context.Context, processType, location, windowStart, windowEnd string) error {
	path := fmt.Sprintf("/process-capacities?process_type=%s&location=%s&window_start=%s&window_end=%s", processType, location, windowStart, windowEnd)
	return w.record(ctx, http.MethodGet, path, nil)
}

// iRegisterAProcessPath handles "I register a process path "<id>" named
// "<name>" with steps PICK, REBIN, PACK" -- stepsRaw is the raw
// comma-separated step list as it appears in the Gherkin step text.
func (w *world) iRegisterAProcessPath(ctx context.Context, id, name, stepsRaw string) error {
	rawSteps := strings.Split(stepsRaw, ",")
	steps := make([]string, 0, len(rawSteps))
	for _, s := range rawSteps {
		steps = append(steps, strings.TrimSpace(s))
	}
	return w.record(ctx, http.MethodPost, "/process-paths", map[string]any{
		"id":    id,
		"name":  name,
		"steps": steps,
	})
}

func (w *world) iLookUpTheProcessPathCapacity(ctx context.Context, pathID, location, windowStart, windowEnd string, unitsPerOrder, packagesPerOrder float64) error {
	path := fmt.Sprintf("/process-paths/%s/capacity?location=%s&window_start=%s&window_end=%s&units_per_order=%v&packages_per_order=%v",
		pathID, location, windowStart, windowEnd, unitsPerOrder, packagesPerOrder)
	return w.record(ctx, http.MethodGet, path, nil)
}

// ----------------------------------------------------------------- Then ----

func (w *world) theResponseStatusIs(expected int) error {
	if w.status != expected {
		return fmt.Errorf("expected status %d, got %d: %s", expected, w.status, string(w.body))
	}
	return nil
}

func (w *world) theEffectiveCapacityResponseReports(rate float64, unit, _ string, binding string) error {
	var body struct {
		EffectiveRate     float64 `json:"effective_rate"`
		EffectiveUnit     string  `json:"effective_unit"`
		BindingConstraint string  `json:"binding_constraint"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.EffectiveRate != rate || body.EffectiveUnit != unit || body.BindingConstraint != binding {
		return fmt.Errorf("expected %v %s/HOUR bound by %s, got %v %s bound by %s", rate, unit, binding, body.EffectiveRate, body.EffectiveUnit, body.BindingConstraint)
	}
	return nil
}

func (w *world) theEffectiveCapacityResponseListsConstraints(expected int) error {
	var body struct {
		Constraints []struct {
			ConstraintType string `json:"constraint_type"`
		} `json:"constraints"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if len(body.Constraints) != expected {
		return fmt.Errorf("expected %d constraints, got %d", expected, len(body.Constraints))
	}
	return nil
}

func (w *world) theProblemDetailTypeIs(slug string) error {
	var problem struct {
		Type   string `json:"type"`
		Status int    `json:"status"`
	}
	if err := w.decode(&problem); err != nil {
		return err
	}
	if got := problem.Type[strings.LastIndex(problem.Type, "/")+1:]; got != slug {
		return fmt.Errorf("expected problem type %q, got %q (from %q)", slug, got, problem.Type)
	}
	if problem.Status != w.status {
		return fmt.Errorf("problem body status %d does not match HTTP status %d", problem.Status, w.status)
	}
	return nil
}

// theProcessPathCapacityResponseReports handles "the process path capacity
// response reports <rate> ORDER per HOUR bound by <step>".
func (w *world) theProcessPathCapacityResponseReports(rate float64, unit, _ string, bottleneck string) error {
	var body struct {
		NormalizedRate float64 `json:"normalized_rate"`
		NormalizedUnit string  `json:"normalized_unit"`
		BottleneckStep string  `json:"bottleneck_step"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.NormalizedRate != rate || body.NormalizedUnit != unit || body.BottleneckStep != bottleneck {
		return fmt.Errorf("expected %v %s/HOUR bound by %s, got %v %s bound by %s", rate, unit, bottleneck, body.NormalizedRate, body.NormalizedUnit, body.BottleneckStep)
	}
	return nil
}

// ------------------------------------------------------------- wiring ------

// InitializeScenario registers the step definitions and gives every
// scenario its own server and its own in-memory state.
func InitializeScenario(sc *godog.ScenarioContext) {
	w := &world{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w.start()
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		w.stop()
		return ctx, nil
	})

	sc.Step(`^I register an? (LABOR|LOCATION|EQUIPMENT|STATION|CONVEYOR|BUFFER|REPLENISHMENT) constraint of (\d+(?:\.\d+)?) (UNIT|LINE|ORDER|PACKAGE) per HOUR for ([A-Z-]+) at ([A-Z0-9-]+) for the window "([^"]*)" to "([^"]*)"$`,
		func(ctx context.Context, constraintType string, quantity float64, unit, processType, location, windowStart, windowEnd string) error {
			return w.iRegisterAConstraint(ctx, constraintType, quantity, unit, processType, location, windowStart, windowEnd)
		})
	sc.Step(`^I look up the effective capacity for ([A-Z-]+) at ([A-Z0-9-]+) for the window "([^"]*)" to "([^"]*)"$`, w.iLookUpTheEffectiveCapacity)
	sc.Step(`^I register a process path "([^"]*)" named "([^"]*)" with steps ([A-Z, ]+)$`, w.iRegisterAProcessPath)
	sc.Step(`^I look up the capacity of process path "([^"]*)" at "([^"]*)" for the window "([^"]*)" to "([^"]*)" with units_per_order (\d+(?:\.\d+)?) and packages_per_order (\d+(?:\.\d+)?)$`, w.iLookUpTheProcessPathCapacity)

	sc.Step(`^the response status is (\d+)$`, w.theResponseStatusIs)
	sc.Step(`^the effective capacity response reports (\d+(?:\.\d+)?) (UNIT|LINE|ORDER|PACKAGE) per (HOUR) bound by (LABOR|LOCATION|EQUIPMENT|STATION|CONVEYOR|BUFFER|REPLENISHMENT)$`, w.theEffectiveCapacityResponseReports)
	sc.Step(`^the effective capacity response lists (\d+) constraints?$`, w.theEffectiveCapacityResponseListsConstraints)
	sc.Step(`^the problem detail type is "([^"]*)"$`, w.theProblemDetailTypeIs)
	sc.Step(`^the process path capacity response reports (\d+(?:\.\d+)?) (ORDER) per (HOUR) bound by ([A-Z-]+)$`, w.theProcessPathCapacityResponseReports)
}
