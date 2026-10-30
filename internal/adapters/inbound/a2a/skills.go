package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// tracerName is the OTel instrumentation scope for A2A skill spans, mirroring
// the MCP adapter's mcp.tool spans.
const tracerName = "github.com/claudioed/warehouse-planning/internal/adapters/inbound/a2a"

// Skill ids advertised on the Agent Card (see Card).
const (
	skillPathCapacity         = "path_capacity"
	skillEvaluateCapacityPlan = "evaluate_capacity_plan"
)

// skillRequest is the wire contract of every skill call: the first message
// part carries (as a Data part, or a Text part holding the same JSON) the
// skill id and its arguments. Strict on purpose -- an agent client is
// machine-to-machine, so unknown fields are rejected, never ignored: a
// misspelled name like "unitsPerOrdr" must not silently drop a conversion
// factor. (Go's encoding/json matches field names case-insensitively, so
// "unitsperorder" still reaches unitsPerOrder; only genuinely unknown names
// are refused.)
type skillRequest struct {
	Skill     string          `json:"skill"`
	Arguments json.RawMessage `json:"arguments"`
}

// pathCapacityArgs and evaluatePlanArgs are the per-skill argument shapes.
// Names are camelCase; the snake_case names of the REST and MCP surfaces are
// deliberately not accepted here.
type pathCapacityArgs struct {
	PathID           string   `json:"pathId"`
	Location         string   `json:"location"`
	WindowStart      string   `json:"windowStart"`
	WindowEnd        string   `json:"windowEnd"`
	UnitsPerOrder    *float64 `json:"unitsPerOrder"`
	PackagesPerOrder *float64 `json:"packagesPerOrder"`
}

type evaluatePlanArgs struct {
	WarehouseID      string   `json:"warehouseId"`
	SiteID           string   `json:"siteId"`
	Location         string   `json:"location"`
	PathID           string   `json:"pathId"`
	WindowStart      string   `json:"windowStart"`
	WindowEnd        string   `json:"windowEnd"`
	AssignedDemand   *float64 `json:"assignedDemand"`
	UnitsPerOrder    *float64 `json:"unitsPerOrder"`
	PackagesPerOrder *float64 `json:"packagesPerOrder"`
}

// answer maps one incoming message to a skill and returns the agent's reply
// Message. It is the whole executor body: this is a message-only agent, so a
// skill run is exactly one request mapped to one answer (ADR 0018).
func answer(ctx context.Context, deps Deps, ec *a2asrv.ExecutorContext) a2a.Event {
	req, prob := parseSkillRequest(ec)
	if prob != nil {
		return reply(ec, envelope{Skill: "", Problem: prob}, prob.summary())
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "a2a.skill "+req.Skill)
	defer span.End()
	span.SetAttributes(attribute.String("a2a.skill", req.Skill))

	dto, prob := run(ctx, deps, req)
	if prob != nil {
		span.SetStatus(codes.Error, prob.Detail)
		return reply(ec, envelope{Skill: req.Skill, Problem: prob}, prob.summary())
	}
	return reply(ec, envelope{Skill: req.Skill, Data: dto}, dto.Summary())
}

// run dispatches one validated request to its skill.
func run(ctx context.Context, deps Deps, req skillRequest) (skillDTO, *problem) {
	switch req.Skill {
	case skillPathCapacity:
		data, prob := deps.pathCapacity(ctx, req.Arguments)
		if prob != nil {
			return skillDTO{}, prob
		}
		return skillDTO{PathCapacity: &data, summary: data.summary}, nil
	case skillEvaluateCapacityPlan:
		data, prob := deps.evaluateCapacityPlan(ctx, req.Arguments)
		if prob != nil {
			return skillDTO{}, prob
		}
		return skillDTO{CapacityPlan: &data, summary: data.summary}, nil
	default:
		return skillDTO{}, problemUnknownSkill(req.Skill)
	}
}

// reply builds the agent Message for one skill run: a Data part carrying the
// structured envelope and a Text part carrying the one-line summary, bound to
// the caller's context (and task, when the client referenced one).
func reply(ec *a2asrv.ExecutorContext, env envelope, summary string) a2a.Event {
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, ec, a2a.NewDataPart(env), a2a.NewTextPart(summary))
}

// parseSkillRequest extracts the skillRequest from the message's first part. A
// Data part (or a Text part holding the same JSON object) is accepted;
// anything else is a request-shape problem, answered -- not raised -- because
// a malformed delegation is a business answer for the calling agent, not a
// transport failure.
func parseSkillRequest(ec *a2asrv.ExecutorContext) (skillRequest, *problem) {
	msg := ec.Message
	if msg == nil || len(msg.Parts) == 0 {
		return skillRequest{}, problemMalformedRequest("the message carries no parts")
	}
	raw, err := partJSON(msg.Parts[0])
	if err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the first message part is not a skill request: %v", err))
	}
	var req skillRequest
	if err := decodeStrict(raw, &req); err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the skill request is not valid: %v", err))
	}
	if req.Skill == "" {
		return skillRequest{}, problemMalformedRequest(`the skill request carries no "skill" field`)
	}
	return req, nil
}

// partJSON renders one part's content as the JSON bytes of a skill request.
func partJSON(part *a2a.Part) ([]byte, error) {
	switch content := part.Content.(type) {
	case a2a.Data:
		return json.Marshal(content.Value)
	case a2a.Text:
		return []byte(string(content)), nil
	default:
		return nil, errors.New("expected a data part (or a text part holding the skill JSON)")
	}
}

// decodeStrict decodes one JSON document into dst, rejecting unknown fields
// and trailing data. Absent arguments decode as the empty object so each
// skill's own required-argument checks produce the specific problem.
func decodeStrict(raw []byte, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after the JSON document")
	}
	return nil
}

// namedArg pairs an argument's wire name with its value for the
// required-argument check.
type namedArg struct{ name, value string }

// requireArgs answers a 400 naming every required argument that is absent or
// blank, in the order given.
func requireArgs(args ...namedArg) *problem {
	var missing []string
	for _, a := range args {
		if strings.TrimSpace(a.value) == "" {
			missing = append(missing, a.name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return problemInvalidArgument(strings.Join(missing, ", ") + " are required")
}

// parseWindow parses the RFC3339 window bounds every skill takes. A malformed
// bound is the REST adapter's 400 malformed-window-start / -end.
func parseWindow(start, end string) (time.Time, time.Time, *problem) {
	windowStart, err := time.Parse(time.RFC3339, start)
	if err != nil {
		return time.Time{}, time.Time{}, problemMalformedTimestamp("malformed-window-start", "windowStart", err)
	}
	windowEnd, err := time.Parse(time.RFC3339, end)
	if err != nil {
		return time.Time{}, time.Time{}, problemMalformedTimestamp("malformed-window-end", "windowEnd", err)
	}
	return windowStart, windowEnd, nil
}

// pathCapacity is the composed read skill: one process path's end-to-end
// capacity in a window, from this context's own registrations. It persists
// nothing.
func (d Deps) pathCapacity(ctx context.Context, rawArgs json.RawMessage) (pathCapacity, *problem) {
	var args pathCapacityArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return pathCapacity{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	if prob := requireArgs(
		namedArg{"pathId", args.PathID}, namedArg{"location", args.Location},
		namedArg{"windowStart", args.WindowStart}, namedArg{"windowEnd", args.WindowEnd},
	); prob != nil {
		return pathCapacity{}, prob
	}
	start, end, prob := parseWindow(args.WindowStart, args.WindowEnd)
	if prob != nil {
		return pathCapacity{}, prob
	}
	result, err := d.GetProcessPathCapacity.Handle(ctx, usecases.GetProcessPathCapacityCommand{
		ProcessPathID:    args.PathID,
		Location:         args.Location,
		WindowStart:      start,
		WindowEnd:        end,
		UnitsPerOrder:    args.UnitsPerOrder,
		PackagesPerOrder: args.PackagesPerOrder,
	})
	if err != nil {
		return pathCapacity{}, problemFromUseCase(err)
	}
	return toPathCapacity(args, start, end, result), nil
}

// evaluateCapacityPlan is the single state-changing delegation: it creates a
// DRAFT capacity plan through the same use case REST and MCP use, so the
// path/window/demand invariants and the transactional outbox (the plan's
// CloudEvents are stored in the same unit of work as the save) behave
// identically regardless of caller. The plan is never published here.
func (d Deps) evaluateCapacityPlan(ctx context.Context, rawArgs json.RawMessage) (capacityPlan, *problem) {
	var args evaluatePlanArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return capacityPlan{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	if prob := requireArgs(
		namedArg{"warehouseId", args.WarehouseID}, namedArg{"siteId", args.SiteID},
		namedArg{"location", args.Location}, namedArg{"pathId", args.PathID},
		namedArg{"windowStart", args.WindowStart}, namedArg{"windowEnd", args.WindowEnd},
	); prob != nil {
		return capacityPlan{}, prob
	}
	start, end, prob := parseWindow(args.WindowStart, args.WindowEnd)
	if prob != nil {
		return capacityPlan{}, prob
	}
	cmd := usecases.CreateCapacityPlanCommand{
		WarehouseID:      args.WarehouseID,
		SiteID:           args.SiteID,
		Location:         args.Location,
		WindowStart:      start,
		WindowEnd:        end,
		ProcessPathID:    args.PathID,
		DemandFromOrders: args.AssignedDemand == nil,
		UnitsPerOrder:    args.UnitsPerOrder,
		PackagesPerOrder: args.PackagesPerOrder,
	}
	if args.AssignedDemand != nil {
		cmd.AssignedDemand = *args.AssignedDemand
	}
	plan, err := d.CreateCapacityPlan.Handle(ctx, cmd)
	if err != nil {
		return capacityPlan{}, problemFromUseCase(err)
	}
	return toCapacityPlan(plan), nil
}
