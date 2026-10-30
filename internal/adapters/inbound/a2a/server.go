// Package a2a is the inbound Agent2Agent adapter: it exposes this bounded
// context to the fleet's agent ecosystem as a third driving adapter over the
// same application-layer use cases the REST and MCP adapters use. It is built
// on the official A2A Go SDK (a2aproject/a2a-go) and speaks JSON-RPC 2.0 over
// HTTP, with the Agent Card served at the well-known path.
//
// Per ADR 0018, A2A is the DELEGATION channel between bounded contexts: an
// agent client sends a goal-shaped skill request as a structured message part
// and this agent answers from its own state through its own use cases. It is
// not a data-sync channel -- facts between contexts still travel exclusively
// as CloudEvents on Kafka (ADR 0006), including the events raised by the write
// skill through the transactional outbox (ADR 0007).
//
// Like the MCP adapter (ADR 0008), this package depends inward on the
// application layer (use cases and ports) and the domain only -- never on an
// outbound adapter -- and nothing depends on it. The composition root
// (cmd/a2a) wires concrete repositories into the use cases. Domain structs
// never leak across the message boundary. There is no auth of any kind
// (fleet-wide revert 2026-09-11; TestNoAuthMiddlewareReintroduced).
package a2a

import (
	"context"
	"iter"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// Config carries the deployment-specific identity of this agent: the absolute
// URL clients use to reach its JSON-RPC endpoint (advertised on the Agent
// Card) and the card version. It is supplied by the composition root from
// A2A_AGENT_URL; the card MUST advertise a URL the fleet's agent clients can
// actually dial (in-cluster: http://<release>-a2a.<ns>.svc.cluster.local:8093/).
type Config struct {
	AgentURL string
	Version  string
}

// Deps is everything the A2A skills need, injected by the composition root.
// It carries the SAME use cases the REST and MCP adapters use (the two the MCP
// tools get_process_path_capacity and create_capacity_plan use); the adapter
// never constructs an outbound adapter itself.
type Deps struct {
	// GetProcessPathCapacity is the existing read-time capacity composition
	// (ADR 0002, ADR 0003), reused unchanged. It persists nothing.
	GetProcessPathCapacity *usecases.GetProcessPathCapacity
	// CreateCapacityPlan is the existing write use case, reused unchanged. It
	// saves the DRAFT plan and stores its CapacityPlanCreated CloudEvents in
	// the transactional outbox in ONE unit of work, exactly as REST and MCP.
	CreateCapacityPlan *usecases.CreateCapacityPlan
}

// Card builds this agent's Agent Card (ADR 0018). The surface is two curated,
// goal-shaped skills -- not one skill per endpoint, the same curation
// discipline ADR 0008 applied to MCP tools. path_capacity answers "how much
// can this process path clear in this window, and which step binds it";
// evaluate_capacity_plan is the single state-changing delegation: "evaluate
// the demand assigned to this window against that capacity and record the
// result". Both are derived from tools the MCP surface already offers
// (get_process_path_capacity and create_capacity_plan), so the write gate holds:
// nothing is exposed here that MCP does not already expose. The card declares
// no security schemes on purpose: this surface is unauthenticated by the same
// fleet-wide decision as REST and MCP; schemes are added here, not in a
// redesign, if that decision is ever reversed.
func Card(cfg Config) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "warehouse-planning",
		Description: "Warehouse Planning agent for the Warehouse Execution System: answers whether a registered process path can clear work at a location in a time window (its end-to-end ORDER/HOUR capacity and the bottleneck step) and evaluates the demand assigned to that window against it, recording a DRAFT capacity plan with the shortage. It answers from this context's own capacity registrations and demand read model; it never publishes a plan, registers capacities or reads another context.",
		Version:     cfg.Version,
		Provider: &a2a.AgentProvider{
			Org: "IQVO",
			URL: "https://github.com/IQVO/warehouse-planning",
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(cfg.AgentURL, a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"application/json", "text/plain"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		Capabilities:       a2a.AgentCapabilities{Streaming: false},
		Skills: []a2a.AgentSkill{
			{
				ID:          skillPathCapacity,
				Name:        "Capacity of a process path in a window",
				Description: "Read: the end-to-end capacity of one registered process path at one location for [windowStart, windowEnd) in ORDER per HOUR, the bottleneck step and the constraint binding it, each step's own rate and the composition warnings. A registered capacity window applies when it COVERS the requested window, not only when equal. unitsPerOrder / packagesPerOrder are the workload conversion factors and are required when a step is measured in UNIT / PACKAGE. Windows are RFC3339 timestamps. Persists nothing.",
				Tags:        []string{"planning", "capacity", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"path_capacity","arguments":{"pathId":"tote-path","location":"FC01","windowStart":"2026-10-05T08:00:00Z","windowEnd":"2026-10-05T16:00:00Z","unitsPerOrder":2.5,"packagesPerOrder":1}}`,
				},
			},
			{
				ID:          skillEvaluateCapacityPlan,
				Name:        "Evaluate assigned demand against a path's capacity",
				Description: "Write: evaluates the demand assigned to a window against a process path's capacity and records a DRAFT capacity plan (capacity over the window, shortage, bottleneck), exactly as the REST and MCP create-plan operation does, and stores its CapacityPlanCreated CloudEvents in the transactional outbox. assignedDemand (ORDERS, 0 is a real figure) is optional: when omitted the demand is the number of orders order-management has promised at the location in the window, and the call is rejected (nothing written) when there are none. siteId is the REQUIRED canonical site code. The plan is NOT published by this skill. NOT idempotent: every accepted call records a new plan, so a caller must not blindly retry an ambiguous failure.",
				Tags:        []string{"planning", "capacity-plan", "write"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"evaluate_capacity_plan","arguments":{"warehouseId":"WH-1","siteId":"SIM1","location":"FC01","pathId":"tote-path","windowStart":"2026-10-05T08:00:00Z","windowEnd":"2026-10-05T16:00:00Z","assignedDemand":12000,"unitsPerOrder":2.5,"packagesPerOrder":1}}`,
				},
			},
		},
	}
}

// NewExecutor returns the AgentExecutor that maps an incoming message to a
// skill and answers with a single agent Message (a message-only agent in A2A
// terms): both curated skills are transactional request/response work, so
// there is no long-running Task lifecycle to track in this first A2A
// surface. The response carries the caller's contextId (and taskId when the
// client referenced one) so a delegating saga can group its interactions.
func NewExecutor(deps Deps) a2asrv.AgentExecutor {
	return a2asrv.AgentExecutorFunc(func(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(answer(ctx, deps, ec), nil)
		}
	})
}

// Handler wires the executor into the transport-agnostic A2A request handler
// and wraps it in the JSON-RPC 2.0 HTTP handler -- the only transport this
// agent ships (ADR 0018), mirroring ADR 0008's "Streamable HTTP only"
// minimalism for MCP.
func Handler(executor a2asrv.AgentExecutor) http.Handler {
	return a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
}
