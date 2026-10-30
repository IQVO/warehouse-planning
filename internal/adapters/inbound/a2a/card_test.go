package a2a

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

const testAgentURL = "http://warehouse-planning-a2a.warehouse-systems.svc.cluster.local:8093/"

// TestCardShape pins the identity, interface and capability surface of the
// Agent Card: the discovery contract other contexts program against.
func TestCardShape(t *testing.T) {
	card := Card(Config{AgentURL: testAgentURL, Version: "1.0.0"})

	if card.Name != "warehouse-planning" {
		t.Errorf("card name = %q, want warehouse-planning", card.Name)
	}
	if len(card.SupportedInterfaces) != 1 {
		t.Fatalf("card has %d interfaces, want 1", len(card.SupportedInterfaces))
	}
	iface := card.SupportedInterfaces[0]
	if iface.URL != testAgentURL || iface.ProtocolBinding != a2a.TransportProtocolJSONRPC {
		t.Errorf("interface = %+v, want JSONRPC at the configured agent URL", iface)
	}
	if card.Capabilities.Streaming {
		t.Error("streaming must not be advertised: this is a message-only agent")
	}
	if card.SecuritySchemes != nil {
		t.Error("card must declare no security schemes: the surface is unauthenticated fleet-wide")
	}
	if card.Provider == nil || card.Provider.Org != "IQVO" {
		t.Errorf("provider = %+v, want IQVO", card.Provider)
	}
}

// TestCardSkillsAreCuratedAndTruthful pins the two-skill surface and proves
// the card never lies about its own contract: every advertised example must
// parse under the SAME strict decoder the executor uses, name its own skill,
// and carry arguments that skill accepts.
func TestCardSkillsAreCuratedAndTruthful(t *testing.T) {
	card := Card(Config{AgentURL: testAgentURL, Version: "1.0.0"})

	tags := map[string]string{skillPathCapacity: "read", skillEvaluateCapacityPlan: "write"}
	if len(card.Skills) != len(tags) {
		t.Fatalf("card has %d skills, want exactly %d (curated, goal-shaped -- not one per endpoint)", len(card.Skills), len(tags))
	}
	for _, s := range card.Skills {
		wantTag, known := tags[s.ID]
		if !known {
			t.Errorf("unexpected skill %q on the card", s.ID)
			continue
		}
		if s.Name == "" || s.Description == "" || len(s.Examples) == 0 {
			t.Errorf("skill %q lacks a name, description or example", s.ID)
		}
		if !containsTag(s.Tags, wantTag) {
			t.Errorf("skill %q must be tagged %q so a delegating agent can tell reads from writes, tags = %v", s.ID, wantTag, s.Tags)
		}
		for _, ex := range s.Examples {
			var req skillRequest
			if err := decodeStrict([]byte(ex), &req); err != nil {
				t.Errorf("skill %q example is not valid under the strict decoder: %v", s.ID, err)
				continue
			}
			if req.Skill != s.ID {
				t.Errorf("skill %q example names skill %q", s.ID, req.Skill)
			}
			assertArgsAccepted(t, s.ID, req.Arguments)
		}
	}
}

func assertArgsAccepted(t *testing.T, skill string, raw json.RawMessage) {
	t.Helper()
	var err error
	switch skill {
	case skillPathCapacity:
		var args pathCapacityArgs
		if err = decodeStrict(raw, &args); err == nil {
			err = requireAll(args.PathID, args.Location, args.WindowStart, args.WindowEnd)
		}
	case skillEvaluateCapacityPlan:
		var args evaluatePlanArgs
		if err = decodeStrict(raw, &args); err == nil {
			err = requireAll(args.WarehouseID, args.SiteID, args.Location, args.PathID, args.WindowStart, args.WindowEnd)
		}
	}
	if err != nil {
		t.Errorf("skill %q example arguments are rejected by its own contract: %v", skill, err)
	}
}

func requireAll(values ...string) error {
	for _, v := range values {
		if strings.TrimSpace(v) == "" {
			return errBlankExampleArgument
		}
	}
	return nil
}

var errBlankExampleArgument = errors.New("an example omits a required argument")

func containsTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

// TestCardOffersOnlyTheCamelCaseVocabulary: the REST and MCP surfaces speak
// snake_case (window_start, assigned_demand, ...). The A2A card must not teach
// those names, or an agent following the card would be refused as an unknown
// argument.
func TestCardOffersOnlyTheCamelCaseVocabulary(t *testing.T) {
	raw, err := json.Marshal(Card(Config{AgentURL: testAgentURL, Version: "1.0.0"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range snakeCaseKeys {
		if strings.Contains(string(raw), name) {
			t.Errorf("the Agent Card mentions %q, a name this surface does not accept", name)
		}
	}
}

// TestCardDoesNotPromiseWhatTheGateForbids pins the write gate: the card
// advertises no skill beyond the two derived from existing MCP tools, and its
// description does not claim publishing or registration.
func TestCardDoesNotPromiseWhatTheGateForbids(t *testing.T) {
	card := Card(Config{AgentURL: testAgentURL, Version: "1.0.0"})
	for _, s := range card.Skills {
		if s.ID != skillPathCapacity && s.ID != skillEvaluateCapacityPlan {
			t.Errorf("skill %q is not derived from an MCP tool", s.ID)
		}
	}
	if !strings.Contains(card.Description, "never publishes") {
		t.Error("the card description must say the agent never publishes a plan")
	}
}

// TestCardSerializable proves the card marshals to JSON a client can parse
// back, with the discovery fields intact.
func TestCardSerializable(t *testing.T) {
	raw, err := json.Marshal(Card(Config{AgentURL: testAgentURL, Version: "1.0.0"}))
	if err != nil {
		t.Fatalf("marshal card: %v", err)
	}
	var back a2a.AgentCard
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("card JSON does not round-trip: %v", err)
	}
	if back.Name != "warehouse-planning" || len(back.Skills) != 2 {
		t.Errorf("round-tripped card = %q with %d skills", back.Name, len(back.Skills))
	}
}
