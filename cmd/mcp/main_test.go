package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// newTestServer wires the same use cases buildDeps does, over the in-memory
// TEST DOUBLES (the shipped binary has no in-memory mode: ADR 0016), behind
// the real router. The Postgres wiring is covered by the integration test.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	pc, plans, outboxRepo := memory.NewProcessCapacityRepo(), memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	tally := memory.NewStorageTallyRepo()
	deps, err := newDeps(repositories{
		processCapacities: pc, processPaths: memory.NewProcessPathRepo(), standards: memory.NewStationStandardRepo(),
		tally: tally, plans: plans, outbox: outboxRepo, demand: memory.NewOrderDemandRepo(),
		uow: memory.NewUnitOfWork(pc, plans, outboxRepo),

		inboundAppointments: memory.NewInboundAppointmentRepo(), inboundStandards: memory.NewInboundStandardRepo(),
		observedCapacity: memory.NewObservedCapacityRepo(),
	})
	if err != nil {
		t.Fatalf("newDeps: %v", err)
	}
	srv := httptest.NewServer(newRouter(inboundmcp.Handler(inboundmcp.NewServer(deps))))
	t.Cleanup(srv.Close)
	return srv
}

// There is no in-memory mode: an empty DATABASE_URL is a boot error naming it.
func TestBuildDeps_RefusesAnEmptyDatabaseURL(t *testing.T) {
	_, _, err := buildDeps(context.Background(), quietLogger(), "", "", defaultMigrationsPath)
	if !errors.Is(err, errMissingDatabaseURL) || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("buildDeps(\"\") = %v, want errMissingDatabaseURL naming DATABASE_URL", err)
	}
}

func TestRouter_HealthzIsOpenAndServesOK(t *testing.T) {
	srv := newTestServer(t)
	// No Authorization header: /healthz is open.
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != `{"status":"ok"}` {
		t.Fatalf("GET /healthz = %d %q, want 200 {\"status\":\"ok\"}", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

// Both mount points speak Streamable HTTP, with no credentials, and a real
// client can list the 14 tools and call one end to end.
func TestStreamableHTTP_RootAndMCPPathsServeTools(t *testing.T) {
	srv := newTestServer(t)
	for _, path := range []string{"/", "/mcp"} {
		t.Run(path, func(t *testing.T) {
			client := sdk.NewClient(&sdk.Implementation{Name: "t", Version: "0"}, nil)
			session, err := client.Connect(context.Background(), &sdk.StreamableClientTransport{Endpoint: srv.URL + path}, nil)
			if err != nil {
				t.Fatalf("connect %s: %v", path, err)
			}
			defer session.Close()

			tools, err := session.ListTools(context.Background(), nil)
			if err != nil {
				t.Fatalf("list tools: %v", err)
			}
			if len(tools.Tools) != 14 {
				t.Fatalf("tools = %d, want 14", len(tools.Tools))
			}

			res, err := session.CallTool(context.Background(), &sdk.CallToolParams{
				Name: "register_process_path", Arguments: map[string]any{"id": "p" + strings.ReplaceAll(path, "/", ""), "name": "n", "steps": []string{"PICK"}},
			})
			if err != nil || res.IsError {
				t.Fatalf("register_process_path: err=%v res=%+v", err, res)
			}
		})
	}
}

// The binary reads no API key: requests carrying (or lacking) any
// Authorization header behave identically.
func TestNoAuthIsRequired(t *testing.T) {
	srv := newTestServer(t)
	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`
	for _, auth := range []string{"", "Bearer nonsense"} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("initialize with Authorization=%q = %d, want 200", auth, resp.StatusCode)
		}
	}
}

func TestGetenv(t *testing.T) {
	t.Setenv("WP_MCP_TEST_SET", "v")
	t.Setenv("WP_MCP_TEST_EMPTY", "")
	if got := getenv("WP_MCP_TEST_SET", "d"); got != "v" {
		t.Errorf("set: %q", got)
	}
	if got := getenv("WP_MCP_TEST_EMPTY", "d"); got != "d" {
		t.Errorf("empty: %q", got)
	}
}

func TestNewLoggerLevels(t *testing.T) {
	for level, want := range map[string]slog.Level{"debug": slog.LevelDebug, "WARN": slog.LevelWarn, "warning": slog.LevelWarn, "error": slog.LevelError, "": slog.LevelInfo, "bogus": slog.LevelInfo} {
		if got := newLogger(level); !got.Handler().Enabled(context.Background(), want) || (want > slog.LevelDebug && got.Handler().Enabled(context.Background(), want-1)) {
			t.Errorf("newLogger(%q): level %v not the minimum enabled", level, want)
		}
	}
}
