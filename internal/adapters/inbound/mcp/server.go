// Package mcp is the inbound Model Context Protocol adapter: it exposes this
// bounded context to the AI ecosystem as a second driving adapter over the
// same application-layer use cases the REST adapter uses. It is built on the
// official MCP Go SDK and served over Streamable HTTP only.
//
// Per ADR-0008 this package depends inward on the application layer (use
// cases and ports) and the domain only -- never on an outbound adapter or the
// REST adapter -- and nothing else may depend on it (internal/architecture's
// TestMCPAdapterDependencyRule). The composition root (cmd/mcp) wires
// concrete repositories into the use cases. There is no auth of any kind
// (fleet-wide revert 2026-09-11; TestNoAuthMiddlewareReintroduced).
package mcp

import (
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// serverName and serverVersion identify this server in the MCP initialize
// handshake.
const (
	serverName    = "warehouse-planning-mcp"
	serverVersion = "1.0.0"
)

// NewServer builds the MCP server for this bounded context with every tool
// registered.
func NewServer(deps Deps) *mcp.Server {
	server := mcp.NewServer(
		&mcp.Implementation{Name: serverName, Version: serverVersion},
		&mcp.ServerOptions{
			Instructions: "Warehouse capacity planning: register process capacity constraints " +
				"(register_process_capacity_constraint) and read the effective rate " +
				"(get_effective_process_capacity); declare a process path (register_process_path) and read " +
				"its end-to-end ORDER/HOUR capacity and bottleneck step (get_process_path_capacity); evaluate " +
				"assigned demand against it with create_capacity_plan (DRAFT), then publish_capacity_plan " +
				"(once only) and read it back with get_capacity_plan. Read the orders order-management expects at a site " +
				"in a window with get_expected_demand (create_capacity_plan uses it when assigned_demand is omitted). Every step of a path needs a capacity " +
				"registered for EXACTLY the same location and [window_start, window_end).",
		},
	)

	deps.registerTools(server)

	return server
}

// Handler returns the Streamable HTTP handler for the MCP server.
func Handler(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
}
