---
id: tools
title: MCP tools
sidebar_label: MCP tools
---

# MCP tools

`cmd/mcp` is a separate deployable that serves the
[Model Context Protocol](https://modelcontextprotocol.io) over **Streamable
HTTP only** (no stdio, no SSE transport), on `MCP_ADDR` (default `:8090`). The
same handler is mounted at `/` and `/mcp`; `GET /healthz` returns
`{"status":"ok"}` for probes ([ADR 0008](/docs/adr/0008-mcp-server-adoption)).
Server name `warehouse-planning-mcp`; official Go SDK
`github.com/modelcontextprotocol/go-sdk` v1.8.0. **There is no
authentication**; access control is the in-cluster ClusterIP boundary. In the
cluster, `warehouse-ops-agent` is the client (four read-only tools).

The tools call the **same use cases and repositories** as the REST API
(`internal/adapters/inbound/mcp/tools.go`); arguments are snake_case like the
REST bodies. The two plan-writing tools insert their CloudEvents into the
transactional outbox, but `cmd/mcp` never relays them: the relay in `cmd/api`
does (see the [Runbook](/docs/operations/runbook#outbox-relay)).

The input schemas below were read from a live `tools/list` of `cmd/mcp`. The
SDK marks every field without `omitempty` as **required**, which is why
`site_id` is required here while optional numbers are nullable.

## Read tools (`readOnlyHint: true`)

| Tool | Required inputs | Optional inputs | Returns |
| --- | --- | --- | --- |
| `get_effective_process_capacity` | `process_type`, `location`, `window_start`, `window_end` (RFC 3339; must equal the registered window **exactly**) | none | `effective_rate`, `effective_unit`, `binding_constraint`, `constraints[]`; `process-capacity-not-found` when nothing is registered for exactly that key |
| `get_process_path_capacity` | `id`, `location`, `window_start`, `window_end` (a registered window applies when it **covers** the requested one, ADR 0003) | `units_per_order`, `packages_per_order` | `normalized_rate` (ORDER per hour), `normalized_unit`, `bottleneck_step`, `step_breakdown[]` (`step`, `normalized_rate`, `binding_constraint`), `warnings[]` |
| `get_capacity_plan` | `id` | none | the plan, identical to the REST body (`status`, `path_capacity`, `capacity_over_window`, `shortage`, `bottleneck_step`, `bottleneck_constraint`, `site_id`, `demand_source`, `warnings`, ...) |
| `list_station_standards` | none | `location` (site code filter) | `location`, `standards[]` (`location`, `process_type`, `quantity`, `unit`, `period_seconds`) |
| `get_storage_capacity` | `location` (site code, e.g. `SIM1`) | none | `location`, `storage_positions[]` (`zone_id`, `location_type`, `positions`), `stations[]` (`zone_id`, `activity`, `stations`); empty lists when nothing is tallied |
| `get_expected_demand` | `location`, `window_start` (inclusive), `window_end` (exclusive) | none | `orders`, `released_lines` (lines, not units), `source` = `order-management`, `as_of` (null when the site has no data) |

## Write tools (`readOnlyHint: false`, `destructiveHint: true`)

| Tool | `idempotentHint` | Required inputs | Optional inputs | Effect |
| --- | --- | --- | --- | --- |
| `register_process_capacity_constraint` | false | `process_type`, `location`, `window_start`, `window_end`, `constraint_type` (`LABOR`, `LOCATION`, `EQUIPMENT`, `STATION`, `CONVEYOR`, `BUFFER`, `REPLENISHMENT`), `quantity`, `unit` (`UNIT`, `LINE`, `ORDER`, `PACKAGE`), `period_seconds` | none | upserts one constraint (`RegisterProcessCapacityConstraint`); returns the new effective rate and binding constraint. Re-registering a constraint type for the same key **replaces** its rate |
| `register_process_path` | true | `id`, `name`, `steps` (ordered, non-empty) | none | declares or wholesale replaces a ProcessPath (`RegisterProcessPath`) |
| `create_capacity_plan` | false | `warehouse_id`, `site_id`, `location`, `window_start`, `window_end`, `path_id` | `assigned_demand` (orders; omitted = the expected orders, else `missing-assigned-demand`), `units_per_order`, `packages_per_order` | creates a DRAFT plan and queues `CapacityPlanCreated` in the outbox (`CreateCapacityPlan`); every call creates a new plan; increments `warehouse_planning.capacity_plans.created` |
| `publish_capacity_plan` | false | `id` | none | publishes once (`PublishCapacityPlan`) and queues `CapacityPlanPublished`, plus `CapacityShortageDetected` and `BottleneckDetected` when there is a shortage; a second call fails with `capacity-plan-already-published` |
| `declare_station_standard` | true | `location`, `process_type`, `quantity` (> 0), `unit` (`UNIT`, `PACKAGE` or `ORDER`), `period_seconds` | none | upserts a StationStandard (`DeclareStationStandard`); `created` is true for a new one, false when replaced |

Eleven tools in total; `maxTools` in
`internal/adapters/inbound/mcp/governance_test.go` pins the budget at 11.

## Errors

A failing tool returns a normal result with `isError: true` and one text item
`<slug>: <message>`, where the slug is the same one the REST API puts in its
RFC 7807 `type` (see [Troubleshooting](/docs/operations/troubleshooting#rest-problem-types)).
Besides the use-case slugs, the tool handlers return `missing-location`,
`malformed-window-start`, `malformed-window-end`, `non-positive-period` and
`process-capacity-not-found` from their own input checks. An untyped error is logged by the server and returned
as `internal-error: an unexpected internal error occurred`, so database
details never reach the model. A real call to an unknown plan:

```json
{"content":[{"type":"text","text":"capacity-plan-not-found: usecases: no CapacityPlan exists for this id"}],"isError":true}
```

## Calling it by hand

```bash
go run ./cmd/mcp     # in-memory without DATABASE_URL
curl -s -X POST localhost:8090/mcp -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}' -i
# reuse the Mcp-Session-Id response header on every later call:
# notifications/initialized, then tools/list or tools/call
```
