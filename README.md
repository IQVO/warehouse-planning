# warehouse-planning

Warehouse Planning — a Core bounded context in the `warehouse-systems`
fleet. Answers: **can this warehouse process the demand assigned to it**,
given its current labor, location, equipment, station, conveyor and
buffer constraints? This is distinct from Inventory Management ("what do
we have") and from every existing fleet context, none of which currently
computes normalized, cross-process effective capacity or forward-looking
capacity shortages.

See `CLAUDE.md` for the full repo guide, `.claude/rules/domain-model.md`
for the ubiquitous language/aggregates, and
`docs/adr/0001-warehouse-planning-bounded-context.md` for why this context
exists and its context map.

Scaffolded from `warehouse-harness-template: v2`. See `HARNESS.md` for the
sensor manifest.

## Study project

This repo, like the rest of the `warehouse-systems` fleet, is a personal
study project exploring Domain-Driven Design, hexagonal architecture, and
AI-agent harness engineering. It is not production software and carries
no support guarantee.

## Deployment

- **Image**: root `Dockerfile` builds every `cmd/*` directory (`api` and `mcp`) into `/app/<name>`; `ENTRYPOINT` is `./api`. Migrations are copied to `/app/migrations`.
- **Chart**: `charts/warehouse-planning` (OLTP `api` component; optional `mcp` component, off by default). It refuses to render without `database.url` or `database.existingSecret`. Run `helm lint charts/warehouse-planning --set database.url=postgres://u@example.invalid:5432/db` and `python3 charts/warehouse-planning/tests/test_service_selectors.py`.
- **MCP**: `cmd/mcp` serves 7 tools over Streamable HTTP on `:8090` (`/` and `/mcp`, `GET /healthz`, no auth); see `.claude/rules/mcp.md`. Enable in the chart with `mcp.enabled=true`.
- **Kind cluster**: wired by `warehouse-infra` (`local.services`); ArgoCD deploys the chart from this repo's `develop`.
- **Not deployed yet (tracked deferrals)**: analytics projector/reports (no analytics stream yet) and a frontend remote (no `web/` yet).
