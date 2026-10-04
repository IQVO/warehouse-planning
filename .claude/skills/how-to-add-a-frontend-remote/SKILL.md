---
name: how-to-add-a-frontend-remote
description: Add or change a screen in warehouse-planning's web/ micro-frontend remote (capacity_mfe: vite federation config in object form, /mfes/warehouse-planning/ base, remoteEntry, Docker/nginx packaging, chart frontend component, console integration). Use when touching web/ or the chart's frontend component.
---

# How to add a frontend remote screen

Use when adding or changing a screen in `web/`, this repo's Module Federation
remote (`capacity-mfe`, federation container `capacity_mfe`) that
`warehouse-console` lazy-loads. Read `.claude/rules/frontend.md` first: it holds
the contract (names, ports, the no-props `./App` expose) and the traps.

## Add a screen

1. Backend first: the endpoint must exist and be in `apis/openapi.yaml` (see
   `how-to-add-a-rest-endpoint`). Copy request/response field names from
   `internal/adapters/inbound/http/dto.go`, never from memory; mirror them in
   `web/src/types.ts` (snake_case on the wire).
2. Add the client call to `web/src/api.ts` (every failure becomes an `ApiError`
   with the RFC 7807 `title`/`detail`/`slug`) and test its URL, verb and body in
   `api.test.ts`.
3. Write the screen in `web/src/screens/`. Fetch with `useRequest` (keeps the
   problem body, drops stale responses) and render with `RequestView` so loading,
   error (title AND detail), empty and success are all handled in one place. Use
   only `@warehouse/ui-kit` components and `var(--wh-*)` tokens. An empty read
   model is "no data", never `0`.
4. Route it relatively in `web/src/App.tsx` (`<Route path="/thing" ...>`): the
   shell mounts the app under a prefix of its choosing.
5. Test every state with `mockApi` (`web/src/test/fetchMock.ts`): loading
   (`pending()`), success, empty, 4xx/5xx `problem(...)`, a network failure, plus
   validation that must NOT send a request.

## Gates (all in `web/`)

```bash
npm install                # sibling ../../warehouse-ui-kit must be checked out and built
npm run lint               # oxlint
npm run typecheck          # tsc -b
npm test                   # vitest run: confirm your test file is actually picked up
npm run build              # tsc -b && vite build -> dist/
```

CI's `web` job (`.github/workflows/ci.yml`) checks this repo out as
`warehouse-planning/` next to `warehouse-ui-kit/` (develop), builds the kit, then
runs `npm ci`, lint, `npx tsc -b`, `npm test`, `npm run build`. If you change
dependencies, commit the regenerated `web/package-lock.json`.

## `web/vite.config.ts` stays in OBJECT form, always

`vitest.config.ts` does `mergeConfig(viteConfig, ...)`; Vite throws on a
function export and the whole suite dies. Derive the base statically
(`process.argv.includes("build")`), as the file's own comment explains. A tiny
relative `dist/remoteEntry.js` is correct: do not grep it for the prefix and do
not force an absolute public path; verify through the real gateway path.

## Packaging and the chart

- `web/Dockerfile`, `web/nginx.conf`, `web/.dockerignore` follow the fleet
  recipe (node:22-bookworm-slim, no committed lockfile or host `node_modules` in
  the image, `uikit` named build context, nginx-unprivileged on 8080, `/healthz`,
  SPA fallback, `no-cache` on `remoteEntry.js`/`index.html`, one
  `Cache-Control` on `/assets/`).
  `docker build --build-context uikit=../../warehouse-ui-kit -t warehouse/warehouse-planning-frontend:local web`.
- The chart's `frontend` component (`charts/warehouse-planning/templates/frontend-*.yaml`,
  values `frontend.*`, default `enabled: false`) is its own Deployment + ClusterIP
  Service with `app.kubernetes.io/component: frontend`. No ingress/HTTPRoute for
  it: path routing belongs to warehouse-infra's Nginx web gateway. After any
  chart change run `helm lint`, `helm template` with `frontend.enabled=true` and
  `mcp.enabled=true`, and `python3 charts/warehouse-planning/tests/test_service_selectors.py`
  (each Service must select exactly one Deployment). The default render must stay
  byte-identical when the component is off.
- Wiring into the console and into `warehouse-infra` (`local.services`, the
  gateway's `/mfes/warehouse-planning/` route, content-hashed image tag) happens
  in THOSE repos, as separate PRs: remotes are consumed at runtime, never built
  together with the shell.
