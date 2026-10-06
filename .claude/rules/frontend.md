---
paths:
  - "web/**"
---

# Frontend micro-frontend remote (`web/`)

This repo also owns `web/`: `capacity-mfe`, a Vite + React + TypeScript **Module
Federation remote** consumed by the separate `warehouse-console` shell repo
(fleet MFE console architecture; ADR-0011 in `workforce-management`). It is a
plain browser client of this service's own REST API: nothing in `web/` talks to
any other bounded context, and nothing in `internal/` knows `web/` exists.

- Federation container `capacity_mfe` (NOT `planning_mfe`: that is
  `wes-work-planning`'s). Exposes ONE module, `./App` (`src/App.tsx`, default
  export, **no props**): the shell mounts it under a route prefix of its choosing
  and provides the `BrowserRouter`, the design tokens and
  `window.__WAREHOUSE_CONFIG__`. Its routes are relative: `/` (capacity
  overview), `/paths` (path capacity), `/plans` (capacity plans).
- Dev/preview port **5190** (`vite --port 5190`, strictPort). Production base
  `/mfes/warehouse-planning/`; context segment `warehouse-planning`.
- API base = `${apiOrigin}/api/warehouse-planning` (`src/config.ts`); a
  production build throws when `apiOrigin` is missing, the `window === undefined`
  guard keeps the module importable in tests and with `/config.json` absent.
- Own `package.json`, build and tests. Does **not** participate in the Go quality
  gate and is not part of the Go module: `make check`/`check-all` never touch it.
  CI's `web` job runs `npm run lint`, `npx tsc -b`, `npm test`, `npm run build`
  after building the sibling `warehouse-ui-kit`.
- Requires `CORS_ALLOWED_ORIGINS` to include the remote's origin only for a
  direct-to-pod caller (standalone dev on `:5190`); in the cluster Kong's global
  CORS plugin answers browsers.

## Rules that each cost real time elsewhere in the fleet

- **`web/vite.config.ts` stays in OBJECT form.** `vitest.config.ts` does
  `mergeConfig(viteConfig, ...)` and Vite throws "Cannot merge config in form of
  callback" on a function export, killing the whole test suite. The deployment
  base is derived statically: `IS_BUILD = process.argv.includes("build")`,
  `base = IS_BUILD ? "/mfes/warehouse-planning/" : "/"`.
- **A small relative `dist/remoteEntry.js` is correct** (chunks resolve relative
  to where `remoteEntry.js` was fetched; `dist/index.html` carries the absolute
  prefixed URLs). Do not force an absolute public path.
- **The Docker build cannot use the committed lockfile or a host `node_modules`**
  (missing linux native bindings, npm/cli#4828): base `node:22-bookworm-slim`,
  `web/.dockerignore` excluding `node_modules`/`dist`, `rm -rf node_modules
  package-lock.json` in the ui-kit stage, `COPY package.json ./` only then
  `npm install --no-save`. `@warehouse/ui-kit` arrives as the named build context
  `uikit` (`docker build --build-context uikit=../../warehouse-ui-kit ...`).
- **Use only `@warehouse/ui-kit`** components and tokens (StatusPill, Card,
  DataTable, KpiStat...). Do not add to the kit from here. A status the kit does
  not know (`DRAFT`, `PUBLISHED`, `Bottleneck`) is rendered with StatusPill's
  `tone` override rather than a hand-rolled colour.
- **No Dependabot `npm` entry for `/web`**: it depends on `file:../../warehouse-ui-kit`
  (outside the repo), which Dependabot cannot fetch, so the job would fail weekly
  (`scripts/harness/repo_lint.py` R3 enforces it).
- Tests mock `fetch` (`src/test/fetchMock.ts`: an unmocked request REJECTS, so a
  surprise call fails the test). Every screen has loading, success, empty and
  error/problem tests; an RFC 7807 failure must surface BOTH `title` and `detail`.
- **Relative links must come from a layout route WITH a path, and tests must
  mount the remote under the host's splat route.** The shell mounts `App` in
  `<Route path="/capacity/*">`. react-router 7 resolves a relative link against
  the last *path-contributing* match, and that match's `pathname` includes the
  splat, so a nav rendered above `<Routes>` (or in a pathless layout route,
  which react-router drops from resolution) pointed `paths` at
  `/capacity/paths/paths` and kept "Overview" active on every screen. `App.tsx`
  therefore renders the nav from `<Route path="/" element={<CapacityLayout/>}>`.
  `App.test.tsx` has a `mountUnderHost` helper for exactly this: a test that
  mounts `App` at the router root cannot see this class of bug (the original
  suite passed while the live console was broken).

## Contract the screens depend on (read `rest-api.md` for the shapes)

| Screen | Reads | Writes |
|---|---|---|
| Capacity overview (`/`) | `GET /storage-capacity`, `GET /station-standards`, `GET /demand` | `PUT /station-standards/{location}/{process_type}` |
| Path capacity (`/paths`) | `GET /process-paths`, `GET /process-paths/{id}/capacity` | none |
| Capacity plans (`/plans`) | `GET /capacity-plans`, `GET /process-paths` | `POST /capacity-plans`, `POST /capacity-plans/{id}/publish` |

Behaviours that are contract, not taste: no site is ever assumed (the site field
starts empty); a `GET /demand` answer with `as_of: null` is rendered as **no
data**, never as 0; every `warnings` string of a path capacity is shown;
`assigned_demand` is omitted from `POST /capacity-plans` when the field is empty
(an explicit `0` is sent as `0`) and the returned `demand_source` is displayed;
`422 missing-assigned-demand` gets guidance text plus the problem's title/detail;
`409 capacity-plan-already-published` is treated as "already published" and the
list is refreshed. Window pickers are labelled and read as **UTC**.
