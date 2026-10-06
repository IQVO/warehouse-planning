import { NavLink, Outlet, Route, Routes } from "react-router-dom";
import { SiteProvider } from "./SiteProvider";
import { CapacityOverviewScreen } from "./screens/CapacityOverviewScreen";
import { CapacityPlansScreen } from "./screens/CapacityPlansScreen";
import { PathCapacityScreen } from "./screens/PathCapacityScreen";

const SUB_NAV = [
  { to: ".", label: "Overview", end: true },
  { to: "paths", label: "Path capacity" },
  { to: "plans", label: "Capacity plans" },
];

const linkStyle = ({ isActive }: { isActive: boolean }) => ({
  display: "inline-flex",
  padding: "6px 12px",
  borderRadius: "var(--wh-radius-pill)",
  fontSize: "var(--wh-font-size-sm)",
  fontWeight: isActive ? 600 : 500,
  color: isActive ? "var(--wh-color-text)" : "var(--wh-color-text-muted)",
  background: isActive ? "var(--wh-color-accent-muted)" : "transparent",
  textDecoration: "none",
});

/** The shell (nav + shared site state) rendered by a layout route with the
 *  path "/".
 *
 *  Why a layout route and not a nav placed above `<Routes>`: the console mounts
 *  this component inside its own `<Route path="/capacity/*">`. A relative
 *  `<NavLink>` rendered straight in that splat route resolves against the FULL
 *  current URL (react-router 7 uses the leaf match's `pathname`, splat
 *  included), so from /capacity/paths the link `paths` pointed at
 *  /capacity/paths/paths, every hop after the first went to a URL no route
 *  matches, and `to=""` (the current URL) kept Overview active everywhere.
 *
 *  The layout route MUST carry a non-empty path: react-router drops pathless
 *  routes from relative-link resolution (`getPathContributingMatches` keeps a
 *  match only if `route.path.length > 0`), so a pathless layout leaves the
 *  host's splat match as the last contributing one and changes nothing (that
 *  was the first, failed attempt at this fix). With path "/" the layout's own
 *  match is the one links resolve against: the mount point, under any prefix
 *  and standalone at `/`. */
function CapacityLayout() {
  return (
    <SiteProvider>
      <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
        <nav aria-label="Capacity sections" style={{ display: "flex", gap: "var(--wh-space-2)" }}>
          {SUB_NAV.map((item) => (
            <NavLink key={item.to} to={item.to} end={item.end} style={linkStyle}>
              {item.label}
            </NavLink>
          ))}
        </nav>
        <Outlet />
      </div>
    </SiteProvider>
  );
}

/** Exposed as capacity_mfe/App via Module Federation. Takes NO props: the
 *  console mounts it under a route prefix of its choosing (`/<prefix>/*`) and
 *  provides the BrowserRouter, the design tokens and
 *  `window.__WAREHOUSE_CONFIG__`. Routes here are RELATIVE, so the component
 *  works identically mounted under that prefix (in the shell) or at /
 *  (standalone dev, see main.tsx).
 *
 *  Three sub-screens, all for one site at a time:
 *   - Overview (index): storage positions, stations, station standards
 *     (declare/replace) and expected demand.
 *   - Path capacity (paths): normalized rate, bottleneck, per-step breakdown
 *     and warnings of a process path.
 *   - Capacity plans (plans): recent plans, create, publish. */
export default function App() {
  return (
    <Routes>
      <Route path="/" element={<CapacityLayout />}>
        <Route index element={<CapacityOverviewScreen />} />
        <Route path="paths" element={<PathCapacityScreen />} />
        <Route path="plans" element={<CapacityPlansScreen />} />
      </Route>
    </Routes>
  );
}
