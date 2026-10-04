import { NavLink, Route, Routes } from "react-router-dom";
import { SiteProvider } from "./SiteProvider";
import { CapacityOverviewScreen } from "./screens/CapacityOverviewScreen";
import { CapacityPlansScreen } from "./screens/CapacityPlansScreen";
import { PathCapacityScreen } from "./screens/PathCapacityScreen";

const SUB_NAV = [
  { to: "", label: "Overview", end: true },
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
    <SiteProvider>
      <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
        <nav aria-label="Capacity sections" style={{ display: "flex", gap: "var(--wh-space-2)" }}>
          {SUB_NAV.map((item) => (
            <NavLink key={item.to} to={item.to} end={item.end} style={linkStyle}>
              {item.label}
            </NavLink>
          ))}
        </nav>
        <Routes>
          <Route path="/" element={<CapacityOverviewScreen />} />
          <Route path="/paths" element={<PathCapacityScreen />} />
          <Route path="/plans" element={<CapacityPlansScreen />} />
        </Routes>
      </div>
    </SiteProvider>
  );
}
