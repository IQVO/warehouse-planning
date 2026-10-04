import { useState } from "react";
import { Card, DataTable, KpiStat, StatusPill, formatNumber } from "@warehouse/ui-kit";
import { getPathCapacity, listProcessPaths } from "../api";
import { Form, FormRow, InlineError, SelectField, SubmitButton, TextField } from "../components/formkit";
import { PageHeader, Stack, WindowFields } from "../components/layout";
import { EmptyNote, RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import { checkWindow, optionalNumber } from "../lib";
import { useSiteLocation } from "../siteContext";
import type { PathCapacityQuery, ProcessPathCapacity } from "../types";

/**
 * Path capacity: how many ORDERs per hour a process path can move at a site
 * over a window, which step is the bottleneck and why. Picks a path from
 * GET /process-paths, then calls GET /process-paths/{id}/capacity.
 *
 * The response's `warnings` carry real operator guidance (e.g. a step has
 * stations tallied but no station standard declared, so they add no
 * capacity): every one is rendered, above the table, never collapsed.
 */
export function PathCapacityScreen() {
  const paths = useRequest(listProcessPaths, "paths");
  const [location, setLocation] = useSiteLocation();
  const [pathId, setPathId] = useState("");
  const [windowStart, setWindowStart] = useState("");
  const [windowEnd, setWindowEnd] = useState("");
  const [units, setUnits] = useState("");
  const [packages, setPackages] = useState("");
  const [invalid, setInvalid] = useState<string | null>(null);
  const [query, setQuery] = useState<PathCapacityQuery | null>(null);

  const submit = () => {
    const window = checkWindow(windowStart, windowEnd);
    const unitsPerOrder = optionalNumber(units);
    const packagesPerOrder = optionalNumber(packages);
    if (!pathId) return setInvalid("Choose a process path.");
    if (!location.trim()) return setInvalid("Enter a site code.");
    if (!window.ok) return setInvalid(window.message);
    if (unitsPerOrder !== undefined && !(unitsPerOrder > 0)) return setInvalid("Units per order must be a number above zero.");
    if (packagesPerOrder !== undefined && !(packagesPerOrder > 0))
      return setInvalid("Packages per order must be a number above zero.");
    setInvalid(null);
    setQuery({ pathId, location: location.trim(), windowStart: window.start, windowEnd: window.end, unitsPerOrder, packagesPerOrder });
  };

  const selected = paths.status === "success" ? paths.data.find((p) => p.id === pathId) : undefined;

  return (
    <Stack gap={5}>
      <PageHeader
        title="Path capacity"
        subtitle="warehouse-planning · normalized end-to-end rate of a process path, its bottleneck and the binding constraint of every step"
      />
      <Card title="Path, site and window">
        <RequestView
          state={paths}
          what="process paths"
          isEmpty={(p) => p.length === 0}
          empty="No process paths are registered yet. A path is registered with POST /process-paths."
        >
          {(list) => (
            <Form label="Path capacity query" onSubmit={submit}>
              <FormRow>
                <SelectField
                  label="Process path"
                  value={pathId}
                  onChange={setPathId}
                  options={list.map((p) => ({ value: p.id, label: `${p.name} (${p.id})` }))}
                  required
                />
                <TextField label="Site code" value={location} onChange={setLocation} required />
              </FormRow>
              {selected && (
                <div style={{ fontSize: "var(--wh-font-size-sm)", color: "var(--wh-color-text-muted)" }}>
                  Steps: {selected.steps.join(" → ")}
                </div>
              )}
              <WindowFields start={windowStart} end={windowEnd} onStart={setWindowStart} onEnd={setWindowEnd} required />
              <FormRow>
                <TextField
                  label="Units per order"
                  type="number"
                  min={0}
                  step="any"
                  value={units}
                  onChange={setUnits}
                  hint="Needed when a step is measured in UNIT"
                />
                <TextField
                  label="Packages per order"
                  type="number"
                  min={0}
                  step="any"
                  value={packages}
                  onChange={setPackages}
                  hint="Needed when a step is measured in PACKAGE"
                />
                <SubmitButton>Calculate capacity</SubmitButton>
              </FormRow>
              <InlineError message={invalid} />
            </Form>
          )}
        </RequestView>
      </Card>

      {query && <PathCapacityResult query={query} />}
    </Stack>
  );
}

function PathCapacityResult({ query }: { query: PathCapacityQuery }) {
  const state = useRequest(
    () => getPathCapacity(query),
    `capacity|${query.pathId}|${query.location}|${query.windowStart}|${query.windowEnd}|${query.unitsPerOrder ?? ""}|${query.packagesPerOrder ?? ""}`,
  );
  return (
    <Card title={`Capacity of ${query.pathId} at ${query.location}`}>
      <RequestView state={state} what="path capacity" empty="">
        {(capacity) => <CapacityBreakdown capacity={capacity} />}
      </RequestView>
    </Card>
  );
}

function CapacityBreakdown({ capacity }: { capacity: ProcessPathCapacity }) {
  const bottleneck = capacity.step_breakdown.find((s) => s.step === capacity.bottleneck_step);
  const rows = capacity.step_breakdown.map((item, i) => ({ ...item, key: `${i}|${item.step}` }));
  return (
    <Stack gap={5}>
      <div style={{ display: "flex", gap: "var(--wh-space-4)", flexWrap: "wrap" }}>
        <KpiStat label="Normalized rate" value={capacity.normalized_rate} caption={`${capacity.normalized_unit} per hour`} />
        <KpiStat
          label="Bottleneck step"
          value={capacity.bottleneck_step}
          caption={bottleneck ? `bound by ${bottleneck.binding_constraint}` : undefined}
          tone="warning"
        />
      </div>

      {capacity.warnings.length > 0 && (
        <div
          role="region"
          aria-label="Warnings"
          style={{
            color: "var(--wh-color-status-warning)",
            background: "var(--wh-color-status-warning-bg)",
            borderRadius: "var(--wh-radius-md)",
            padding: "10px 14px",
            fontSize: "var(--wh-font-size-sm)",
          }}
        >
          <strong>Warnings ({capacity.warnings.length})</strong>
          <ul style={{ margin: "4px 0 0", paddingLeft: 18 }}>
            {capacity.warnings.map((w, i) => (
              <li key={`${i}|${w}`}>{w}</li>
            ))}
          </ul>
        </div>
      )}

      <DataTable
        rowKey={(r) => r.key}
        rows={rows}
        emptyState={<EmptyNote>The path reported no steps.</EmptyNote>}
        columns={[
          {
            key: "step",
            header: "Step",
            render: (r) => (
              <span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                {r.step}
                {r.step === capacity.bottleneck_step && <StatusPill status="Bottleneck" tone="warning" size="sm" />}
              </span>
            ),
          },
          {
            key: "rate",
            header: `Normalized rate (${capacity.normalized_unit}/h)`,
            align: "right",
            render: (r) => formatNumber(r.normalized_rate),
          },
          { key: "binding", header: "Binding constraint", render: (r) => r.binding_constraint },
        ]}
      />
    </Stack>
  );
}
