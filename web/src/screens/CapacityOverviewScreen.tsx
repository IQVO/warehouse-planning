import { useState } from "react";
import { Card, DataTable, KpiStat, formatNumber } from "@warehouse/ui-kit";
import { getDemand, getStationStandards, getStorageCapacity, putStationStandard, toApiError } from "../api";
import type { ApiError } from "../api";
import { Form, FormRow, InlineSuccess, SelectField, SubmitButton, TextField, InlineError } from "../components/formkit";
import { PageHeader, Stack, WindowFields } from "../components/layout";
import { EmptyNote, ProblemAlert, RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import { checkWindow, perHour, utcLabel } from "../lib";
import { useSiteLocation } from "../siteContext";
import type { ExpectedDemand, StandardUnit, StationStandard } from "../types";

const UNIT_OPTIONS: { value: StandardUnit; label: string }[] = [
  { value: "UNIT", label: "UNIT" },
  { value: "PACKAGE", label: "PACKAGE" },
  { value: "ORDER", label: "ORDER" },
];

interface Applied {
  location: string;
  windowStart: string;
  windowEnd: string;
}

/**
 * Capacity overview of ONE SITE (the `location` the operator types; nothing is
 * assumed): the storage positions and stations facility-layout registered
 * (GET /storage-capacity), the operator-declared throughput of one station per
 * process (GET /station-standards, PUT to declare or replace one), and the
 * orders order-management expects in a window (GET /demand).
 *
 * Read-mostly; the only write is the station standard form.
 */
export function CapacityOverviewScreen() {
  const [location, setLocation] = useSiteLocation();
  const [windowStart, setWindowStart] = useState("");
  const [windowEnd, setWindowEnd] = useState("");
  const [applied, setApplied] = useState<Applied | null>(null);

  const load = () => setApplied({ location: location.trim(), windowStart, windowEnd });

  return (
    <Stack gap={5}>
      <PageHeader
        title="Capacity overview"
        subtitle="warehouse-planning · storage, stations, station standards and expected demand of one site"
      />
      <Card title="Site and window">
        <Form label="Site and window" onSubmit={load}>
          <FormRow>
            <TextField
              label="Site code"
              value={location}
              onChange={setLocation}
              placeholder="e.g. a site/building code"
              required
            />
          </FormRow>
          <WindowFields start={windowStart} end={windowEnd} onStart={setWindowStart} onEnd={setWindowEnd} />
          <FormRow>
            <SubmitButton>Load</SubmitButton>
          </FormRow>
        </Form>
      </Card>

      {!applied || applied.location === "" ? (
        <EmptyNote>Enter a site code and press Load to see its capacity.</EmptyNote>
      ) : (
        <SiteCapacity applied={applied} />
      )}
    </Stack>
  );
}

function SiteCapacity({ applied }: { applied: Applied }) {
  const { location } = applied;
  return (
    <Stack gap={5}>
      <StorageSection location={location} />
      <StandardsSection location={location} />
      <DemandSection applied={applied} />
    </Stack>
  );
}

function SectionTitle({ children }: { children: string }) {
  return <h3 style={{ margin: "0 0 var(--wh-space-2)", fontSize: "var(--wh-font-size-sm)" }}>{children}</h3>;
}

function StorageSection({ location }: { location: string }) {
  const state = useRequest(() => getStorageCapacity(location), `storage|${location}`);
  return (
    <Card title={`Storage and stations — ${location}`}>
      <RequestView state={state} what="storage capacity" empty="">
        {(c) => (
          <Stack gap={5}>
            <div>
              <SectionTitle>Storage positions per zone and location type</SectionTitle>
              {c.storage_positions.length === 0 ? (
                <EmptyNote>No storage positions are tallied for this site.</EmptyNote>
              ) : (
                <DataTable
                  rowKey={(r) => `${r.zone_id}|${r.location_type}`}
                  rows={c.storage_positions}
                  columns={[
                    { key: "zone", header: "Zone", render: (r) => r.zone_id },
                    { key: "type", header: "Location type", render: (r) => r.location_type },
                    { key: "positions", header: "Positions", align: "right", render: (r) => formatNumber(r.positions) },
                  ]}
                />
              )}
            </div>
            <div>
              <SectionTitle>Stations per zone and activity</SectionTitle>
              {c.stations.length === 0 ? (
                <EmptyNote>No stations are tallied for this site.</EmptyNote>
              ) : (
                <DataTable
                  rowKey={(r) => `${r.zone_id}|${r.activity}`}
                  rows={c.stations}
                  columns={[
                    { key: "zone", header: "Zone", render: (r) => r.zone_id },
                    { key: "activity", header: "Activity", render: (r) => r.activity },
                    { key: "stations", header: "Stations", align: "right", render: (r) => formatNumber(r.stations) },
                  ]}
                />
              )}
            </div>
          </Stack>
        )}
      </RequestView>
    </Card>
  );
}

function StandardsSection({ location }: { location: string }) {
  const state = useRequest(() => getStationStandards(location), `standards|${location}`);
  const [prefill, setPrefill] = useState<StationStandard | null>(null);

  return (
    <Card title={`Station standards — ${location}`}>
      <Stack>
        <RequestView
          state={state}
          what="station standards"
          isEmpty={(s) => s.length === 0}
          empty="No station standard is declared for this site yet. Without one, a step's stations add no capacity."
        >
          {(standards) => (
            <DataTable
              rowKey={(s) => s.process_type}
              rows={standards}
              onRowClick={setPrefill}
              columns={[
                { key: "process", header: "Process", render: (s) => s.process_type },
                { key: "qty", header: "Per station", align: "right", render: (s) => `${formatNumber(s.quantity)} ${s.unit}` },
                { key: "period", header: "Period (s)", align: "right", render: (s) => formatNumber(s.period_seconds) },
                { key: "hour", header: "Per hour", align: "right", render: (s) => `${formatNumber(perHour(s.quantity, s.period_seconds))} ${s.unit}/h` },
              ]}
            />
          )}
        </RequestView>
        <StandardForm location={location} prefill={prefill} onSaved={state.reload} />
      </Stack>
    </Card>
  );
}

function StandardForm({
  location,
  prefill,
  onSaved,
}: {
  location: string;
  prefill: StationStandard | null;
  onSaved: () => void;
}) {
  const [processType, setProcessType] = useState("");
  const [quantity, setQuantity] = useState("");
  const [unit, setUnit] = useState<StandardUnit | "">("");
  const [period, setPeriod] = useState("3600");
  const [problem, setProblem] = useState<ApiError | null>(null);
  const [invalid, setInvalid] = useState<string | null>(null);
  const [done, setDone] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  // Clicking a standard row loads it into the form to be replaced. Adjusting
  // state while rendering (rather than in an effect) avoids a one-frame flash
  // of the previous values.
  const [seen, setSeen] = useState<StationStandard | null>(null);
  if (prefill !== seen) {
    setSeen(prefill);
    if (prefill) {
      setProcessType(prefill.process_type);
      setQuantity(String(prefill.quantity));
      setUnit((UNIT_OPTIONS.find((o) => o.value === prefill.unit)?.value ?? "") as StandardUnit | "");
      setPeriod(String(prefill.period_seconds));
    }
  }

  const submit = async () => {
    setProblem(null);
    setDone(null);
    const process = processType.trim().toUpperCase();
    const qty = Number(quantity);
    const periodSeconds = Number(period);
    if (!process) return setInvalid("Enter the process type (e.g. PACK).");
    if (quantity.trim() === "" || !Number.isFinite(qty) || qty <= 0)
      return setInvalid("Quantity per station must be a number above zero.");
    if (!unit) return setInvalid("Choose the unit the process is measured in.");
    if (!Number.isFinite(periodSeconds) || periodSeconds <= 0) return setInvalid("Period must be a number of seconds above zero.");
    setInvalid(null);
    setSaving(true);
    try {
      const res = await putStationStandard(location, process, { quantity: qty, unit, period_seconds: periodSeconds });
      setDone(
        `${res.status === 201 ? "Declared" : "Updated"} the ${res.data.process_type} standard for ${res.data.location}: ` +
          `${formatNumber(res.data.quantity)} ${res.data.unit} per station per ${formatNumber(res.data.period_seconds)} s.`,
      );
      onSaved();
    } catch (err) {
      setProblem(toApiError(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Form label="Declare station standard" onSubmit={submit}>
      <div style={{ fontSize: "var(--wh-font-size-sm)", fontWeight: 600 }}>
        Declare or replace a station standard for {location}
      </div>
      <FormRow>
        <TextField label="Process type" value={processType} onChange={setProcessType} placeholder="PACK" required />
        <TextField label="Quantity per station" type="number" min={0} step="any" value={quantity} onChange={setQuantity} required />
        <SelectField label="Unit" value={unit} onChange={(v) => setUnit(v as StandardUnit)} options={UNIT_OPTIONS} required />
        <TextField label="Period (seconds)" type="number" min={0} step="any" value={period} onChange={setPeriod} required />
        <SubmitButton disabled={saving}>{saving ? "Saving…" : "Save standard"}</SubmitButton>
      </FormRow>
      <InlineError message={invalid} />
      {problem && <ProblemAlert error={problem} />}
      <InlineSuccess message={done} />
    </Form>
  );
}

function DemandSection({ applied }: { applied: Applied }) {
  const check = checkWindow(applied.windowStart, applied.windowEnd);
  const state = useRequest(
    check.ok ? () => getDemand(applied.location, check.start, check.end) : null,
    check.ok ? `demand|${applied.location}|${check.start}|${check.end}` : "demand|none",
  );
  return (
    <Card title={`Expected demand — ${applied.location}`}>
      {!check.ok ? (
        <EmptyNote>{check.reason === "missing" ? "Pick a window start and end (UTC) to see the orders expected in it." : check.message}</EmptyNote>
      ) : (
        <RequestView state={state} what="expected demand" empty="">
          {(demand) => <DemandFigures demand={demand} />}
        </RequestView>
      )}
    </Card>
  );
}

/** An empty read model (as_of null) is "no data" -- never rendered as zero. */
function DemandFigures({ demand }: { demand: ExpectedDemand }) {
  const known = demand.as_of !== null;
  return (
    <Stack>
      {!known && (
        <EmptyNote>
          No data: order-management has reported no order for this site, so expected demand is unknown (not zero).
        </EmptyNote>
      )}
      <div style={{ display: "flex", gap: "var(--wh-space-4)", flexWrap: "wrap" }}>
        <KpiStat
          label="Orders"
          value={known ? demand.orders : null}
          state={known ? "ok" : "no-data"}
          caption={`expected in ${utcLabel(demand.window_start)} → ${utcLabel(demand.window_end)}`}
        />
        <KpiStat
          label="Released lines"
          value={known ? demand.released_lines : null}
          state={known ? "ok" : "no-data"}
          caption="lines (not units)"
        />
        <KpiStat label="As of" value={known ? utcLabel(demand.as_of) : null} state={known ? "ok" : "no-data"} caption={`source: ${demand.source}`} />
      </div>
    </Stack>
  );
}
