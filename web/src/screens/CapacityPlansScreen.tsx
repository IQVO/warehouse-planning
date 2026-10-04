import { useState } from "react";
import { Card, DataTable, StatusPill, formatNumber } from "@warehouse/ui-kit";
import { createCapacityPlan, listCapacityPlans, listProcessPaths, publishCapacityPlan, toApiError } from "../api";
import type { ApiError } from "../api";
import { Form, FormRow, InlineError, InlineSuccess, SelectField, SubmitButton, TextField } from "../components/formkit";
import { PageHeader, Stack, WindowFields } from "../components/layout";
import { ProblemAlert, RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import { checkWindow, optionalNumber, utcLabel } from "../lib";
import { useSiteLocation } from "../siteContext";
import type { CapacityPlan, CreateCapacityPlanInput } from "../types";

const PLAN_PROBLEM_LEADS: Record<string, string> = {
  "missing-assigned-demand":
    "There is no order data for this site and window, so the demand could not be defaulted. " +
    "Enter an explicit assigned demand, or check Expected demand on the Capacity overview.",
};

/**
 * Capacity plans: the most recent plans (GET /capacity-plans), a form to
 * create one (POST /capacity-plans) and a Publish action on DRAFT plans
 * (POST /capacity-plans/{id}/publish).
 *
 * `assigned_demand` is optional on create: left empty, the service defaults it
 * from the order-management read model and the plan says so via demand_source.
 */
export function CapacityPlansScreen() {
  const [location, setLocation] = useSiteLocation();
  const [filter, setFilter] = useState(location.trim());
  const plans = useRequest(() => listCapacityPlans(filter), `plans|${filter}`);

  return (
    <Stack gap={5}>
      <PageHeader
        title="Capacity plans"
        subtitle="warehouse-planning · assigned demand against path capacity: shortage, bottleneck, publish"
      />
      <Card title="Recent plans">
        <Stack>
          <Form label="Plan filter" onSubmit={() => setFilter(location.trim())}>
            <FormRow>
              <TextField label="Site code" value={location} onChange={setLocation} hint="Empty shows the latest plans of every site" />
              <SubmitButton>Show plans</SubmitButton>
            </FormRow>
          </Form>
          <PlansTable plans={plans} onChanged={plans.reload} />
        </Stack>
      </Card>
      <Card title="Create a plan">
        <CreatePlanForm defaultLocation={location} onCreated={plans.reload} />
      </Card>
    </Stack>
  );
}

function PlansTable({
  plans,
  onChanged,
}: {
  plans: ReturnType<typeof useRequest<CapacityPlan[]>>;
  onChanged: () => void;
}) {
  const [publishing, setPublishing] = useState<string | null>(null);
  const [problem, setProblem] = useState<ApiError | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const publish = async (plan: CapacityPlan) => {
    setProblem(null);
    setNotice(null);
    setPublishing(plan.id);
    try {
      await publishCapacityPlan(plan.id);
      setNotice(`Plan ${plan.id} is published.`);
    } catch (err) {
      const error = toApiError(err);
      if (error.slug === "capacity-plan-already-published") {
        setNotice(`Plan ${plan.id} was already published; the list now shows its current state.`);
      } else {
        setProblem(error);
      }
    } finally {
      setPublishing(null);
      onChanged();
    }
  };

  return (
    <Stack>
      {problem && <ProblemAlert error={problem} />}
      <InlineSuccess message={notice} />
      <RequestView
        state={plans}
        what="capacity plans"
        isEmpty={(p) => p.length === 0}
        empty="No capacity plans yet for this selection."
      >
        {(list) => (
          <DataTable
            rowKey={(p) => p.id}
            rows={list}
            columns={[
              { key: "created", header: "Created", render: (p) => utcLabel(p.created_at) },
              { key: "site", header: "Site", render: (p) => p.location },
              { key: "path", header: "Path", render: (p) => p.path_id },
              { key: "window", header: "Window (UTC)", render: (p) => `${utcLabel(p.window_start)} → ${utcLabel(p.window_end)}` },
              {
                key: "demand",
                header: "Demand",
                align: "right",
                render: (p) => (
                  <span>
                    {formatNumber(p.assigned_demand)}{" "}
                    <small style={{ color: "var(--wh-color-text-muted)" }}>
                      {p.demand_source === "orders" ? "from orders" : "as stated"}
                    </small>
                  </span>
                ),
              },
              { key: "capacity", header: "Capacity over window", align: "right", render: (p) => formatNumber(p.capacity_over_window) },
              {
                key: "shortage",
                header: "Shortage",
                align: "right",
                render: (p) => (
                  <span style={{ color: p.shortage > 0 ? "var(--wh-color-status-danger)" : undefined, fontWeight: p.shortage > 0 ? 600 : undefined }}>
                    {formatNumber(p.shortage)}
                  </span>
                ),
              },
              {
                key: "bottleneck",
                header: "Bottleneck",
                render: (p) => (p.bottleneck_constraint ? `${p.bottleneck_step} (${p.bottleneck_constraint})` : p.bottleneck_step),
              },
              {
                key: "warnings",
                header: "Warnings",
                render: (p) =>
                  p.warnings.length === 0 ? (
                    "—"
                  ) : (
                    <ul style={{ margin: 0, paddingLeft: 16 }}>
                      {p.warnings.map((w, i) => (
                        <li key={`${i}|${w}`}>{w}</li>
                      ))}
                    </ul>
                  ),
              },
              {
                key: "status",
                header: "Status",
                render: (p) => <StatusPill status={p.status} tone={p.status === "PUBLISHED" ? "success" : "progress"} size="sm" />,
              },
              {
                key: "actions",
                header: "",
                render: (p) =>
                  p.status === "DRAFT" ? (
                    <SubmitButton type="button" ariaLabel={`Publish plan ${p.id}`} disabled={publishing !== null} onClick={() => void publish(p)}>
                      {publishing === p.id ? "Publishing…" : "Publish"}
                    </SubmitButton>
                  ) : null,
              },
            ]}
          />
        )}
      </RequestView>
    </Stack>
  );
}

function CreatePlanForm({ defaultLocation, onCreated }: { defaultLocation: string; onCreated: () => void }) {
  const paths = useRequest(listProcessPaths, "paths");
  const [warehouseId, setWarehouseId] = useState("");
  const [location, setLocation] = useState(defaultLocation);
  const [pathId, setPathId] = useState("");
  const [windowStart, setWindowStart] = useState("");
  const [windowEnd, setWindowEnd] = useState("");
  const [demand, setDemand] = useState("");
  const [units, setUnits] = useState("");
  const [packages, setPackages] = useState("");
  const [invalid, setInvalid] = useState<string | null>(null);
  const [problem, setProblem] = useState<ApiError | null>(null);
  const [created, setCreated] = useState<CapacityPlan | null>(null);
  const [saving, setSaving] = useState(false);

  const submit = async () => {
    setProblem(null);
    setCreated(null);
    const window = checkWindow(windowStart, windowEnd);
    const assigned = optionalNumber(demand);
    const unitsPerOrder = optionalNumber(units);
    const packagesPerOrder = optionalNumber(packages);
    if (!warehouseId.trim()) return setInvalid("Enter the warehouse id.");
    if (!location.trim()) return setInvalid("Enter a site code.");
    if (!pathId) return setInvalid("Choose a process path.");
    if (!window.ok) return setInvalid(window.message);
    if (assigned !== undefined && !(Number.isFinite(assigned) && assigned >= 0))
      return setInvalid("Assigned demand must be zero or more orders, or left empty.");
    if (unitsPerOrder !== undefined && !(unitsPerOrder > 0)) return setInvalid("Units per order must be a number above zero.");
    if (packagesPerOrder !== undefined && !(packagesPerOrder > 0))
      return setInvalid("Packages per order must be a number above zero.");
    setInvalid(null);

    const input: CreateCapacityPlanInput = {
      warehouse_id: warehouseId.trim(),
      location: location.trim(),
      window_start: window.start,
      window_end: window.end,
      path_id: pathId,
      // Omitted (not 0) when empty: the service then defaults it from orders.
      ...(assigned !== undefined && { assigned_demand: assigned }),
      ...(unitsPerOrder !== undefined && { units_per_order: unitsPerOrder }),
      ...(packagesPerOrder !== undefined && { packages_per_order: packagesPerOrder }),
    };
    setSaving(true);
    try {
      setCreated(await createCapacityPlan(input));
      onCreated();
    } catch (err) {
      setProblem(toApiError(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <RequestView
      state={paths}
      what="process paths"
      isEmpty={(p) => p.length === 0}
      empty="No process paths are registered yet, so a plan cannot be created. A path is registered with POST /process-paths."
    >
      {(list) => (
        <Form label="Create capacity plan" onSubmit={submit}>
          <FormRow>
            <TextField label="Warehouse id" value={warehouseId} onChange={setWarehouseId} required />
            <TextField label="Plan site code" value={location} onChange={setLocation} required />
            <SelectField
              label="Plan process path"
              value={pathId}
              onChange={setPathId}
              options={list.map((p) => ({ value: p.id, label: `${p.name} (${p.id})` }))}
              required
            />
          </FormRow>
          <WindowFields start={windowStart} end={windowEnd} onStart={setWindowStart} onEnd={setWindowEnd} required />
          <FormRow>
            <TextField
              label="Assigned demand (orders)"
              type="number"
              min={0}
              step="any"
              value={demand}
              onChange={setDemand}
              hint="Optional. Leave empty to use the orders order-management expects in the window."
            />
            <TextField label="Plan units per order" type="number" min={0} step="any" value={units} onChange={setUnits} />
            <TextField label="Plan packages per order" type="number" min={0} step="any" value={packages} onChange={setPackages} />
            <SubmitButton disabled={saving}>{saving ? "Creating…" : "Create plan"}</SubmitButton>
          </FormRow>
          <InlineError message={invalid} />
          {problem && <ProblemAlert error={problem} lead={PLAN_PROBLEM_LEADS[problem.slug]} />}
          {created && <CreatedPlan plan={created} />}
        </Form>
      )}
    </RequestView>
  );
}

function CreatedPlan({ plan }: { plan: CapacityPlan }) {
  const fromOrders = plan.demand_source === "orders";
  return (
    <InlineSuccess
      message={
        <span>
          Created plan {plan.id} ({plan.status}). Demand source:{" "}
          {fromOrders
            ? `orders — defaulted to ${formatNumber(plan.assigned_demand)} orders expected in the window`
            : `request — ${formatNumber(plan.assigned_demand)} orders as stated`}
          . Shortage: {formatNumber(plan.shortage)} (bottleneck {plan.bottleneck_step}).
          {plan.warnings.length > 0 && ` Warnings: ${plan.warnings.join("; ")}`}
        </span>
      }
    />
  );
}
