import { describe, expect, it } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { CapacityPlansScreen } from "./CapacityPlansScreen";
import { json, mockApi, pending, problem } from "../test/fetchMock";
import { PATHS, plan } from "../test/fixtures";

const DRAFT = plan();
const PUBLISHED = plan({
  id: "11111111-2222-3333-4444-555555555555",
  status: "PUBLISHED",
  published_at: "2026-10-04T18:45:10Z",
  shortage: 0,
  assigned_demand: 6000,
  demand_source: "orders",
  location: "SIM2",
  bottleneck_constraint: "STATION",
  bottleneck_step: "PACK",
  created_at: "2026-10-04T16:00:00Z",
  warnings: [WARNING_TEXT()],
});

function WARNING_TEXT() {
  return "PACK has 11 stations tallied but no station standard is declared";
}

function baseRoutes(over: Record<string, Parameters<typeof mockApi>[0][string]> = {}) {
  return mockApi({
    "GET /capacity-plans": json({ capacity_plans: [DRAFT, PUBLISHED] }),
    "GET /process-paths": json({ process_paths: PATHS }),
    ...over,
  });
}

async function fillCreate(
  user: ReturnType<typeof userEvent.setup>,
  o: { warehouse?: string; siteId?: string; site?: string; path?: string; window?: boolean; demand?: string; units?: string; packages?: string } = {},
) {
  const form = await screen.findByRole("form", { name: "Create capacity plan" });
  const f = within(form);
  await user.type(f.getByLabelText(/Warehouse id/), o.warehouse ?? "WH-1");
  await user.type(f.getByLabelText(/^Site id/), o.siteId ?? "SIM1");
  const site = f.getByLabelText(/Plan site code/);
  await user.clear(site);
  await user.type(site, o.site ?? "SIM1");
  if (o.path !== "") await user.selectOptions(f.getByLabelText(/Plan process path/), o.path ?? "pick-rebin-pack");
  if (o.window !== false) {
    fireEvent.change(f.getByLabelText(/Window start/), { target: { value: "2026-10-05T08:00" } });
    fireEvent.change(f.getByLabelText(/Window end/), { target: { value: "2026-10-05T16:00" } });
  }
  if (o.demand) await user.type(f.getByLabelText(/Assigned demand/), o.demand);
  if (o.units) await user.type(f.getByLabelText(/Plan units per order/), o.units);
  if (o.packages) await user.type(f.getByLabelText(/Plan packages per order/), o.packages);
  return f;
}

describe("CapacityPlansScreen list", () => {
  it("shows a loading state", () => {
    baseRoutes({ "GET /capacity-plans": pending() });
    render(<CapacityPlansScreen />);
    expect(screen.getByText("Loading capacity plans…")).toBeInTheDocument();
  });

  it("lists the plans with status pills, shortage, bottleneck and demand source", async () => {
    const api = baseRoutes();
    render(<CapacityPlansScreen />);
    const table = await screen.findByRole("table");
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(2);

    const draft = within(rows[0]);
    expect(draft.getByText("DRAFT")).toHaveAttribute("data-status", "DRAFT");
    expect(draft.getByText("REBIN (LABOR)")).toBeInTheDocument();
    expect(draft.getByText("4,000")).toBeInTheDocument(); // shortage
    expect(draft.getByText("12,000")).toBeInTheDocument(); // demand
    expect(draft.getByText("as stated")).toBeInTheDocument();
    expect(draft.getByText("8,000")).toBeInTheDocument(); // capacity over window
    expect(draft.getByText("2026-10-05 08:00Z → 2026-10-05 16:00Z")).toBeInTheDocument();
    expect(draft.getByRole("button", { name: `Publish plan ${DRAFT.id}` })).toBeInTheDocument();

    const published = within(rows[1]);
    expect(published.getByText("PUBLISHED")).toHaveAttribute("data-status", "PUBLISHED");
    expect(published.getByText("from orders")).toBeInTheDocument();
    expect(published.getByText("PACK (STATION)")).toBeInTheDocument();
    expect(published.getByText(WARNING_TEXT())).toBeInTheDocument();
    expect(published.queryByRole("button", { name: /Publish/ })).not.toBeInTheDocument();

    // no site typed: the unfiltered list is requested (no location param)
    expect(api.to("GET /capacity-plans")[0].query.has("location")).toBe(false);
  });

  it("filters by the typed site", async () => {
    const api = baseRoutes();
    const user = userEvent.setup();
    render(<CapacityPlansScreen />);
    await screen.findByRole("table");
    await user.type(screen.getByLabelText("Site code"), "SIM1");
    await user.click(screen.getByRole("button", { name: "Show plans" }));
    await waitFor(() => expect(api.to("GET /capacity-plans").at(-1)?.query.get("location")).toBe("SIM1"));
  });

  it("shows an empty state", async () => {
    baseRoutes({ "GET /capacity-plans": json({ capacity_plans: [] }) });
    render(<CapacityPlansScreen />);
    expect(await screen.findByText("No capacity plans yet for this selection.")).toBeInTheDocument();
  });

  it("surfaces the problem title and detail when the list fails", async () => {
    baseRoutes({ "GET /capacity-plans": problem(400, "malformed-limit", "limit must be a positive integer", "limit must be a positive integer (at most 100)") });
    render(<CapacityPlansScreen />);
    expect(await screen.findByText("limit must be a positive integer")).toBeInTheDocument();
    expect(screen.getByText("limit must be a positive integer (at most 100)")).toBeInTheDocument();
  });
});

describe("publish", () => {
  const route = `POST /capacity-plans/${DRAFT.id}/publish`;

  it("publishes a DRAFT plan and refreshes the list", async () => {
    let published = false;
    const api = baseRoutes({
      "GET /capacity-plans": () => json({ capacity_plans: [published ? { ...DRAFT, status: "PUBLISHED", published_at: "2026-10-04T18:00:00Z" } : DRAFT] }),
      [route]: () => {
        published = true;
        return json({ ...DRAFT, status: "PUBLISHED" });
      },
    });
    const user = userEvent.setup();
    render(<CapacityPlansScreen />);
    await user.click(await screen.findByRole("button", { name: `Publish plan ${DRAFT.id}` }));
    expect(await screen.findByText(`Plan ${DRAFT.id} is published.`)).toBeInTheDocument();
    await waitFor(() => expect(screen.queryByRole("button", { name: /Publish plan/ })).not.toBeInTheDocument());
    expect(screen.getByText("PUBLISHED")).toBeInTheDocument();
    expect(api.to(route)).toHaveLength(1);
    expect(api.to("GET /capacity-plans")).toHaveLength(2);
  });

  it("treats 409 capacity-plan-already-published as a state, not a failure, and refreshes", async () => {
    const api = baseRoutes({
      [route]: problem(409, "capacity-plan-already-published", "This CapacityPlan has already been published", "capacity plan already published"),
    });
    const user = userEvent.setup();
    render(<CapacityPlansScreen />);
    await user.click(await screen.findByRole("button", { name: `Publish plan ${DRAFT.id}` }));
    expect(await screen.findByText(new RegExp(`Plan ${DRAFT.id} was already published`))).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    await waitFor(() => expect(api.to("GET /capacity-plans")).toHaveLength(2));
  });

  it("shows any other publish failure with its title and detail", async () => {
    baseRoutes({
      [route]: problem(404, "capacity-plan-not-found", "No CapacityPlan exists under this id", "capacity plan not found"),
    });
    const user = userEvent.setup();
    render(<CapacityPlansScreen />);
    await user.click(await screen.findByRole("button", { name: `Publish plan ${DRAFT.id}` }));
    expect(await screen.findByText("No CapacityPlan exists under this id")).toBeInTheDocument();
    expect(screen.getByText("capacity plan not found")).toBeInTheDocument();
  });

  it("disables the publish buttons while one is in flight", async () => {
    baseRoutes({ [route]: pending() });
    const user = userEvent.setup();
    render(<CapacityPlansScreen />);
    await user.click(await screen.findByRole("button", { name: `Publish plan ${DRAFT.id}` }));
    expect(await screen.findByRole("button", { name: `Publish plan ${DRAFT.id}` })).toBeDisabled();
    expect(screen.getByText("Publishing…")).toBeInTheDocument();
  });
});

describe("create form", () => {
  it("creates with an explicit assigned demand, sending it", async () => {
    const created = plan({ id: "new-plan", assigned_demand: 12000 });
    const api = baseRoutes({ "POST /capacity-plans": json(created, 201) });
    const user = userEvent.setup();
    render(<CapacityPlansScreen />);
    const f = await fillCreate(user, { demand: "12000", units: "2.5", packages: "1" });
    await user.click(f.getByRole("button", { name: "Create plan" }));

    expect(await screen.findByText(/Created plan new-plan \(DRAFT\)/)).toBeInTheDocument();
    expect(screen.getByText(/request — 12,000 orders as stated/)).toBeInTheDocument();
    expect(screen.getByText(/Shortage: 4,000 \(bottleneck REBIN\)/)).toBeInTheDocument();
    expect(api.to("POST /capacity-plans")[0].body).toEqual({
      warehouse_id: "WH-1",
      site_id: "SIM1",
      location: "SIM1",
      window_start: "2026-10-05T08:00:00Z",
      window_end: "2026-10-05T16:00:00Z",
      path_id: "pick-rebin-pack",
      assigned_demand: 12000,
      units_per_order: 2.5,
      packages_per_order: 1,
    });
    await waitFor(() => expect(api.to("GET /capacity-plans")).toHaveLength(2));
  });

  it("sends an explicit 0 demand as 0 (not as absent)", async () => {
    const api = baseRoutes({ "POST /capacity-plans": json(plan({ assigned_demand: 0, shortage: 0 }), 201) });
    const user = userEvent.setup();
    render(<CapacityPlansScreen />);
    const f = await fillCreate(user, { demand: "0" });
    await user.click(f.getByRole("button", { name: "Create plan" }));
    await screen.findByText(/Created plan/);
    expect(api.to("POST /capacity-plans")[0].body).toHaveProperty("assigned_demand", 0);
  });

  it("omits assigned_demand when left empty and shows the demand source the service returned", async () => {
    const created = plan({ id: "from-orders", assigned_demand: 7, demand_source: "orders", shortage: 0 });
    const api = baseRoutes({ "POST /capacity-plans": json(created, 201) });
    const user = userEvent.setup();
    render(<CapacityPlansScreen />);
    const f = await fillCreate(user, { units: "2.5" });
    await user.click(f.getByRole("button", { name: "Create plan" }));

    expect(await screen.findByText(/Demand source: orders — defaulted to 7 orders expected in the window/)).toBeInTheDocument();
    const body = api.to("POST /capacity-plans")[0].body as Record<string, unknown>;
    expect(body).not.toHaveProperty("assigned_demand");
    expect(body).not.toHaveProperty("packages_per_order");
    expect(body).toHaveProperty("units_per_order", 2.5);
  });

  it("shows the missing-assigned-demand 422 as a clear message with the problem's title and detail", async () => {
    baseRoutes({
      "POST /capacity-plans": problem(422, "missing-assigned-demand", "assigned_demand is required", "assigned_demand (orders) must be provided"),
    });
    const user = userEvent.setup();
    render(<CapacityPlansScreen />);
    const f = await fillCreate(user);
    await user.click(f.getByRole("button", { name: "Create plan" }));

    expect(await screen.findByText(/There is no order data for this site and window, so the demand could not be defaulted/)).toBeInTheDocument();
    expect(screen.getByText(/Enter an explicit assigned demand, or check Expected demand/)).toBeInTheDocument();
    expect(screen.getByText("assigned_demand is required")).toBeInTheDocument();
    expect(screen.getByText("assigned_demand (orders) must be provided")).toBeInTheDocument();
    expect(screen.queryByText(/Created plan/)).not.toBeInTheDocument();
  });

  it("shows other rejections with title and detail only (no demand guidance)", async () => {
    baseRoutes({
      "POST /capacity-plans": problem(404, "process-path-not-found", "No ProcessPath is registered under this id", 'process path "x" not found'),
    });
    const user = userEvent.setup();
    render(<CapacityPlansScreen />);
    const f = await fillCreate(user, { demand: "10" });
    await user.click(f.getByRole("button", { name: "Create plan" }));
    expect(await screen.findByText("No ProcessPath is registered under this id")).toBeInTheDocument();
    expect(screen.getByText('process path "x" not found')).toBeInTheDocument();
    expect(screen.queryByText(/no order data/)).not.toBeInTheDocument();
  });

  it.each([
    ["warehouse", { warehouse: " " }, "Enter the warehouse id."],
    ["path", { path: "" }, "Choose a process path."],
    ["window", { window: false }, "Pick a window start and end (UTC)."],
    ["negative demand", { demand: "-5" }, "Assigned demand must be zero or more orders, or left empty."],
    ["bad factor", { units: "0" }, "Units per order must be a number above zero."],
  ])("validates before sending (%s)", async (_name, fields, message) => {
    const api = baseRoutes();
    const user = userEvent.setup();
    render(<CapacityPlansScreen />);
    const f = await fillCreate(user, fields);
    await user.click(f.getByRole("button", { name: "Create plan" }));
    expect(await screen.findByText(message)).toBeInTheDocument();
    expect(api.to("POST /capacity-plans")).toHaveLength(0);
  });

  it("explains an empty path catalogue instead of offering a form", async () => {
    baseRoutes({ "GET /process-paths": json({ process_paths: [] }) });
    render(<CapacityPlansScreen />);
    expect(await screen.findByText(/so a plan cannot be created/)).toBeInTheDocument();
    expect(screen.queryByRole("form", { name: "Create capacity plan" })).not.toBeInTheDocument();
  });
});
