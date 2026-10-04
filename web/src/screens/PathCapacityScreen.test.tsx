import { describe, expect, it } from "vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { PathCapacityScreen } from "./PathCapacityScreen";
import { json, mockApi, pending, problem } from "../test/fetchMock";
import { PATHS, PATH_CAPACITY, WARNING } from "../test/fixtures";

const CAPACITY_ROUTE = "GET /process-paths/pick-rebin-pack/capacity";

async function fillAndCalculate(extra?: { units?: string; packages?: string; skipWindow?: boolean; site?: string }) {
  const user = userEvent.setup();
  render(<PathCapacityScreen />);
  await user.selectOptions(await screen.findByLabelText(/Process path/), "pick-rebin-pack");
  await user.type(screen.getByLabelText(/Site code/), extra?.site ?? "SIM1");
  if (!extra?.skipWindow) {
    fireEvent.change(screen.getByLabelText(/Window start/), { target: { value: "2026-10-05T08:00" } });
    fireEvent.change(screen.getByLabelText(/Window end/), { target: { value: "2026-10-05T16:00" } });
  }
  if (extra?.units) await user.type(screen.getByLabelText(/Units per order/), extra.units);
  if (extra?.packages) await user.type(screen.getByLabelText(/Packages per order/), extra.packages);
  await user.click(screen.getByRole("button", { name: "Calculate capacity" }));
  return user;
}

describe("PathCapacityScreen", () => {
  it("shows a loading state while the process paths load", () => {
    mockApi({ "GET /process-paths": pending() });
    render(<PathCapacityScreen />);
    expect(screen.getByText("Loading process paths…")).toBeInTheDocument();
  });

  it("explains an empty path catalogue instead of showing a dead form", async () => {
    mockApi({ "GET /process-paths": json({ process_paths: [] }) });
    render(<PathCapacityScreen />);
    expect(await screen.findByText(/No process paths are registered yet/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Calculate capacity" })).not.toBeInTheDocument();
  });

  it("surfaces the problem when the paths cannot be loaded", async () => {
    mockApi({ "GET /process-paths": problem(500, "internal-error", "An unexpected internal error occurred", "database unavailable") });
    render(<PathCapacityScreen />);
    expect(await screen.findByText("An unexpected internal error occurred")).toBeInTheDocument();
    expect(screen.getByText("database unavailable")).toBeInTheDocument();
  });

  it("lists the registered paths and shows the chosen path's steps", async () => {
    mockApi({ "GET /process-paths": json({ process_paths: PATHS }) });
    const user = userEvent.setup();
    render(<PathCapacityScreen />);
    const select = await screen.findByLabelText(/Process path/);
    expect(within(select).getByRole("option", { name: "Pick-Rebin-Pack (pick-rebin-pack)" })).toBeInTheDocument();
    expect(within(select).getByRole("option", { name: "Pick-Pack (pick-pack)" })).toBeInTheDocument();
    await user.selectOptions(select, "pick-rebin-pack");
    expect(screen.getByText("Steps: PICK → REBIN → PACK")).toBeInTheDocument();
  });

  it("shows the normalized rate, the bottleneck step and the per-step breakdown", async () => {
    const api = mockApi({
      "GET /process-paths": json({ process_paths: PATHS }),
      [CAPACITY_ROUTE]: json(PATH_CAPACITY),
    });
    await fillAndCalculate({ units: "2.5", packages: "1" });

    const rate = (await screen.findByText("Normalized rate")).closest("[data-state]") as HTMLElement;
    expect(within(rate).getByText("1,800")).toBeInTheDocument();
    expect(within(rate).getByText("ORDER per hour")).toBeInTheDocument();
    const bottleneck = screen.getByText("Bottleneck step").closest("[data-state]") as HTMLElement;
    expect(within(bottleneck).getByText("PACK")).toBeInTheDocument();
    expect(within(bottleneck).getByText("bound by STATION")).toBeInTheDocument();

    const table = screen.getByRole("table");
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(3);
    expect(within(rows[0]).getByText("PICK")).toBeInTheDocument();
    expect(within(rows[0]).getByText("3,200")).toBeInTheDocument();
    expect(within(rows[0]).getByText("LABOR")).toBeInTheDocument();
    expect(within(rows[2]).getByText("STATION")).toBeInTheDocument();
    expect(within(rows[2]).getByText("Bottleneck")).toBeInTheDocument();
    expect(within(rows[0]).queryByText("Bottleneck")).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Warnings" })).not.toBeInTheDocument();

    const call = api.to(CAPACITY_ROUTE)[0];
    expect(Object.fromEntries(call.query)).toEqual({
      location: "SIM1",
      window_start: "2026-10-05T08:00:00Z",
      window_end: "2026-10-05T16:00:00Z",
      units_per_order: "2.5",
      packages_per_order: "1",
    });
  });

  it("shows every warning string visibly", async () => {
    const second = "REBIN has 4 stations tallied but no station standard is declared";
    mockApi({
      "GET /process-paths": json({ process_paths: PATHS }),
      [CAPACITY_ROUTE]: json({ ...PATH_CAPACITY, warnings: [WARNING, second] }),
    });
    await fillAndCalculate();
    const region = await screen.findByRole("region", { name: "Warnings" });
    expect(within(region).getByText("Warnings (2)")).toBeInTheDocument();
    expect(within(region).getByText(WARNING)).toBeVisible();
    expect(within(region).getByText(second)).toBeVisible();
  });

  it("omits the optional factors from the request when left empty", async () => {
    const api = mockApi({
      "GET /process-paths": json({ process_paths: PATHS }),
      [CAPACITY_ROUTE]: json(PATH_CAPACITY),
    });
    await fillAndCalculate();
    await screen.findByText("Normalized rate");
    const query = api.to(CAPACITY_ROUTE)[0].query;
    expect(query.has("units_per_order")).toBe(false);
    expect(query.has("packages_per_order")).toBe(false);
  });

  it("shows a loading state while the capacity is computed", async () => {
    mockApi({ "GET /process-paths": json({ process_paths: PATHS }), [CAPACITY_ROUTE]: pending() });
    await fillAndCalculate();
    expect(await screen.findByText("Loading path capacity…")).toBeInTheDocument();
  });

  it("surfaces a 422 problem's title and detail (a step with no capacity)", async () => {
    mockApi({
      "GET /process-paths": json({ process_paths: PATHS }),
      [CAPACITY_ROUTE]: problem(
        422,
        "missing-step-capacity",
        "No registered ProcessCapacity window covers one of the path's steps at this location and window",
        'no capacity for step "PACK" at location SIM1',
      ),
    });
    await fillAndCalculate();
    expect(await screen.findByText(/No registered ProcessCapacity window covers/)).toBeInTheDocument();
    expect(screen.getByText('no capacity for step "PACK" at location SIM1')).toBeInTheDocument();
    expect(screen.getByText("(HTTP 422)")).toBeInTheDocument();
    expect(screen.queryByText("Normalized rate")).not.toBeInTheDocument();
  });

  it.each([
    ["no path", async (u: ReturnType<typeof userEvent.setup>) => {
      await u.type(screen.getByLabelText(/Site code/), "SIM1");
    }, "Choose a process path."],
    ["no site", async (u: ReturnType<typeof userEvent.setup>) => {
      await u.selectOptions(screen.getByLabelText(/Process path/), "pick-pack");
    }, "Enter a site code."],
    ["no window", async (u: ReturnType<typeof userEvent.setup>) => {
      await u.selectOptions(screen.getByLabelText(/Process path/), "pick-pack");
      await u.type(screen.getByLabelText(/Site code/), "SIM1");
    }, "Pick a window start and end (UTC)."],
  ])("validates before calling the service (%s)", async (_name, setup, message) => {
    const api = mockApi({ "GET /process-paths": json({ process_paths: PATHS }) });
    const user = userEvent.setup();
    render(<PathCapacityScreen />);
    await screen.findByLabelText(/Process path/);
    await setup(user);
    await user.click(screen.getByRole("button", { name: "Calculate capacity" }));
    expect(await screen.findByText(message)).toBeInTheDocument();
    expect(api.calls.filter((c) => c.path.endsWith("/capacity"))).toHaveLength(0);
  });

  it("rejects a non-positive conversion factor client-side", async () => {
    const api = mockApi({ "GET /process-paths": json({ process_paths: PATHS }), [CAPACITY_ROUTE]: json(PATH_CAPACITY) });
    await fillAndCalculate({ units: "0" });
    expect(await screen.findByText("Units per order must be a number above zero.")).toBeInTheDocument();
    expect(api.to(CAPACITY_ROUTE)).toHaveLength(0);
  });
});
