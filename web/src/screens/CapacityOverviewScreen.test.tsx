import { describe, expect, it } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { CapacityOverviewScreen } from "./CapacityOverviewScreen";
import { json, mockApi, pending, problem } from "../test/fetchMock";
import { DEMAND, EMPTY_STORAGE, NO_DEMAND_DATA, PACK_STANDARD, STORAGE } from "../test/fixtures";

function routes(over: Record<string, Parameters<typeof mockApi>[0][string]> = {}) {
  return mockApi({
    "GET /storage-capacity": json(STORAGE),
    "GET /station-standards": json({ location: "SIM1", standards: [PACK_STANDARD] }),
    "GET /demand": json(DEMAND),
    ...over,
  });
}

async function loadSite(site = "SIM1", window = true) {
  const user = userEvent.setup();
  render(<CapacityOverviewScreen />);
  await user.type(screen.getByLabelText(/Site code/), site);
  if (window) {
    fireEvent.change(screen.getByLabelText(/Window start/), { target: { value: "2026-10-05T08:00" } });
    fireEvent.change(screen.getByLabelText(/Window end/), { target: { value: "2026-10-05T16:00" } });
  }
  await user.click(screen.getByRole("button", { name: "Load" }));
  return user;
}

describe("CapacityOverviewScreen", () => {
  it("starts empty: no site is assumed and nothing is fetched", () => {
    const api = routes();
    render(<CapacityOverviewScreen />);
    expect(screen.getByLabelText(/Site code/)).toHaveValue("");
    expect(screen.getByText(/Enter a site code and press Load/)).toBeInTheDocument();
    expect(api.calls).toHaveLength(0);
  });

  it("does not fetch for a blank site code", async () => {
    const api = routes();
    const user = userEvent.setup();
    render(<CapacityOverviewScreen />);
    await user.type(screen.getByLabelText(/Site code/), "   ");
    await user.click(screen.getByRole("button", { name: "Load" }));
    expect(screen.getByText(/Enter a site code and press Load/)).toBeInTheDocument();
    expect(api.calls).toHaveLength(0);
  });

  it("shows a loading state while the site is being fetched", async () => {
    routes({
      "GET /storage-capacity": pending(),
      "GET /station-standards": pending(),
      "GET /demand": pending(),
    });
    await loadSite();
    expect(await screen.findByText("Loading storage capacity…")).toBeInTheDocument();
    expect(screen.getByText("Loading station standards…")).toBeInTheDocument();
    expect(screen.getByText("Loading expected demand…")).toBeInTheDocument();
  });

  it("renders storage positions, stations, standards and demand for the typed site", async () => {
    const api = routes();
    await loadSite();

    expect(await screen.findByText("SIM1-STOR-AMB")).toBeInTheDocument();
    expect(screen.getByText("SimShelf")).toBeInTheDocument();
    expect(screen.getByText("1,200")).toBeInTheDocument();
    expect(screen.getByText("SIM1-OPS-WC")).toBeInTheDocument();
    expect(screen.getByText("11")).toBeInTheDocument();

    // standards: per-station throughput and the derived hourly rate
    expect(await screen.findByText("180 PACKAGE")).toBeInTheDocument();
    expect(screen.getByText("180 PACKAGE/h")).toBeInTheDocument();

    // demand: orders, released lines, as_of
    const orders = (await screen.findByText("Orders")).closest("[data-state]") as HTMLElement;
    expect(orders).toHaveAttribute("data-state", "ok");
    expect(within(orders).getByText("3")).toBeInTheDocument();
    const lines = screen.getByText("Released lines").closest("[data-state]") as HTMLElement;
    expect(within(lines).getByText("6")).toBeInTheDocument();
    expect(screen.getByText("2026-10-04 09:15Z")).toBeInTheDocument();

    expect(api.to("GET /storage-capacity")[0].query.get("location")).toBe("SIM1");
    const demandCall = api.to("GET /demand")[0];
    expect(demandCall.query.get("window_start")).toBe("2026-10-05T08:00:00Z");
    expect(demandCall.query.get("window_end")).toBe("2026-10-05T16:00:00Z");
  });

  it("shows empty states for a site with nothing tallied or declared", async () => {
    routes({
      "GET /storage-capacity": json(EMPTY_STORAGE),
      "GET /station-standards": json({ location: "NEW1", standards: [] }),
    });
    await loadSite("NEW1");
    expect(await screen.findByText("No storage positions are tallied for this site.")).toBeInTheDocument();
    expect(screen.getByText("No stations are tallied for this site.")).toBeInTheDocument();
    expect(screen.getByText(/No station standard is declared for this site yet/)).toBeInTheDocument();
  });

  it("asks for a window instead of fetching demand without one", async () => {
    const api = routes();
    await loadSite("SIM1", false);
    expect(await screen.findByText(/Pick a window start and end \(UTC\)/)).toBeInTheDocument();
    expect(api.to("GET /demand")).toHaveLength(0);
    expect(api.to("GET /storage-capacity")).toHaveLength(1);
  });

  it("refuses an inverted window client-side", async () => {
    const api = routes();
    const user = userEvent.setup();
    render(<CapacityOverviewScreen />);
    await user.type(screen.getByLabelText(/Site code/), "SIM1");
    fireEvent.change(screen.getByLabelText(/Window start/), { target: { value: "2026-10-05T16:00" } });
    fireEvent.change(screen.getByLabelText(/Window end/), { target: { value: "2026-10-05T08:00" } });
    await user.click(screen.getByRole("button", { name: "Load" }));
    expect(await screen.findByText("The window end must be after the window start.")).toBeInTheDocument();
    expect(api.to("GET /demand")).toHaveLength(0);
  });

  it("an empty demand model is 'no data', never zero", async () => {
    routes({ "GET /demand": json(NO_DEMAND_DATA) });
    await loadSite();
    expect(await screen.findByText(/No data: order-management has reported no order/)).toBeInTheDocument();
    for (const label of ["Orders", "Released lines", "As of"]) {
      const kpi = screen.getByText(label).closest("[data-state]") as HTMLElement;
      expect(kpi).toHaveAttribute("data-state", "no-data");
      expect(within(kpi).getByText("—")).toBeInTheDocument();
      expect(within(kpi).queryByText("0")).not.toBeInTheDocument();
    }
  });

  it("zero orders WITH an as_of is real data and shows 0", async () => {
    routes({ "GET /demand": json({ ...DEMAND, orders: 0, released_lines: 0 }) });
    await loadSite();
    const orders = (await screen.findByText("Orders")).closest("[data-state]") as HTMLElement;
    expect(orders).toHaveAttribute("data-state", "ok");
    expect(within(orders).getByText("0")).toBeInTheDocument();
    expect(screen.queryByText(/No data:/)).not.toBeInTheDocument();
  });

  it("surfaces the problem title and detail when a read fails, section by section", async () => {
    routes({
      "GET /storage-capacity": problem(400, "missing-location", "location is required", "the location (site code) query parameter must be provided"),
      "GET /demand": problem(400, "invalid-capacity-window", "Capacity window end must be strictly after start", "capacity window end must be strictly after start"),
    });
    await loadSite();
    expect(await screen.findByText("location is required")).toBeInTheDocument();
    expect(screen.getByText("the location (site code) query parameter must be provided")).toBeInTheDocument();
    expect(screen.getByText("Capacity window end must be strictly after start")).toBeInTheDocument();
    // the standards read still succeeded and is shown
    expect(await screen.findByText("180 PACKAGE")).toBeInTheDocument();
  });

  it("shows a network failure as an alert", async () => {
    routes({ "GET /station-standards": () => Promise.reject(new TypeError("Failed to fetch")) });
    await loadSite();
    expect(await screen.findByText("Network error")).toBeInTheDocument();
    expect(screen.getByText("Failed to fetch")).toBeInTheDocument();
  });
});

describe("station standard form", () => {
  async function fillStandard(user: ReturnType<typeof userEvent.setup>, fields: { process?: string; qty?: string; unit?: string; period?: string }) {
    const form = await screen.findByRole("form", { name: "Declare station standard" });
    const f = within(form);
    if (fields.process !== undefined) await user.type(f.getByLabelText(/Process type/), fields.process);
    if (fields.qty !== undefined) await user.type(f.getByLabelText(/Quantity per station/), fields.qty);
    if (fields.unit !== undefined) await user.selectOptions(f.getByLabelText(/Unit/), fields.unit);
    if (fields.period !== undefined) {
      await user.clear(f.getByLabelText(/Period/));
      await user.type(f.getByLabelText(/Period/), fields.period);
    }
    return f;
  }

  it("declares a standard (201), sends the exact body and refreshes the table", async () => {
    let declared = false;
    const api = routes({
      "GET /station-standards": () => json({ location: "SIM1", standards: declared ? [PACK_STANDARD] : [] }),
      "PUT /station-standards/SIM1/PACK": () => {
        declared = true;
        return json(PACK_STANDARD, 201);
      },
    });
    const user = await loadSite();
    expect(await screen.findByText(/No station standard is declared/)).toBeInTheDocument();

    const f = await fillStandard(user, { process: "pack", qty: "180", unit: "PACKAGE", period: "3600" });
    await user.click(f.getByRole("button", { name: "Save standard" }));

    expect(await screen.findByText(/Declared the PACK standard for SIM1: 180 PACKAGE per station per 3,600 s/)).toBeInTheDocument();
    expect(api.to("PUT /station-standards/SIM1/PACK")[0].body).toEqual({ quantity: 180, unit: "PACKAGE", period_seconds: 3600 });
    expect(await screen.findByText("180 PACKAGE/h")).toBeInTheDocument();
    expect(api.to("GET /station-standards")).toHaveLength(2);
  });

  it("says 'Updated' when the standard replaced one (200)", async () => {
    routes({ "PUT /station-standards/SIM1/PACK": json({ ...PACK_STANDARD, quantity: 200 }, 200) });
    const user = await loadSite();
    const f = await fillStandard(user, { process: "PACK", qty: "200", unit: "PACKAGE" });
    await user.click(f.getByRole("button", { name: "Save standard" }));
    expect(await screen.findByText(/Updated the PACK standard for SIM1: 200 PACKAGE/)).toBeInTheDocument();
  });

  it("loads a row into the form to be replaced", async () => {
    routes();
    const user = await loadSite();
    await user.click(await screen.findByText("180 PACKAGE"));
    const form = screen.getByRole("form", { name: "Declare station standard" });
    expect(within(form).getByLabelText(/Process type/)).toHaveValue("PACK");
    expect(within(form).getByLabelText(/Quantity per station/)).toHaveValue(180);
    expect(within(form).getByLabelText(/Unit/)).toHaveValue("PACKAGE");
    expect(within(form).getByLabelText(/Period/)).toHaveValue(3600);
  });

  it.each([
    [{ qty: "180", unit: "PACKAGE" }, "Enter the process type"],
    [{ process: "PACK", unit: "PACKAGE" }, "Quantity per station must be a number above zero."],
    [{ process: "PACK", qty: "0", unit: "PACKAGE" }, "Quantity per station must be a number above zero."],
    [{ process: "PACK", qty: "180" }, "Choose the unit"],
    [{ process: "PACK", qty: "180", unit: "UNIT", period: "0" }, "Period must be a number of seconds above zero."],
  ])("validates before sending (%j)", async (fields, message) => {
    const api = routes();
    const user = await loadSite();
    const f = await fillStandard(user, fields);
    await user.click(f.getByRole("button", { name: "Save standard" }));
    expect(await screen.findByText(new RegExp(message.replace(/[.()]/g, "\\$&")))).toBeInTheDocument();
    expect(api.calls.filter((c) => c.method === "PUT")).toHaveLength(0);
  });

  it("shows the service's problem title and detail when the declaration is rejected", async () => {
    routes({
      "PUT /station-standards/SIM1/PACK": problem(422, "unsupported-normalization-unit", "This step's native unit cannot be normalized to ORDER", "unit LINE cannot be normalized to ORDER"),
    });
    const user = await loadSite();
    const f = await fillStandard(user, { process: "PACK", qty: "10", unit: "UNIT" });
    await user.click(f.getByRole("button", { name: "Save standard" }));
    const alert = await screen.findByText("This step's native unit cannot be normalized to ORDER");
    expect(alert).toBeInTheDocument();
    expect(screen.getByText("unit LINE cannot be normalized to ORDER")).toBeInTheDocument();
    await waitFor(() => expect(screen.queryByText(/Declared the/)).not.toBeInTheDocument());
  });
});
