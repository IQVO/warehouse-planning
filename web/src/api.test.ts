import { describe, expect, it, vi } from "vitest";
import {
  ApiError,
  createCapacityPlan,
  getDemand,
  getPathCapacity,
  getStationStandards,
  getStorageCapacity,
  listCapacityPlans,
  listProcessPaths,
  publishCapacityPlan,
  putStationStandard,
  toApiError,
} from "./api";
import { PLANNING_API_BASE } from "./config";
import { json, mockApi, problem } from "./test/fetchMock";

describe("API client URLs and verbs", () => {
  it("GET /storage-capacity sends the location and unwraps the body", async () => {
    const api = mockApi({
      "GET /storage-capacity": json({ location: "S 1", storage_positions: [], stations: [] }),
    });
    const got = await getStorageCapacity("S 1");
    expect(got.location).toBe("S 1");
    const [call] = api.calls;
    expect(call.url.startsWith(`${PLANNING_API_BASE}/storage-capacity?`)).toBe(true);
    expect(call.query.get("location")).toBe("S 1");
    expect(call.method).toBe("GET");
    expect(call.body).toBeUndefined();
  });

  it("GET /station-standards returns the standards array (and [] if the service omits it)", async () => {
    mockApi({ "GET /station-standards": json({ location: "S1", standards: [{ location: "S1", process_type: "PACK", quantity: 180, unit: "PACKAGE", period_seconds: 3600 }] }) });
    expect(await getStationStandards("S1")).toHaveLength(1);
    mockApi({ "GET /station-standards": json({}) });
    expect(await getStationStandards("S1")).toEqual([]);
  });

  it("PUT /station-standards/{location}/{process_type} path-encodes, sends the body and reports the status", async () => {
    const api = mockApi({
      "PUT /station-standards/S%2F1/PACK": json(
        { location: "S/1", process_type: "PACK", quantity: 180, unit: "PACKAGE", period_seconds: 3600 },
        201,
      ),
    });
    const res = await putStationStandard("S/1", "PACK", { quantity: 180, unit: "PACKAGE", period_seconds: 3600 });
    expect(res.status).toBe(201);
    expect(res.data.process_type).toBe("PACK");
    expect(api.calls[0].method).toBe("PUT");
    expect(api.calls[0].headers["Content-Type"]).toBe("application/json");
    expect(api.calls[0].body).toEqual({ quantity: 180, unit: "PACKAGE", period_seconds: 3600 });
  });

  it("GET /demand sends location and both window bounds", async () => {
    const api = mockApi({
      "GET /demand": json({ location: "S1", window_start: "a", window_end: "b", orders: 0, released_lines: 0, source: "order-management", as_of: null }),
    });
    const demand = await getDemand("S1", "2026-10-05T08:00:00Z", "2026-10-05T16:00:00Z");
    expect(demand.as_of).toBeNull();
    expect(Object.fromEntries(api.calls[0].query)).toEqual({
      location: "S1",
      window_start: "2026-10-05T08:00:00Z",
      window_end: "2026-10-05T16:00:00Z",
    });
  });

  it("GET /process-paths returns the paths", async () => {
    const api = mockApi({ "GET /process-paths": json({ process_paths: [{ id: "p", name: "P", steps: ["PICK"] }] }) });
    expect(await listProcessPaths()).toEqual([{ id: "p", name: "P", steps: ["PICK"] }]);
    expect(api.calls[0].url).toBe(`${PLANNING_API_BASE}/process-paths`);
  });

  it("GET /process-paths/{id}/capacity sends the window and only the factors that were given", async () => {
    const api = mockApi({
      "GET /process-paths/pick-rebin-pack/capacity": json({ normalized_rate: 1, normalized_unit: "ORDER", bottleneck_step: "PICK", step_breakdown: [], warnings: [] }),
    });
    await getPathCapacity({
      pathId: "pick-rebin-pack",
      location: "S1",
      windowStart: "2026-10-05T08:00:00Z",
      windowEnd: "2026-10-05T16:00:00Z",
      unitsPerOrder: 2.5,
    });
    expect(Object.fromEntries(api.calls[0].query)).toEqual({
      location: "S1",
      window_start: "2026-10-05T08:00:00Z",
      window_end: "2026-10-05T16:00:00Z",
      units_per_order: "2.5",
    });
  });

  it("GET /capacity-plans filters by location, and omits an empty one", async () => {
    const api = mockApi({ "GET /capacity-plans": json({ capacity_plans: [] }) });
    expect(await listCapacityPlans("S1")).toEqual([]);
    expect(await listCapacityPlans("")).toEqual([]);
    expect(api.calls[0].query.get("location")).toBe("S1");
    expect(api.calls[1].url).toBe(`${PLANNING_API_BASE}/capacity-plans`);
  });

  it("POST /capacity-plans sends the body as given (assigned_demand absent stays absent)", async () => {
    const api = mockApi({ "POST /capacity-plans": json({ id: "plan-1", status: "DRAFT" }, 201) });
    const plan = await createCapacityPlan({
      warehouse_id: "WH-1",
      location: "S1",
      window_start: "2026-10-05T08:00:00Z",
      window_end: "2026-10-05T16:00:00Z",
      path_id: "p",
    });
    expect(plan.id).toBe("plan-1");
    expect(api.calls[0].method).toBe("POST");
    expect(api.calls[0].body).not.toHaveProperty("assigned_demand");
  });

  it("POST /capacity-plans/{id}/publish sends no body", async () => {
    const api = mockApi({ "POST /capacity-plans/abc/publish": json({ id: "abc", status: "PUBLISHED" }) });
    expect((await publishCapacityPlan("abc")).status).toBe("PUBLISHED");
    expect(api.calls[0].method).toBe("POST");
    expect(api.calls[0].body).toBeUndefined();
  });
});

describe("errors", () => {
  it("surfaces the RFC 7807 title, detail, status and slug", async () => {
    mockApi({
      "POST /capacity-plans": problem(422, "missing-assigned-demand", "assigned_demand is required", "assigned_demand (orders) must be provided"),
    });
    const err = await createCapacityPlan({ warehouse_id: "w", location: "l", window_start: "a", window_end: "b", path_id: "p" }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    const apiErr = err as ApiError;
    expect(apiErr.status).toBe(422);
    expect(apiErr.title).toBe("assigned_demand is required");
    expect(apiErr.detail).toBe("assigned_demand (orders) must be provided");
    expect(apiErr.slug).toBe("missing-assigned-demand");
    expect(apiErr.message).toBe("assigned_demand (orders) must be provided");
  });

  it("falls back to the status line when the error body is not JSON", async () => {
    mockApi({ "GET /process-paths": new Response("<html>bad gateway</html>", { status: 502, statusText: "Bad Gateway" }) });
    const err = (await listProcessPaths().catch((e: unknown) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(502);
    expect(err.title).toBe("502 Bad Gateway");
    expect(err.detail).toBe("");
    expect(err.slug).toBe("");
    expect(err.problem).toBeNull();
  });

  it("turns a network failure into a status-0 ApiError carrying the cause", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new TypeError("Failed to fetch")));
    const err = (await listProcessPaths().catch((e: unknown) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(0);
    expect(err.title).toBe("Network error");
    expect(err.detail).toBe("Failed to fetch");
  });

  it("toApiError passes an ApiError through and wraps anything else", () => {
    const original = new ApiError(404, null, "Not found");
    expect(toApiError(original)).toBe(original);
    expect(toApiError("boom").detail).toBe("boom");
    expect(toApiError(new Error("x")).title).toBe("Network error");
  });
});
