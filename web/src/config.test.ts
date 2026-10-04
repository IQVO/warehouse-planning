import { describe, expect, it } from "vitest";
import { PLANNING_API_BASE, resolvePlanningApiBase } from "./config";

describe("resolvePlanningApiBase", () => {
  it("builds the production API base from the runtime API origin", () => {
    expect(resolvePlanningApiBase({ apiOrigin: "http://localhost:8000" }, true)).toBe(
      "http://localhost:8000/api/warehouse-planning",
    );
  });

  it("normalizes trailing slashes on the runtime API origin", () => {
    expect(resolvePlanningApiBase({ apiOrigin: "https://warehouse.example/" }, true)).toBe(
      "https://warehouse.example/api/warehouse-planning",
    );
    expect(resolvePlanningApiBase({ apiOrigin: "https://warehouse.example///" }, true)).toBe(
      "https://warehouse.example/api/warehouse-planning",
    );
  });

  it("fails loudly when production runtime configuration has no API origin", () => {
    expect(() => resolvePlanningApiBase({}, true)).toThrow(
      "window.__WAREHOUSE_CONFIG__.apiOrigin is required in production",
    );
    expect(() => resolvePlanningApiBase({ apiOrigin: "" }, true)).toThrow(
      "window.__WAREHOUSE_CONFIG__.apiOrigin is required in production",
    );
  });

  it("falls back to the service's own dev port outside production", () => {
    expect(resolvePlanningApiBase({}, false)).toBe("http://localhost:8080");
  });

  it("still prefers a runtime origin over the dev fallback", () => {
    expect(resolvePlanningApiBase({ apiOrigin: "http://localhost:8000" }, false)).toBe(
      "http://localhost:8000/api/warehouse-planning",
    );
  });
});

describe("PLANNING_API_BASE", () => {
  it("loads without /config.json (window.__WAREHOUSE_CONFIG__ absent) in a non-production build", () => {
    // The module evaluated at import time with no published config: it must
    // not throw and must resolve to the dev base.
    expect(window.__WAREHOUSE_CONFIG__).toBeUndefined();
    expect(PLANNING_API_BASE).toBe("http://localhost:8080");
  });
});
