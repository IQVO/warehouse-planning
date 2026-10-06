import type {
  CapacityPlan,
  ExpectedDemand,
  ProcessPath,
  ProcessPathCapacity,
  StationStandard,
  StorageCapacity,
} from "../types";

export const STORAGE: StorageCapacity = {
  location: "SIM1",
  storage_positions: [
    { zone_id: "SIM1-STOR-AMB", location_type: "SimShelf", positions: 24 },
    { zone_id: "SIM1-STOR-COLD", location_type: "ColdBin", positions: 1200 },
  ],
  stations: [{ zone_id: "SIM1-OPS-WC", activity: "PACK", stations: 11 }],
};

export const EMPTY_STORAGE: StorageCapacity = { location: "NEW1", storage_positions: [], stations: [] };

export const PACK_STANDARD: StationStandard = {
  location: "SIM1",
  process_type: "PACK",
  quantity: 180,
  unit: "PACKAGE",
  period_seconds: 3600,
};

export const DEMAND: ExpectedDemand = {
  location: "SIM1",
  window_start: "2026-10-05T08:00:00Z",
  window_end: "2026-10-05T16:00:00Z",
  orders: 3,
  released_lines: 6,
  source: "order-management",
  as_of: "2026-10-04T09:15:30Z",
};

export const NO_DEMAND_DATA: ExpectedDemand = { ...DEMAND, orders: 0, released_lines: 0, as_of: null };

export const PATHS: ProcessPath[] = [
  { id: "pick-rebin-pack", name: "Pick-Rebin-Pack", steps: ["PICK", "REBIN", "PACK"] },
  { id: "pick-pack", name: "Pick-Pack", steps: ["PICK", "PACK"] },
];

export const PATH_CAPACITY: ProcessPathCapacity = {
  normalized_rate: 1800,
  normalized_unit: "ORDER",
  bottleneck_step: "PACK",
  step_breakdown: [
    { step: "PICK", normalized_rate: 3200, binding_constraint: "LABOR" },
    { step: "REBIN", normalized_rate: 2400, binding_constraint: "LABOR" },
    { step: "PACK", normalized_rate: 1800, binding_constraint: "STATION" },
  ],
  warnings: [],
};

export const WARNING = "PACK has 11 stations tallied but no station standard is declared; they add no capacity";

export function plan(overrides: Partial<CapacityPlan> = {}): CapacityPlan {
  return {
    id: "0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10",
    warehouse_id: "WH-1",
    site_id: "SIM1",
    location: "SIM1",
    window_start: "2026-10-05T08:00:00Z",
    window_end: "2026-10-05T16:00:00Z",
    path_id: "pick-rebin-pack",
    assigned_demand: 12000,
    demand_source: "request",
    status: "DRAFT",
    path_capacity: 1000,
    bottleneck_step: "REBIN",
    bottleneck_constraint: "LABOR",
    capacity_over_window: 8000,
    shortage: 4000,
    created_at: "2026-10-04T17:15:30Z",
    warnings: [],
    ...overrides,
  };
}
