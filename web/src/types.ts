/** Wire types mirroring warehouse-planning's REST DTOs exactly
 *  (internal/adapters/inbound/http/dto.go and demand_handler.go; field names
 *  are snake_case on the wire). Kept hand-in-sync with the Go DTOs and
 *  apis/openapi.yaml rather than code-generated. */

/** The natural unit of a process's throughput (a station standard). */
export type StandardUnit = "UNIT" | "PACKAGE" | "ORDER";

export interface StoragePositions {
  zone_id: string;
  location_type: string;
  positions: number;
}

export interface ZoneStations {
  zone_id: string;
  activity: string;
  stations: number;
}

/** GET /storage-capacity?location= */
export interface StorageCapacity {
  location: string;
  storage_positions: StoragePositions[];
  stations: ZoneStations[];
}

export interface StationStandard {
  location: string;
  process_type: string;
  quantity: number;
  unit: string;
  period_seconds: number;
}

/** GET /station-standards?location= (`location` is omitted when unfiltered). */
export interface StationStandardsResponse {
  location?: string;
  standards: StationStandard[];
}

/** PUT /station-standards/{location}/{process_type} body. */
export interface StationStandardInput {
  quantity: number;
  unit: StandardUnit;
  period_seconds: number;
}

/** GET /demand?location=&window_start=&window_end=. `as_of` is null when the
 *  site's read model has no order event at all: that is "no data", NOT zero
 *  demand. */
export interface ExpectedDemand {
  location: string;
  window_start: string;
  window_end: string;
  orders: number;
  released_lines: number;
  source: string;
  as_of: string | null;
}

export interface ProcessPath {
  id: string;
  name: string;
  steps: string[];
}

/** GET /process-paths */
export interface ProcessPathsResponse {
  process_paths: ProcessPath[];
}

export interface StepBreakdownItem {
  step: string;
  /** ORDER per HOUR. */
  normalized_rate: number;
  binding_constraint: string;
}

/** GET /process-paths/{id}/capacity */
export interface ProcessPathCapacity {
  normalized_rate: number;
  normalized_unit: string;
  bottleneck_step: string;
  step_breakdown: StepBreakdownItem[];
  warnings: string[];
}

export interface PathCapacityQuery {
  pathId: string;
  location: string;
  windowStart: string;
  windowEnd: string;
  unitsPerOrder?: number;
  packagesPerOrder?: number;
}

export type PlanStatus = "DRAFT" | "PUBLISHED";
export type DemandSource = "request" | "orders";

/** One CapacityPlan: GET /capacity-plans items, GET/POST /capacity-plans and
 *  POST /capacity-plans/{id}/publish all return this shape. */
export interface CapacityPlan {
  id: string;
  warehouse_id: string;
  /** Canonical site (facility-layout site_code); empty for plans stored before migration 0008. */
  site_id: string;
  location: string;
  window_start: string;
  window_end: string;
  path_id: string;
  assigned_demand: number;
  demand_source: DemandSource;
  status: PlanStatus;
  /** ORDER per HOUR. */
  path_capacity: number;
  bottleneck_step: string;
  bottleneck_constraint: string;
  capacity_over_window: number;
  shortage: number;
  created_at: string;
  published_at?: string;
  warnings: string[];
}

/** GET /capacity-plans?location=&limit= (`location` omitted when unfiltered). */
export interface CapacityPlansResponse {
  location?: string;
  capacity_plans: CapacityPlan[];
}

/** POST /capacity-plans body. `assigned_demand` is OPTIONAL: when omitted the
 *  service defaults it from the order-management read model (and answers 422
 *  `missing-assigned-demand` when that has no order for the site and window). */
export interface CreateCapacityPlanInput {
  warehouse_id: string;
  /** REQUIRED canonical site (facility-layout site_code). */
  site_id: string;
  location: string;
  window_start: string;
  window_end: string;
  path_id: string;
  assigned_demand?: number;
  units_per_order?: number;
  packages_per_order?: number;
}
