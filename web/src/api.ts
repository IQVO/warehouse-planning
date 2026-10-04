import { PLANNING_API_BASE } from "./config";
import type {
  CapacityPlan,
  CapacityPlansResponse,
  CreateCapacityPlanInput,
  ExpectedDemand,
  PathCapacityQuery,
  ProcessPath,
  ProcessPathCapacity,
  ProcessPathsResponse,
  StationStandard,
  StationStandardInput,
  StationStandardsResponse,
  StorageCapacity,
} from "./types";

/** RFC 7807 problem+json body every error response from warehouse-planning
 *  returns (`type` ends in the problem slug, e.g. `.../missing-assigned-demand`). */
export interface ProblemDetails {
  type?: string;
  title?: string;
  status?: number;
  detail?: string;
  instance?: string;
}

/**
 * Every failed call -- an HTTP error with a problem+json body, an HTTP error
 * without one, or a network failure -- surfaces as one ApiError, so a screen
 * has a single shape to render: `title` (the problem's category) and `detail`
 * (what exactly was wrong), plus `slug` to branch on a specific problem.
 */
export class ApiError extends Error {
  readonly status: number;
  readonly problem: ProblemDetails | null;
  readonly title: string;
  readonly detail: string;
  /** Last path segment of problem.type (e.g. "capacity-plan-already-published"); "" when unknown. */
  readonly slug: string;

  constructor(status: number, problem: ProblemDetails | null, fallbackTitle: string) {
    const title = problem?.title || fallbackTitle;
    const detail = problem?.detail ?? "";
    super(detail || title);
    this.name = "ApiError";
    this.status = status;
    this.problem = problem;
    this.title = title;
    this.detail = detail;
    this.slug = problem?.type ? (problem.type.split("/").pop() ?? "") : "";
  }
}

/** Normalizes anything thrown by a request into an ApiError. */
export function toApiError(err: unknown): ApiError {
  if (err instanceof ApiError) return err;
  const message = err instanceof Error ? err.message : String(err);
  return new ApiError(0, { detail: message }, "Network error");
}

type Query = Record<string, string | number | undefined>;

function withQuery(path: string, query?: Query): string {
  if (!query) return path;
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== "") params.set(key, String(value));
  }
  const qs = params.toString();
  return qs ? `${path}?${qs}` : path;
}

export interface ApiResult<T> {
  status: number;
  data: T;
}

async function request<T>(
  method: "GET" | "POST" | "PUT",
  path: string,
  body?: unknown,
): Promise<ApiResult<T>> {
  let res: Response;
  try {
    res = await fetch(`${PLANNING_API_BASE}${path}`, {
      method,
      ...(body === undefined
        ? {}
        : { headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }),
    });
  } catch (err) {
    throw toApiError(err);
  }
  if (!res.ok) {
    let problem: ProblemDetails | null = null;
    try {
      problem = (await res.json()) as ProblemDetails;
    } catch {
      // non-JSON error body -- fall through with problem = null
    }
    throw new ApiError(res.status, problem, `${res.status} ${res.statusText}`.trim());
  }
  return { status: res.status, data: (await res.json()) as T };
}

const seg = encodeURIComponent;

/** GET /storage-capacity?location= */
export async function getStorageCapacity(location: string): Promise<StorageCapacity> {
  return (await request<StorageCapacity>("GET", withQuery("/storage-capacity", { location }))).data;
}

/** GET /station-standards?location= */
export async function getStationStandards(location: string): Promise<StationStandard[]> {
  const res = await request<StationStandardsResponse>(
    "GET",
    withQuery("/station-standards", { location }),
  );
  return res.data.standards ?? [];
}

/** PUT /station-standards/{location}/{process_type}; status is 201 (declared) or 200 (replaced). */
export function putStationStandard(
  location: string,
  processType: string,
  input: StationStandardInput,
): Promise<ApiResult<StationStandard>> {
  return request<StationStandard>("PUT", `/station-standards/${seg(location)}/${seg(processType)}`, input);
}

/** GET /demand?location=&window_start=&window_end= */
export async function getDemand(
  location: string,
  windowStart: string,
  windowEnd: string,
): Promise<ExpectedDemand> {
  return (
    await request<ExpectedDemand>(
      "GET",
      withQuery("/demand", { location, window_start: windowStart, window_end: windowEnd }),
    )
  ).data;
}

/** GET /process-paths */
export async function listProcessPaths(): Promise<ProcessPath[]> {
  return (await request<ProcessPathsResponse>("GET", "/process-paths")).data.process_paths ?? [];
}

/** GET /process-paths/{id}/capacity?... */
export async function getPathCapacity(q: PathCapacityQuery): Promise<ProcessPathCapacity> {
  return (
    await request<ProcessPathCapacity>(
      "GET",
      withQuery(`/process-paths/${seg(q.pathId)}/capacity`, {
        location: q.location,
        window_start: q.windowStart,
        window_end: q.windowEnd,
        units_per_order: q.unitsPerOrder,
        packages_per_order: q.packagesPerOrder,
      }),
    )
  ).data;
}

/** GET /capacity-plans?location= (the service's own default limit of 20 applies). */
export async function listCapacityPlans(location: string): Promise<CapacityPlan[]> {
  const res = await request<CapacityPlansResponse>("GET", withQuery("/capacity-plans", { location }));
  return res.data.capacity_plans ?? [];
}

/** POST /capacity-plans (201 -> the new DRAFT plan). */
export async function createCapacityPlan(input: CreateCapacityPlanInput): Promise<CapacityPlan> {
  return (await request<CapacityPlan>("POST", "/capacity-plans", input)).data;
}

/** POST /capacity-plans/{id}/publish (200 -> the PUBLISHED plan). */
export async function publishCapacityPlan(id: string): Promise<CapacityPlan> {
  return (await request<CapacityPlan>("POST", `/capacity-plans/${seg(id)}/publish`)).data;
}
