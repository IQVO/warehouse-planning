/** Small pure helpers shared by the screens. */

const DATETIME_LOCAL = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})(?::(\d{2}))?$/;

/**
 * Converts an <input type="datetime-local"> value to the RFC 3339 instant the
 * API wants. The pickers are labelled UTC and READ AS UTC: a planning window
 * is a warehouse-wide fact that the API stores and returns in UTC, so
 * silently shifting it by the browser's zone would plan the wrong hours.
 * Returns null for an empty or malformed value.
 */
export function datetimeLocalToRfc3339(value: string): string | null {
  const m = DATETIME_LOCAL.exec(value.trim());
  if (!m) return null;
  const iso = `${m[1]}T${m[2]}:${m[3] ?? "00"}Z`;
  return Number.isNaN(Date.parse(iso)) ? null : iso;
}

export type WindowCheck =
  | { ok: true; start: string; end: string }
  | { ok: false; reason: "missing" | "invalid"; message: string };

/** Validates a window picked with two datetime-local inputs. */
export function checkWindow(startValue: string, endValue: string): WindowCheck {
  const start = datetimeLocalToRfc3339(startValue);
  const end = datetimeLocalToRfc3339(endValue);
  if (!start || !end) {
    return { ok: false, reason: "missing", message: "Pick a window start and end (UTC)." };
  }
  if (Date.parse(end) <= Date.parse(start)) {
    return { ok: false, reason: "invalid", message: "The window end must be after the window start." };
  }
  return { ok: true, start, end };
}

/** "2026-10-05T08:00:00Z" -> "2026-10-05 08:00Z" (deterministic, always UTC). */
export function utcLabel(iso: string | null | undefined): string {
  if (!iso) return "—";
  const m = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})/.exec(iso);
  return m ? `${m[1]} ${m[2]}Z` : iso;
}

/** Parses an optional number field: "" -> undefined; garbage -> NaN. */
export function optionalNumber(raw: string): number | undefined {
  const trimmed = raw.trim();
  return trimmed === "" ? undefined : Number(trimmed);
}

/** A throughput quantity per period as a per-hour figure. */
export function perHour(quantity: number, periodSeconds: number): number {
  return periodSeconds > 0 ? (quantity / periodSeconds) * 3600 : 0;
}
