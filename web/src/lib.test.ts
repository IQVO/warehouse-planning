import { describe, expect, it } from "vitest";
import { checkWindow, datetimeLocalToRfc3339, optionalNumber, perHour, utcLabel } from "./lib";

describe("datetimeLocalToRfc3339", () => {
  it("reads a datetime-local value as UTC", () => {
    expect(datetimeLocalToRfc3339("2026-10-05T08:00")).toBe("2026-10-05T08:00:00Z");
    expect(datetimeLocalToRfc3339("2026-10-05T08:00:30")).toBe("2026-10-05T08:00:30Z");
  });
  it("rejects empty, malformed and impossible values", () => {
    expect(datetimeLocalToRfc3339("")).toBeNull();
    expect(datetimeLocalToRfc3339("tomorrow")).toBeNull();
    expect(datetimeLocalToRfc3339("2026-13-45T25:61")).toBeNull();
  });
});

describe("checkWindow", () => {
  it("accepts an ordered window", () => {
    expect(checkWindow("2026-10-05T08:00", "2026-10-05T16:00")).toEqual({
      ok: true,
      start: "2026-10-05T08:00:00Z",
      end: "2026-10-05T16:00:00Z",
    });
  });
  it("reports a missing bound", () => {
    expect(checkWindow("", "2026-10-05T16:00")).toMatchObject({ ok: false, reason: "missing" });
    expect(checkWindow("2026-10-05T08:00", "")).toMatchObject({ ok: false, reason: "missing" });
  });
  it("rejects an end that is not after the start (equal included)", () => {
    expect(checkWindow("2026-10-05T16:00", "2026-10-05T08:00")).toMatchObject({ ok: false, reason: "invalid" });
    expect(checkWindow("2026-10-05T08:00", "2026-10-05T08:00")).toMatchObject({ ok: false, reason: "invalid" });
  });
});

describe("utcLabel", () => {
  it("formats an RFC 3339 instant in UTC", () => {
    expect(utcLabel("2026-10-05T08:00:00Z")).toBe("2026-10-05 08:00Z");
  });
  it("renders absence as a dash and leaves unparseable text readable", () => {
    expect(utcLabel(null)).toBe("—");
    expect(utcLabel(undefined)).toBe("—");
    expect(utcLabel("")).toBe("—");
    expect(utcLabel("soon")).toBe("soon");
  });
});

describe("optionalNumber / perHour", () => {
  it("treats blank as absent and keeps garbage visible as NaN", () => {
    expect(optionalNumber("")).toBeUndefined();
    expect(optionalNumber("  ")).toBeUndefined();
    expect(optionalNumber("2.5")).toBe(2.5);
    expect(optionalNumber("0")).toBe(0);
    expect(optionalNumber("abc")).toBeNaN();
  });
  it("scales a per-period quantity to an hour", () => {
    expect(perHour(180, 3600)).toBe(180);
    expect(perHour(30, 60)).toBe(1800);
    expect(perHour(1, 0)).toBe(0);
  });
});
