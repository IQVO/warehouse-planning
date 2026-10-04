import { describe, expect, it } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import { ApiError } from "../api";
import { useRequest } from "./useRequest";

function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("useRequest", () => {
  it("is idle while the request is not asked for", () => {
    const { result } = renderHook(() => useRequest<string>(null, "k"));
    expect(result.current.status).toBe("idle");
  });

  it("goes loading -> success", async () => {
    const d = deferred<string>();
    const { result } = renderHook(() => useRequest(() => d.promise, "k"));
    expect(result.current.status).toBe("loading");
    await act(async () => d.resolve("data"));
    expect(result.current).toMatchObject({ status: "success", data: "data" });
  });

  it("goes loading -> error, normalizing a non-ApiError rejection", async () => {
    const d = deferred<string>();
    const { result } = renderHook(() => useRequest(() => d.promise, "k"));
    await act(async () => d.reject(new TypeError("Failed to fetch")));
    expect(result.current.status).toBe("error");
    if (result.current.status === "error") {
      expect(result.current.error).toBeInstanceOf(ApiError);
      expect(result.current.error.detail).toBe("Failed to fetch");
    }
  });

  it("drops a response that arrives after the key changed", async () => {
    const first = deferred<string>();
    const second = deferred<string>();
    const { result, rerender } = renderHook(
      ({ key }) => useRequest(() => (key === "a" ? first.promise : second.promise), key),
      { initialProps: { key: "a" } },
    );
    rerender({ key: "b" });
    await act(async () => second.resolve("B"));
    expect(result.current).toMatchObject({ status: "success", data: "B" });
    await act(async () => first.resolve("A (stale)"));
    expect(result.current).toMatchObject({ status: "success", data: "B" });
  });

  it("reload() runs the loader again", async () => {
    let n = 0;
    const { result } = renderHook(() => useRequest(async () => ++n, "k"));
    await waitFor(() => expect(result.current).toMatchObject({ status: "success", data: 1 }));
    act(() => result.current.reload());
    await waitFor(() => expect(result.current).toMatchObject({ status: "success", data: 2 }));
  });

  it("returns to idle when the request is withdrawn", async () => {
    const { result, rerender } = renderHook(
      ({ on }) => useRequest(on ? async () => "x" : null, on ? "k" : "none"),
      { initialProps: { on: true } },
    );
    await waitFor(() => expect(result.current.status).toBe("success"));
    rerender({ on: false });
    await waitFor(() => expect(result.current.status).toBe("idle"));
  });
});
