import { describe, expect, it } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import App from "./App";
import { json, mockApi } from "./test/fetchMock";
import { PATHS, STORAGE } from "./test/fixtures";

function mount(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <App />
    </MemoryRouter>,
  );
}

/** Mounted the way the console mounts the remote: inside the host's own
 *  `<Route path="/capacity/*">` splat route. Relative links behave differently
 *  there than at the router root, which is exactly what the root-mounted tests
 *  above cannot see. */
function mountUnderHost(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/capacity/*" element={<App />} />
      </Routes>
    </MemoryRouter>,
  );
}

function subNavHrefs(): string[] {
  const nav = screen.getByRole("navigation", { name: "Capacity sections" });
  return within(nav)
    .getAllByRole("link")
    .map((a) => a.getAttribute("href") ?? "");
}

function routes() {
  return mockApi({
    "GET /storage-capacity": json(STORAGE),
    "GET /station-standards": json({ standards: [] }),
    "GET /demand": json({ location: "SIM1", window_start: "a", window_end: "b", orders: 3, released_lines: 6, source: "order-management", as_of: "2026-10-04T09:15:30Z" }),
    "GET /process-paths": json({ process_paths: PATHS }),
    "GET /capacity-plans": json({ capacity_plans: [] }),
  });
}

describe("App (the exposed ./App)", () => {
  it("renders the overview at the index route with a three-way sub-nav", () => {
    routes();
    mount("/");
    expect(screen.getByRole("heading", { name: "Capacity overview" })).toBeInTheDocument();
    const nav = screen.getByRole("navigation", { name: "Capacity sections" });
    expect(nav).toHaveTextContent("Overview");
    expect(nav).toHaveTextContent("Path capacity");
    expect(nav).toHaveTextContent("Capacity plans");
  });

  it("routes relatively: /paths and /plans work under any mount prefix", async () => {
    routes();
    const { unmount } = mount("/paths");
    expect(await screen.findByRole("heading", { name: "Path capacity" })).toBeInTheDocument();
    unmount();
    mount("/plans");
    expect(await screen.findByRole("heading", { name: "Capacity plans" })).toBeInTheDocument();
  });

  it("navigates between screens and carries the typed site code along", async () => {
    routes();
    const user = userEvent.setup();
    mount("/");
    await user.type(screen.getByLabelText(/Site code/), "SIM1");
    fireEvent.change(screen.getByLabelText(/Window start/), { target: { value: "2026-10-05T08:00" } });
    fireEvent.change(screen.getByLabelText(/Window end/), { target: { value: "2026-10-05T16:00" } });

    await user.click(screen.getByRole("link", { name: "Path capacity" }));
    expect(await screen.findByRole("heading", { name: "Path capacity" })).toBeInTheDocument();
    expect(await screen.findByLabelText(/Site code/)).toHaveValue("SIM1");

    await user.click(screen.getByRole("link", { name: "Capacity plans" }));
    expect(await screen.findByRole("heading", { name: "Capacity plans" })).toBeInTheDocument();
    await waitFor(() => expect(screen.getByLabelText("Site code")).toHaveValue("SIM1"));
  });

  it("starts with no site assumed", () => {
    routes();
    mount("/");
    expect(screen.getByLabelText(/Site code/)).toHaveValue("");
  });

  describe("mounted under the console's /capacity/* splat route", () => {
    it.each(["/capacity", "/capacity/paths", "/capacity/plans"])(
      "sub-nav links resolve against the mount point, not the current URL (%s)",
      async (start) => {
        routes();
        mountUnderHost(start);
        await screen.findByRole("navigation", { name: "Capacity sections" });
        expect(subNavHrefs()).toEqual(["/capacity", "/capacity/paths", "/capacity/plans"]);
      },
    );

    it.each([
      ["/capacity", "Overview"],
      ["/capacity/paths", "Path capacity"],
      ["/capacity/plans", "Capacity plans"],
    ])("marks only the current section active (%s)", async (start, active) => {
      routes();
      mountUnderHost(start);
      const nav = await screen.findByRole("navigation", { name: "Capacity sections" });
      for (const name of ["Overview", "Path capacity", "Capacity plans"]) {
        const link = within(nav).getByRole("link", { name });
        if (name === active) expect(link).toHaveAttribute("aria-current", "page");
        else expect(link).not.toHaveAttribute("aria-current");
      }
    });

    it("navigates between all screens in any order, hop after hop", async () => {
      routes();
      const user = userEvent.setup();
      mountUnderHost("/capacity/plans");
      expect(await screen.findByRole("heading", { name: "Capacity plans" })).toBeInTheDocument();

      await user.click(screen.getByRole("link", { name: "Path capacity" }));
      expect(await screen.findByRole("heading", { name: "Path capacity" })).toBeInTheDocument();

      await user.click(screen.getByRole("link", { name: "Overview" }));
      expect(await screen.findByRole("heading", { name: "Capacity overview" })).toBeInTheDocument();

      await user.click(screen.getByRole("link", { name: "Capacity plans" }));
      expect(await screen.findByRole("heading", { name: "Capacity plans" })).toBeInTheDocument();
      expect(subNavHrefs()).toEqual(["/capacity", "/capacity/paths", "/capacity/plans"]);
    });
  });
});
