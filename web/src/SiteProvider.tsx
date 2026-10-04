import { useState } from "react";
import type { ReactNode } from "react";
import { SiteContext } from "./siteContext";

/** Shares the operator's site code between the three screens, so typing it
 *  once on the overview carries to path capacity and plans. Starts EMPTY: no
 *  site is assumed. */
export function SiteProvider({ children }: { children: ReactNode }) {
  const state = useState("");
  return <SiteContext.Provider value={state}>{children}</SiteContext.Provider>;
}
