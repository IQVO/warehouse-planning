import { createContext, useContext, useState } from "react";

export type SiteState = [string, (location: string) => void];

export const SiteContext = createContext<SiteState | null>(null);

/** The shared site code (see SiteProvider); falls back to screen-local state
 *  when a screen is rendered outside a SiteProvider (tests, embedding). */
export function useSiteLocation(): SiteState {
  const shared = useContext(SiteContext);
  const local = useState("");
  return shared ?? local;
}
