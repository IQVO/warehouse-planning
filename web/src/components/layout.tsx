import type { ReactNode } from "react";
import { FormRow, TextField } from "./formkit";

export function PageHeader({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <div>
      <h1 style={{ fontSize: "var(--wh-font-size-2xl)", margin: 0 }}>{title}</h1>
      <p style={{ color: "var(--wh-color-text-muted)", marginTop: 4 }}>{subtitle}</p>
    </div>
  );
}

export function Stack({ children, gap = 4 }: { children: ReactNode; gap?: 3 | 4 | 5 }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: `var(--wh-space-${gap})` }}>
      {children}
    </div>
  );
}

/** Two datetime-local pickers for a planning window. Read as UTC (see lib.ts). */
export function WindowFields({
  start,
  end,
  onStart,
  onEnd,
  required,
}: {
  start: string;
  end: string;
  onStart: (value: string) => void;
  onEnd: (value: string) => void;
  required?: boolean;
}) {
  return (
    <FormRow>
      <TextField label="Window start (UTC)" type="datetime-local" value={start} onChange={onStart} required={required} />
      <TextField label="Window end (UTC)" type="datetime-local" value={end} onChange={onEnd} required={required} />
    </FormRow>
  );
}
