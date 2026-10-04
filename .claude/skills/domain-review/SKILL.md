---
name: domain-review
description: Ubiquitous-language drift review of a warehouse-planning change against .claude/rules/domain-model.md (ProcessCapacity, CapacityWindow, StationStandard, CapacityPlan, ...) - renamed or invented terms, aggregate-invariant leaks, units. Invoke explicitly - /domain-review [range].
disable-model-invocation: true
argument-hint: "[git range]"
---

Perform a ubiquitous-language drift review of the current changes (or
`$ARGUMENTS` if given), comparing new/changed code against
`.claude/rules/domain-model.md`. The same vocabulary is published for readers
in `docs/docs/ddd/ubiquitous-language.md`, `docs/docs/ddd/use-cases.md` and
`docs/docs/overview/aggregates.md`; the rule file is the source of truth, the
docs pages must follow it.

Drift is the quiet failure: code that works but renames, reshapes or
duplicates a concept the model already named, so nobody can map code back to
the domain conversation.

## What to check, in priority order

1. **A new type/field/method duplicating an existing concept under another
   name.** The vocabulary is `ProcessCapacity`, `CapacityConstraint`,
   `CapacityRate`, `CapacityWindow`, `WorkloadProfile`, `ProcessPath`,
   `ProcessPathCapacity`, `StationStandard`, station count, site/location,
   `CapacityPlan`, shortage, bottleneck. A second way to say "throughput of one
   station" (`PerStationRate`, `StationRate`) or "end-to-end rate"
   (`PathThroughput`) next to `StationStandard` / `ProcessPathCapacity` is
   drift even when the math is right. Point at the existing name.
2. **Implementation words in domain code.** `internal/domain/` should read like
   the language: `Publish`, `Covers`, `ComposeStepCapacity`, `EffectiveRate`,
   not `UpdateRow`, `SetStatus`, `PatchWindow`. The fitness tests cannot catch
   naming.
3. **Units and windows treated loosely.** A capacity number with no window is
   incomplete by definition; a `CapacityRate` is quantity + native unit +
   period and is never compared across units without a `WorkloadProfile`
   (`NormalizeToOrderRate`). Flag a raw float compared across UNIT / PACKAGE /
   ORDER, or a `CapacityWindow` replaced by two bare timestamps. Registration
   uses the window as exact identity; planning lookups use `Covers` (ADR 0003).
4. **An invariant in code that the model does not mention, or vice versa.**
   A new validation or state guard (`ErrAlreadyPublished`, shortage is never
   negative and demand equal to capacity is not a shortage, one native unit per
   `ProcessCapacity`) must be written into `.claude/rules/domain-model.md` in
   the SAME PR; an undocumented invariant looks like dead code to the next
   editor.
5. **Open versus closed vocabularies.** `ConstraintType` (LABOR, LOCATION,
   EQUIPMENT, STATION, CONVEYOR, BUFFER, REPLENISHMENT) and `CapacityUnit`
   (UNIT, LINE, ORDER, PACKAGE) are closed sets with defined behaviour (a
   `StationStandard` accepts only UNIT, PACKAGE or ORDER; `LINE` is rejected
   by normalization). A new categorical value must be checked against what
   the domain wants, and against the OpenAPI enums.
6. **Where a rule is allowed to get its data.** Station capacity is
   `stationCount x StationStandard` composed at read time from local data
   (ADR 0002); a count has no throughput of its own, and the standard is an
   operator-declared parameter of this context. Flag a change that stores a
   derived capacity, invents a standard, or reaches into a sibling context
   or into stock levels.
7. **Terms the model deliberately keeps unimplemented.**
   `ProcessCapacityRegistered` and `ProcessCapacityChanged` are vocabulary
   only; a change that references them as if published is wrong until it
   adds the raise path, the encoder case and the AsyncAPI message.
8. **New terminology without a doc update.** A genuinely new concept needs an
   entry in `.claude/rules/domain-model.md` (and the ubiquitous-language page)
   in the same PR, or code and prose fork.

## Output format

For each finding: the term involved, where it is in the domain-model rule (or
"not yet documented"), where the drift is in code, and a one-sentence
recommendation (rename, document, or confirm it is an intentional new
concept). If the change introduces no new concepts and uses the existing
vocabulary correctly, say so plainly.

This command never modifies files; it is advisory input for the author.
