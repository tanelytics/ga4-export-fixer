# Sprint Plan: Enhanced Events Explicit Column Listing

## Summary

Replace the two `utils.selectOtherColumns` wildcard calls in the `enhanced_events` step with a new `utils.buildQualifiedPassThroughs(step, alreadyCovered)` utility that returns explicit qualified column entries. Delete the asymmetric overlap-filter block (the `enrichmentColumnNames` filter that exists only to feed `selectOtherColumns`'s `EXCEPT`). Delete the now-unused `selectOtherColumns` itself. Final result: zero wildcards anywhere in the package's generated SQL.

**Duration:** Single session (~1.5 hours)
**Dependencies:** `event-data-explicit-columns` shipped through `0.9.0-dev.6`; `enrichment-cte-generation` shipped (commit `4012d74`). No active blockers.
**Risk Level:** Low
**Design doc:** [enhanced-events-explicit-columns.md](enhanced-events-explicit-columns.md) — all four design questions RESOLVED, no open questions.

## Current Status Analysis

### Completed Recently

| Sprint / commit | LOC | Sessions |
|---|---|---|
| enrichment-cte-generation (just merged) | +200 (M1) / −58 (M2) | 1 |
| buildPassThroughs extraction (M3 of event-data-explicit-columns) | +161 / −18 | 1 (part of larger sprint) |
| event-data-explicit-columns M1 | +168 / −81 | 1 |

### Velocity

- **Direct precedent:** the `buildPassThroughs` and `buildEnrichments` extractions are exactly the same shape — add utility + add unit tests + swap call sites. Both landed in a single session. This sprint sits at comparable scope.
- **Estimated capacity:** ~70 LOC net (+30 new utility / −40 old / +80 tests / 0 in index.js). Single-session.

### Remaining from Design Doc

This sprint covers the entire scope of [enhanced-events-explicit-columns.md](enhanced-events-explicit-columns.md). No follow-up sprints planned for this design doc — it's the final refactor in the multi-step push toward fully-explicit generated SQL.

## Proposed Milestones

Two milestones split along a natural dependency boundary: M1 introduces the new utility independently (callable but unused), M2 swaps the call sites, deletes the overlap-filter block, and deletes the now-orphaned `selectOtherColumns`. Same shape as the previous two refactors.

### Milestone 1: M1_BUILD_QUALIFIED_PASSTHROUGHS_UTILITY

**Goal:** Add `utils.buildQualifiedPassThroughs(step, alreadyCovered)` to utils.js with comprehensive unit tests. Utility is callable and tested in isolation; existing call sites are not yet touched. All 487 existing tests pass unchanged.

**Estimated:** ~30 LOC utility + ~80 LOC unit tests = ~110 LOC
**Duration:** ~45 minutes

**Tasks:**

1. **Add `buildQualifiedPassThroughs` to [utils.js](../../utils.js)** immediately after `buildEnrichments` (the precedent established in the previous sprint). Signature: `(step, alreadyCovered) => { [col]: 'step.name.col' }`. Behavior per the design doc:
   - Iterate `Object.entries(step.select.columns)`.
   - Skip entries whose value is `undefined` (user-exclusion sentinel from `getExcludedColumns`).
   - Skip entries whose key is in `alreadyCovered`.
   - Emit `{ [col]: \`${step.name}.${col}\` }` for each remaining column.
   - Names in `alreadyCovered` that don't exist in `step.select.columns` are silently ignored — the loop only iterates the step.

2. **Export `buildQualifiedPassThroughs`** from `utils.js`'s `module.exports` block, alphabetically near the other `build*` utilities.

3. **Add unit tests** in [tests/utils.test.js](../../tests/utils.test.js) under a new `3. buildQualifiedPassThroughs` section. Test cases:
   - Empty step (no columns) → empty result.
   - All-covered step → empty result.
   - Step with mix of covered and uncovered columns → only uncovered columns appear, each qualified with `step.name`.
   - `undefined`-valued entries (user-exclusion sentinel shape) → skipped.
   - `alreadyCovered` names that don't exist in the step → silently ignored (no error, no output).
   - `alreadyCovered` accepts arrays and Sets identically.
   - Result preserves `Object.entries` iteration order (relevant for downstream SELECT column ordering).

**Acceptance criteria:**
- [ ] `utils.buildQualifiedPassThroughs` exported and importable.
- [ ] All 7 new unit tests pass.
- [ ] All 487 existing tests pass unchanged (no call site touched yet).
- [ ] Linting clean.

**Risks:**
- None notable. The utility is a 10-line for-loop with `Object.entries`/`continue` semantics — straightforward and well-bounded.

### Milestone 2: M2_MIGRATE_ENHANCED_EVENTS_SELECT

**Goal:** Replace the two `utils.selectOtherColumns` calls in `enhanced_events.select.columns` with `utils.buildQualifiedPassThroughs` spreads. Delete the overlap-filter block (4 lines) since `enrichmentColumnNames` flows directly into `alreadyMapped`. Delete `utils.selectOtherColumns` and its export per Q2 (zero callers after the swap). Verify SQL byte-equivalence and continued passing of all enrichment scenarios.

**Estimated:** ~+15 / −60 LOC net −45 (delete filter + delete old utility + small call-site addition)
**Duration:** ~45 minutes
**Dependencies:** M1_BUILD_QUALIFIED_PASSTHROUGHS_UTILITY

**Tasks:**

1. **Build `alreadyMapped` at the call site** — a flat list combining `Object.keys(finalColumnOrder)`, `Object.keys(itemListOverrides)`, `Object.keys(enrichmentColumns)`, the internal-only columns (`'entrances'`, conditional `'session_params_prep'`, `'data_is_final'`, `'export_type'`), and `itemListExcludedColumns`. This is the union of "everything the downstream SELECT already maps explicitly" plus "internal-only columns the wildcard should never have re-emitted."

2. **Replace the two `utils.selectOtherColumns` calls** at [tables/ga4EventsEnhanced/index.js:351-368](../../tables/ga4EventsEnhanced/index.js#L351-L368) with `...utils.buildQualifiedPassThroughs(eventDataStep, alreadyMapped)` and `...utils.buildQualifiedPassThroughs(sessionDataStep, alreadyMapped)`. The `[sql]event_data` and `[sql]session_data` keys disappear entirely.

3. **Delete the overlap-filter block** at [tables/ga4EventsEnhanced/index.js:329-336](../../tables/ga4EventsEnhanced/index.js#L329-L336) (the `eventDataExplicit`, `sessionDataExplicit`, `eventDataEnrichmentExcept`, `sessionDataEnrichmentExcept` declarations). They have no callers after the swap.

4. **Audit and delete `utils.selectOtherColumns`** per Q2:
   - Run a grep for `selectOtherColumns` across the codebase; expect zero remaining callers (only the declaration and export in `utils.js`, plus this design doc).
   - Delete the function declaration in [utils.js:488-521](../../utils.js#L488-L521).
   - Remove `selectOtherColumns` from `module.exports`.
   - (No existing unit tests to delete — confirmed by `git grep selectOtherColumns -- tests/` returning nothing.)

5. **Verify SQL byte-equivalence** for 4 representative configs (default, with `excludedColumns: ['app_info', 'publisher']`, with `eventParamsToColumns: [{name: 'page_title', type: 'string'}]`, with an enrichment using composite joinKey + dedupe). Two expected SQL diffs per config:
   - The `event_data.* except (...)` and `session_data.* except (...)` lines disappear.
   - They're replaced by N explicit `event_data.<col> as <col>` / `session_data.<col> as <col>` lines.
   - **Column SET must be identical, in identical order** across all 4 configs. Verify by extracting column-alias names from the outer SELECT before/after and comparing as sorted lists.

6. **Sprint A bug-repro check.** Regenerate SQL for the purely-additive `user_segment_test` enrichment config. Confirm the column appears in SELECT exactly once and no wildcard EXCEPT lists exist anywhere.

**Acceptance criteria:**
- [ ] `enhanced_events.select.columns` contains no `[sql]event_data` or `[sql]session_data` keys.
- [ ] Overlap-filter block at lines 329-336 deleted.
- [ ] `utils.selectOtherColumns` deleted from `utils.js`; removed from `module.exports`.
- [ ] Grep for `selectOtherColumns` returns zero results across `*.js` files.
- [ ] All 494 tests pass (487 baseline + 7 new from M1).
- [ ] Column SET is identical to pre-refactor baseline across 4 verification configs; column order is identical.
- [ ] Sprint A bug-repro (purely additive `user_segment_test`) still produces valid SQL with the column in SELECT and absent from every EXCEPT (none exist in `enhanced_events` after this refactor; pre-existing EXCEPTs in `event_data` and `session_data` source CTEs are unaffected).
- [ ] Linting clean.

**Risks:**
- **Column ordering shift inside the SELECT.** Wildcard expansion in BigQuery produces columns in the source CTE's declaration order; explicit listing via `buildQualifiedPassThroughs` produces the same order (the utility iterates `Object.entries`, which preserves insertion order). Mitigation: the column-order verification in task 5 catches any drift.
- **Internal-only columns leaking.** `entrances`, `session_params_prep`, the item-list row id, `data_is_final`, and `export_type` are currently excluded from the wildcards via `selectOtherColumns`'s `excludedColumns` argument. After the refactor, they must be in `alreadyMapped` so `buildQualifiedPassThroughs` skips them. Mitigation: the existing tests (especially [tests/enrichments.test.js](../../tests/enrichments.test.js) item-list-attribution cases) immediately fail if any internal column leaks.
- **`undefined`-valued sentinels.** `eventDataStep.select.columns` may contain `{ event_dimensions: undefined, traffic_source: undefined, ... }` entries from `getExcludedColumns()`. `buildQualifiedPassThroughs` must skip these. Mitigation: explicit `if (expr === undefined) continue;` guard in the utility, plus a dedicated unit test in M1.

## Success Metrics

- All tests passing: 494/494 (487 existing + 7 new utility tests).
- Linting clean.
- Net LOC: utils.js −10 (new utility +30, old function −40), tables/ga4EventsEnhanced/index.js −45 (overlap filter −5, wildcard calls swap net 0, internal lines saved), tests/utils.test.js +80. Roughly +25 net.
- No wildcards (`* except` or `step.* except`) in the package's generated SQL.
- Generated SQL column-set identical to pre-refactor across 4 verification configs.
- Documentation: [enhanced-events-explicit-columns.md](enhanced-events-explicit-columns.md) and this sprint plan move from `planned/` to `implemented/` after merge.
- No CHANGELOG entry needed — pure internal refactor with no user-visible behavior change.

## Dependencies

None active. Both predecessor sprints (`event-data-explicit-columns` and `enrichment-cte-generation`) have shipped.

## Open Questions

None. Design doc resolved Q1 (new sibling utility vs. extending buildPassThroughs), Q2 (remove `selectOtherColumns` in same sprint), Q3 (internal-only columns inline at call site), Q4 (column ordering matches today's wildcard expansion) before sprint planning began.

## Notes

- This is the final refactor in the multi-step push toward fully-explicit generated SQL. After it lands, the package's SQL output has no wildcard expansion anywhere — every column is statically knowable from the config.
- The two-milestone split mirrors the previous two refactors (buildPassThroughs and buildEnrichments). M1 introduces a tested utility; M2 consumes it and cleans up.
- `selectOtherColumns` deletion makes this slightly different from the previous extractions — it's an extract-and-replace, not just an extract. M2 carries both responsibilities (swap + delete).

---

**Document created**: 2026-05-11
