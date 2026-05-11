# Sprint Plan: Enrichment Coalesce-on-Overlap

## Summary

Implement the coalesce-then-add semantics for event-level enrichment column overlaps, replacing the current REPLACE behavior shipped in Sprint A (`0.9.0-dev.2`). When an enrichment column matches an existing column on `enhanced_events`, the package emits `coalesce(enrich_<name>.<col>, <original_expr>) as <col>` so a missed JOIN falls back to the existing value rather than emitting NULL.

**Duration:** Single session (~1 hour)
**Dependencies:** Design doc updates shipped on main in commit `7d97e41` (data-enrichments.md Q13/Q17). No code dependencies — operates on the existing `enrichmentColumns` map from `utils.buildEnrichments`.
**Risk Level:** Low — small contained change to one block in `tables/ga4EventsEnhanced/index.js`, behavior change is strictly additive (NULL → existing-value) and pre-1.0 per design doc.
**Design doc:** [data-enrichments.md](data-enrichments.md) Q13 (event-level) and Q17 (item-level — out of scope, deferred to Sprint B).

## Current Status Analysis

### Recent comparable work

| Sprint / commit | LOC | Sessions |
|---|---|---|
| `enhanced-events-explicit-columns` M2 (overlap-filter cleanup) | +35 / −96 | 1 |
| `enrichment-cte-generation` M2 (call-site migration) | +4 / −58 | 1 |
| `buildPassThroughs` extraction (M3 of event-data sprint) | +161 / −18 | 1 |

This sprint is even smaller than the recent precedents — a localized post-processing step on the enrichment column map.

### Velocity

- **Pace:** mid-sized refactors of this shape have landed in single sessions consistently.
- **Estimated capacity:** ~50 LOC net (+30 code / +20 test / small README rewrite). Single-session.

### Remaining from Design Doc

This sprint covers only the **event-level** half of the design doc updates (Q13). The item-level half (Q17) ships when Sprint B (item-level enrichments) is implemented — the coalesce-then-add semantics there are part of that Sprint B's design, not this one.

## Proposed Milestones

Two milestones with M1 → M2 dependency: M1 changes the code and test assertions; M2 updates user-facing README to document what now ships. The split satisfies the validator's 2-milestone minimum and has a natural dependency boundary — README documents shipped behavior, so it must follow the code change.

### Milestone 1: M1_IMPLEMENT_COALESCE

**Goal:** Replace the current REPLACE behavior in `tables/ga4EventsEnhanced/index.js` with coalesce-then-add. Update the affected enrichment tests to assert the new SQL form. Existing 494 tests pass after updates.

**Estimated:** ~30 LOC implementation + ~20 LOC test updates = ~50 LOC
**Duration:** ~45 minutes

**Tasks:**

1. **Build `preEnrichmentExpressions` map** at the call site in [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js), positioned after `utils.buildEnrichments` returns but before the `enhanced_events` step construction. The map contains every column already mapped to its qualified source expression before enrichment is layered on:
   - All entries from `finalColumnOrder` (e.g. `event_date: 'event_data.event_date'`)
   - All entries from `itemListOverrides` (e.g. `items: 'coalesce(items_rebuilt.items, event_data.items)'`)
   - Pass-throughs from `event_data.select.columns` (each non-undefined key qualifies to `event_data.<col>`)
   - Pass-throughs from `session_data.select.columns` (each key qualifies to `session_data.<col>`)
   - Skip duplicates — first writer wins (finalColumnOrder takes precedence over pass-throughs).

2. **Post-process `enrichmentColumns`** into a `wrappedEnrichmentColumns` map. For each `[col, enrichExpr]` entry from `utils.buildEnrichments`:
   - If `col` is in `preEnrichmentExpressions`, emit `coalesce(${enrichExpr}, ${preEnrichmentExpressions[col]})` — the coalesce-then-add case.
   - Otherwise emit `enrichExpr` unchanged — the additive case.

3. **Swap the spread**: replace `...enrichmentColumns` with `...wrappedEnrichmentColumns` in `enhanced_events.select.columns`. The `alreadyMapped` array still receives `enrichmentColumnNames` so the downstream pass-through builder continues to suppress the original column.

4. **Update affected enrichment tests** in [tests/enrichments.test.js](../../tests/enrichments.test.js):
   - The two override tests (page_title via `eventParamsToColumns`; app_info via the GA4 pass-through) currently assert `enrich_X.col as col` appears in the SELECT. Update them to assert `coalesce(enrich_X.col, <expected_original>) as col` instead.
   - The session_data overlap test (merged_user_id) similarly needs the assertion updated to expect `coalesce(enrich_users.merged_user_id, session_data.merged_user_id)`.
   - The purely additive test (`custom_cohort_score`) is unaffected — no coalesce since there's no original to fall back to. Confirm it still passes unchanged.
   - Add one new test specifically asserting the coalesce semantics: an event-level enrichment with a known overlap (e.g. `app_info`), verify the SELECT contains `coalesce(enrich_app.app_info, event_data.app_info) as app_info`.

5. **Run full test suite** — expect all tests pass (494 baseline → 495 after adding one new test). Also generate SQL for a representative config and visually confirm the coalesce expression appears as expected.

**Acceptance criteria:**
- [ ] `preEnrichmentExpressions` computed at the call site with entries from `finalColumnOrder`, `itemListOverrides`, `event_data.select.columns` pass-throughs, and `session_data.select.columns` pass-throughs.
- [ ] `wrappedEnrichmentColumns` post-processing emits `coalesce(...)` for overlap and bare `enrich_X.col` for additive columns.
- [ ] `enhanced_events.select.columns` spreads `wrappedEnrichmentColumns` (not `enrichmentColumns`).
- [ ] Three existing enrichment-overlap tests updated to assert coalesce form.
- [ ] One new test specifically asserts coalesce form for a wildcard-overlap case (`app_info`).
- [ ] All 495 tests pass.
- [ ] Linting clean.
- [ ] Generated SQL for a representative overlap config contains `coalesce(enrich_X.col, <original>) as col`.

**Risks:**
- **Source-expression resolution for `event_data` pass-throughs.** Pre-enrichment, `event_data.select.columns` contains entries with transformation expressions (e.g. `event_name: 'event_name'`, `user_traffic_source: 'traffic_source'`). The `preEnrichmentExpressions` value should be `event_data.<col>` — the qualified reference to the column AFTER it lands in `event_data`, NOT the raw transformation expression. The key check is membership; the value is always `event_data.<col>` regardless of how `event_data` produced it. Mitigation: write the map as `event_data.${col}` directly, ignoring the value-side of `eventDataStep.select.columns`.
- **`undefined`-valued entries** (user-exclusion sentinels from `getExcludedColumns`) in `eventDataStep.select.columns`. These columns are excluded from `event_data`'s SELECT and don't exist downstream. Must be skipped when building `preEnrichmentExpressions`. Mitigation: `if (eventDataStep.select.columns[col] === undefined) continue;` guard.
- **Test-update brittleness.** The override tests currently assert `enrich_X.col as col` substrings. Updating those substrings to `coalesce(enrich_X.col, ...)` form is mechanical — the rest of the assertion shape (column appears once in SELECT, source pass-through doesn't double-emit) remains valid.

### Milestone 2: M2_UPDATE_README

**Goal:** Update the user-facing README to describe coalesce-then-add semantics, replacing the REPLACE description. Update the `columns` field-table row and the "Replace-or-add semantics" paragraph in the data-enrichments section.

**Estimated:** ~15 LOC rewritten in README
**Duration:** ~15 minutes
**Dependencies:** M1_IMPLEMENT_COALESCE

**Tasks:**

1. **Update the `columns` field-table row** at [README.md:540](../../README.md#L540): replace "Names matching existing columns REPLACE them." with copy describing the coalesce semantics.

2. **Update the "Replace-or-add semantics" paragraph** at [README.md:543](../../README.md#L543) (and rename it to "Coalesce-or-add semantics" or similar): describe that overlapping enrichment columns emit `coalesce(enrich_<name>.<col>, <original>)` so missed JOINs fall back to the existing value. Note no opt-out flag.

3. **Optionally extend the example block** in the data-enrichments README section with a one-liner explaining a real-world case (e.g. "for a `page_title` enrichment from a metadata table joined on `page_location`, rows where `page_location` doesn't exist in the dim keep the original promoted `page_title` value").

4. **Run `npm run readme`** to regenerate the TOC if any new headings are added.

5. **Final smoke test** — run the full test suite to confirm README changes didn't accidentally touch anything code-related (no expected change; defensive).

**Acceptance criteria:**
- [ ] [README.md:540](../../README.md#L540) field-row reflects coalesce semantics.
- [ ] [README.md:543](../../README.md#L543) paragraph (or equivalent heading) describes coalesce-or-add behavior.
- [ ] Grep for "REPLACE" (case-insensitive, in enrichment context) returns zero results in README.
- [ ] All 495 tests still pass.
- [ ] TOC regenerated if needed.

**Risks:**
- None notable — pure documentation update.

## Success Metrics

- All tests passing: 495/495 (494 baseline + 1 new coalesce test).
- Linting clean.
- Generated SQL for an overlap config contains `coalesce(enrich_X.col, <original>) as col` instead of bare `enrich_X.col as col`.
- README accurately describes shipped behavior.
- No CHANGELOG entry needed (pre-1.0 feature, behavior refinement in dev versions).
- Documentation: this sprint plan moves from `planned/` to `implemented/` after merge. [data-enrichments.md](data-enrichments.md) stays in `planned/` (Sprint B item-level work remains).

## Dependencies

None active. Design doc updates already on main (commit `7d97e41`).

## Open Questions

None. Design doc resolved everything (default coalesce, no opt-out, no schema change).

## Notes

- Item-level (Q17) is explicitly out of scope. The design doc updates already cover item-level for symmetry, but the implementation lives in the future Sprint B (item-level enrichments).
- This is a behavior refinement, not a new feature — pre-1.0 versioning makes this defensible without a deprecation cycle.
- The post-processing approach keeps `utils.buildEnrichments` table-agnostic — exactly the design Q2 from the enrichment-cte-generation doc set up. Future modeled tables can apply the same coalesce post-processing or not, per their own column-overlap rules.

---

**Document created**: 2026-05-11
