# Sprint Plan: Event Data Explicit Column Listing

## Summary

Refactor `eventDataStep` to enumerate every GA4 export column explicitly (transforms + pass-throughs) instead of using a `* except (...)` wildcard, and collapse the asymmetric enrichment EXCEPT filter to a symmetric form. Eliminates a structural asymmetry between `event_data` and `session_data` that complicated Sprint A's enrichment code.

**Duration:** Half a session (~2 hours)
**Dependencies:** Sprint A ([event-level-enrichments-sprint.md](event-level-enrichments-sprint.md)) shipped in `0.9.0-dev.2`. No other blockers.
**Risk Level:** Low
**Design doc:** [event-data-explicit-columns.md](event-data-explicit-columns.md) — all three design questions RESOLVED, no open questions remain.

## Current Status Analysis

### Completed Recently (last 14 days)

| Sprint / commit | LOC (insertions) | Session count |
|---|---|---|
| Sprint A: event-level data enrichments | 1249 | 1 |
| items-cte prep: rename + event_date | 188 | 1 |
| Fix invalid EXCEPT for additive enrichment | 61 | 1 |
| Fix misleading ctx.ref/ref in docs | 34 | 1 |
| data-enrichments design doc | 670 | 1 |

### Velocity

- **Comparable refactor:** items-cte prep (~190 LOC, single session) — closest precedent in scope and shape.
- **Recent pace:** mid-sized sprints (50–250 LOC) consistently land in a single session with full test coverage.
- **Estimated capacity for this sprint:** ~60 LOC — well within single-session bounds.

### Remaining from Design Doc

This sprint covers the entire scope of [event-data-explicit-columns.md](event-data-explicit-columns.md). No follow-up sprints planned for this design doc.

## Proposed Milestones

Two milestones split along a natural dependency boundary: M1 replaces the wildcard with explicit listing (the structural change); M2 consumes the new structure by collapsing the enrichment filter. M1 lands first, and after M1 the package is in a fully working intermediate state — tests pass, SQL is byte-identical, but the enrichment filter still uses its asymmetric form. M2 then simplifies the filter now that `eventDataStep`'s column set is fully knowable from `Object.keys`.

### Milestone 1: M1_EXPLICIT_COLUMN_LISTING

**Goal:** Replace `eventDataStep`'s `[sql]other_columns` wildcard getter with an explicit pass-through builder; verify with a structural test. No changes to the enrichment EXCEPT filter yet.

**Estimated:** ~30 LOC implementation + ~20 LOC tests = ~50 LOC
**Duration:** ~1 hour

**Tasks:**

1. **Export `ga4ExportColumns` array** from [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js). Lift the array literal currently inside `isGa4ExportColumn` to a module-level `const ga4ExportColumns = [...]`; refactor `isGa4ExportColumn` to read from it (`ga4ExportColumns.includes(columnName)`); add `ga4ExportColumns` to `module.exports`. ~5 LOC.

2. **Replace `eventDataStep`'s wildcard getter with pass-through builder** in [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js) (~lines 201-250). Lift the explicit `select.columns` literal into a named const (`eventDataExplicitColumns`), build a `eventDataPassThroughs` object by iterating `helpers.ga4ExportColumns` and adding entries for columns not already in `eventDataExplicitColumns`, then spread both into `eventDataStep.select.columns`. Remove the `get '[sql]other_columns'()` getter entirely. ~25 LOC net.

3. **Add structural test** in `tests/ga4EventsEnhanced.test.js` (or new file) asserting that `event_data`'s emitted column list matches the expected union of explicit transforms and pass-through entries derived from `helpers.ga4ExportColumns ∪ promotedEventParameters − mergedConfig.excludedColumns`. ~20 LOC.

4. **Run full test suite + diff generated SQL** on three representative configs (default, with `excludedColumns`, with `eventParamsToColumns`) — expect byte-identical emitted column set (modulo column ordering).

**Acceptance criteria:**
- [ ] `helpers.ga4ExportColumns` exported and used as the single source of truth in both `isGa4ExportColumn` and the pass-through builder.
- [ ] `[sql]other_columns` getter no longer present in `eventDataStep`.
- [ ] `Object.keys(eventDataStep.select.columns).filter(k => eventDataStep.select.columns[k] !== undefined)` is the complete column set of `event_data.*`.
- [ ] New structural test passes; existing tests pass unchanged (461 baseline → 462).
- [ ] Generated SQL diff on the three verification configs shows no column-set changes.
- [ ] Linting clean.

**Risks:**

- **Pass-through column ordering may shift inside `event_data`.** Mitigation: the outer `enhanced_events` SELECT controls final column order via `finalColumnOrder`; pass-through ordering inside `event_data` is internal and not user-facing. Confirm during the SQL diff that the outer SELECT shape is unchanged.
- **queryBuilder `undefined` behavior.** Resolved in design doc Q2: `(c in obj)` check in the pass-through builder skips user-excluded keys, and the queryBuilder drops `undefined` entries from the rendered SELECT regardless of the wildcard's presence. Verified end-to-end via the `excludedColumns: ['app_info', 'publisher']` config diff.

### Milestone 2: M2_COLLAPSE_ENRICHMENT_FILTER

**Goal:** Now that `eventDataStep`'s column set is fully knowable from `Object.keys`, collapse the asymmetric enrichment EXCEPT filter to a single symmetric form across both wildcards. No new behavior — pure simplification.

**Estimated:** ~−10 LOC net (delete more than add)
**Duration:** ~30 minutes
**Dependencies:** M1_EXPLICIT_COLUMN_LISTING

**Tasks:**

1. **Collapse the enrichment EXCEPT filter** at [tables/ga4EventsEnhanced/index.js:388-401](../../tables/ga4EventsEnhanced/index.js#L388-L401) to:
   ```js
   const eventDataExplicit = new Set(Object.keys(eventDataStep.select.columns));
   const sessionDataExplicit = new Set(Object.keys(sessionDataStep.select.columns));
   const eventDataEnrichmentExcept = enrichmentExcludedColumns.filter(c => eventDataExplicit.has(c));
   const sessionDataEnrichmentExcept = enrichmentExcludedColumns.filter(c => sessionDataExplicit.has(c));
   ```
   Drop the `userExcluded` set and the `helpers.isGa4ExportColumn` call from this code path. Update the inline comment to reflect symmetric semantics.

2. **Verify enrichment tests still pass** — especially the existing wildcard-overlap (`app_info`), explicit-column-overlap (`page_title`), session-data-overlap (`merged_user_id`), and purely-additive (`user_segment_test`) cases at [tests/enrichments.test.js:262-340](../../tests/enrichments.test.js#L262-L340).

3. **BigQuery dry-run** of the Sprint A bug-repro config (purely additive enrichment `user_segment_test`) — still produces valid SQL.

**Acceptance criteria:**
- [ ] Enrichment EXCEPT filter uses identical predicate shape (`Object.keys(...).has(c)`) for both `event_data` and `session_data`.
- [ ] `helpers.isGa4ExportColumn` no longer referenced from the enrichment block at [tables/ga4EventsEnhanced/index.js:328-402](../../tables/ga4EventsEnhanced/index.js#L328-L402).
- [ ] All four enrichment overlap/additive scenarios from Sprint A continue to pass.
- [ ] BigQuery dry-run of `user_segment_test` config produces valid SQL.
- [ ] All 462 tests pass.
- [ ] Linting clean.

**Risks:**

- **Filter collapse changes the EXCEPT list contents.** Mitigation: M1 ensures `eventDataExplicit` now contains every GA4 export column the user didn't exclude — so any enrichment column matching a GA4 pass-through gets caught by `eventDataExplicit.has(c)` just as the prior asymmetric predicate caught it via `isGa4ExportColumn(c) && !userExcluded.has(c)`. The two predicates are now equivalent after M1; the existing wildcard-overlap test (`app_info`) verifies this end-to-end.

## Success Metrics

- All tests passing: 462/462 (461 existing + 1 new structural test).
- Linting clean.
- Documentation: design doc [event-data-explicit-columns.md](event-data-explicit-columns.md) moves from `planned/` to `implemented/` after merge.
- Code reduction: net ~−10 LOC in `tables/ga4EventsEnhanced/index.js` (delete getter + collapse filter), offset by ~+20 LOC of structural test.

## Dependencies

None active. Sprint A shipped; the asymmetric filter it introduced is what this refactor consolidates.

## Open Questions

None. Design doc resolved Q1 (export array), Q2 (queryBuilder behavior), Q3 (column-description audit) before sprint planning began.

## Notes

- Single milestone is correct here: the three code changes (export array, replace wildcard, collapse filter) are tightly coupled and verify together. Splitting would yield meaningless intermediate states (e.g., array exported but unused).
- No CHANGELOG entry expected — this is a pure internal refactor with no user-visible behavior change. The design doc and commit message carry the rationale.
- After completion, the design doc moves to `design_docs/implemented/` per the project's snapshot convention ([AGENTS.md](../../AGENTS.md) "Design docs").

---

**Document created**: 2026-05-11
