# Sprint Plan: Table Assertions (Item Revenue)

## Summary
Implement the assertion infrastructure for ga4_events_enhanced and the first assertion (item revenue reconciliation). This adds exportable SQL generators under `tables/ga4EventsEnhanced/assertions/` that compare item_revenue between the enhanced table and raw export data.

**Duration:** 1 day (2 milestones)
**Dependencies:** None
**Risk Level:** Low

## Current Status Analysis

### Completed Recently
- Item list attribution feature: ~17 LOC changes to index.js over several iterations
- Performance improvements to temp event row ID

### Velocity
- Recent work has been focused on smaller iterative changes
- This feature is self-contained with no cross-cutting concerns

### Remaining from Design Doc
- Milestone 1: Assertion SQL generator (~80 LOC)
- Milestone 2: Tests (~60 LOC)

## Proposed Milestones

### Milestone 1: Item Revenue Assertion SQL Generator
**Goal:** Create `itemRevenue.js` that generates a reconciliation query comparing item_revenue between the enhanced table and raw GA4 export data, plus the assertion index and module export wiring.
**Estimated:** ~80 LOC implementation + ~10 LOC wiring = ~90 LOC
**Duration:** Half day

**Tasks:**
1. Create `tables/ga4EventsEnhanced/assertions/itemRevenue.js`:
   - Internal `_generateItemRevenueAssertionSql(tableRef, mergedConfig)` that builds the SQL query
   - Enhanced side CTE: query `tableRef` for item_revenue grouped by (event_date, item_id), filtered to `data_is_final = true` and last 5 days, ecommerce events only
   - Raw side CTE: query `config.sourceTable` with per-export-type date filters built from `ga4ExportDateFilter()` and `config.includedExportTypes`, excluded events from config, `isFinalData()` condition from `config.dataIsFinal`, same 5-day window
   - FULL OUTER JOIN on (event_date, item_id), return mismatched rows with both sides' values for debugging
   - Exported wrapper `generateItemRevenueAssertionSql(tableRef, config)` that merges config with defaults and validates before delegating
2. Create `tables/ga4EventsEnhanced/assertions/index.js` re-exporting `itemRevenue`
3. Add `assertions` to `tables/ga4EventsEnhanced/index.js` exports (hidden -- not in docs)

**Acceptance Criteria:**
- [ ] `ga4EventsEnhanced.assertions.itemRevenue(tableRef, config)` returns a SQL string
- [ ] Raw side uses `ga4ExportDateFilter()` (per-export-type) with fixed 5-day range, not `ga4ExportDateFilters()`
- [ ] Raw side applies `includedExportTypes`, `excludedEvents`, and `dataIsFinal` from config
- [ ] Enhanced side filters to `data_is_final = true`, last 5 days, ecommerce events
- [ ] FULL OUTER JOIN returns rows with both sides' revenue/count for debugging
- [ ] Floating point tolerance via `round(..., 2)`
- [ ] Config merge + validation follows same pattern as `generateSql` wrapper

**Risks:**
- None significant -- follows established patterns in the codebase

### Milestone 2: Tests
**Goal:** Add SQL validation tests for the assertion generator, following the existing BigQuery dry-run test pattern.
**Estimated:** ~60 LOC tests
**Duration:** Half day

**Tasks:**
1. Create `tests/assertions.test.js` following the `ga4EventsEnhanced.test.js` pattern:
   - Test SQL generation with default config (daily+intraday, DAY_THRESHOLD)
   - Test with daily-only export types
   - Test with all three export types enabled (daily+fresh+intraday)
   - Test with EXPORT_TYPE detection method
   - Test with excluded events that include an ecommerce event
   - Test with custom timezone
   - Validate all generated SQL against BigQuery dry-run
2. Register in `tests/testRunner.js`

**Acceptance Criteria:**
- [ ] All assertion SQL variants pass BigQuery dry-run validation
- [ ] Tests cover different `includedExportTypes` combinations
- [ ] Tests cover both `dataIsFinal` detection methods
- [ ] Tests cover excluded events overlapping with ecommerce events
- [ ] All existing tests still pass (no regressions)
- [ ] Test registered in testRunner.js

**Risks:**
- BigQuery dry-run requires valid credentials and source table -- same as existing tests

## Success Metrics
- All new and existing tests pass
- Assertion SQL generates valid BigQuery SQL across all config combinations

## Notes
- This is a hidden feature -- no documentation or public API changes
- The assertion signature `(tableRef, config)` keeps the config object clean for future bundling with `createTable`
- Only the `itemRevenue` assertion is implemented; additional assertions are future work
