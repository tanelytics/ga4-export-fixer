# Sprint Plan: Daily Quality Assertion

## Summary

Implement a day-level data quality assertion for ga4_events_enhanced that validates session counts, event counts, item_revenue totals, data completeness, and non-final data integrity against the raw GA4 export -- all in a single query.

**Duration:** <1 day (single session)
**Dependencies:** Assertion infrastructure (already implemented)
**Risk Level:** Low

## Current Status Analysis

### Completed Recently
- `itemRevenue` assertion: ~150 LOC impl + ~180 LOC tests, single commit
- Assertion sourceTable input validation: follow-up commit
- Integration test assertion result collection: ~50 LOC across 2 files

### Velocity
- The `itemRevenue` assertion (same pattern, same architecture) was completed in one session
- This feature is structurally identical -- same function signature, same config merge, same test approach

### Remaining from Design Doc
- `dailyQuality.js`: ~120 LOC implementation
- `assertions/index.js`: +2 LOC
- `tests/assertions.test.js`: +30 LOC

## Proposed Milestones

### Milestone 1: Implement dailyQuality assertion generator

**Goal:** Create `dailyQuality.js` with the SQL generator function, wire it into the assertions index, and export it.

**Estimated:** ~120 LOC implementation

**Tasks:**
- Create `tables/ga4EventsEnhanced/assertions/dailyQuality.js` with:
  - `buildAssertionDateFilter` (reuse pattern from itemRevenue)
  - `_generateDailyQualityAssertionSql` (core SQL generation)
  - `generateDailyQualityAssertionSql` (exported wrapper with config merge + validation)
- Add `dailyQuality` export to `assertions/index.js`

**Acceptance Criteria:**
- [ ] `ga4EventsEnhanced.assertions.dailyQuality(tableRef, config)` returns a SQL string
- [ ] SQL includes all 5 violation types: MISSING_DAY, SESSION_COUNT_MISMATCH, EVENT_COUNT_MISMATCH, REVENUE_MISMATCH, NON_FINAL_EXCESS_EVENTS
- [ ] Raw side applies same date filters, event exclusions, and data_is_final logic
- [ ] Session count derived using `concat(user_pseudo_id, ga_session_id)` on raw side
- [ ] Throws helpful error if sourceTable is a Dataform reference object
- [ ] Throws if tableRef is empty/missing

### Milestone 2: Add BigQuery dry-run tests

**Goal:** Validate the generated SQL against BigQuery with the same configuration matrix used for itemRevenue tests.

**Estimated:** ~30 LOC test additions

**Tasks:**
- Update `testTableRef` in `tests/assertions.test.js` to include `session_id` column (needed by dailyQuality but not itemRevenue)
- Add `dailyQuality` test configurations (same 8 configs as itemRevenue)
- Run `npm run test:assertions` and verify all pass
- Run `npm test` to verify no regressions

**Acceptance Criteria:**
- [ ] All 8 dailyQuality dry-run configurations pass BigQuery validation
- [ ] All existing itemRevenue tests still pass
- [ ] `npm test` passes (full test suite)

**Risks:**
- `testTableRef` change (adding session_id) could break existing itemRevenue tests if the subquery syntax changes -- Mitigation: keep the existing columns intact, only add session_id as an additional column

## Success Metrics
- All BigQuery dry-run tests pass for both assertions
- Full test suite (`npm test`) passes
- No regressions in existing itemRevenue assertion

## Notes
- The `buildAssertionDateFilter` function can be extracted from `itemRevenue.js` to share between both assertions, or duplicated. Given it's ~10 lines and the design doc says helpers stay flat, duplicating within the assertions directory is acceptable for now.
- The testTableRef needs session_id for dailyQuality but itemRevenue doesn't use it -- adding the column to the shared testTableRef is harmless (extra column is ignored by itemRevenue's SQL).
