# Sprint Plan: Schema Lock Export Type Support

## Summary
Extend the `schemaLock` config option to accept intraday (`"intraday_YYYYMMDD"`) and fresh (`"fresh_YYYYMMDD"`) export table suffixes alongside the existing daily (`"YYYYMMDD"`) format. This is a validation-and-docs change — the SQL generation already works for all three formats.

**Duration:** <1 day (single session)
**Dependencies:** None
**Risk Level:** Low

## Current Status Analysis

### Completed Recently
- createTable tests: ~415 LOC in <1 day (Apr 10)
- input validation tests: ~1008 LOC in <1 day (Apr 10)
- modular table structure refactor: ~518 LOC in <1 day (Apr 9)
- dataform integration tests: ~1434 LOC in <1 day (Apr 8)

### Velocity
- Recent average: 500-1000+ LOC/day for focused features
- This sprint is small (~50 LOC changes + ~40 LOC tests)

### Remaining from Design Doc
- Milestone 1 — Validation: ~15 LOC
- Milestone 2 — Tests: ~40 LOC
- Milestone 3 — README: ~5 LOC

## Proposed Milestones

### Milestone 1: Update validation logic
**Goal:** Accept `"intraday_YYYYMMDD"` and `"fresh_YYYYMMDD"` formats in the schemaLock validator while preserving all existing behavior.
**Estimated:** ~15 LOC
**Duration:** Minutes

**Tasks:**
1. Update regex in `tables/ga4EventsEnhanced/validation.js:45` from `/^\d{8}$/` to `/^(?:(?:intraday|fresh)_)?\d{8}$/`
2. Extract date portion via `.slice(-8)` instead of using the full string for date parsing (lines 49-51)
3. Update the minimum-date comparison to use the extracted date portion (line 57)
4. Update error messages to list all accepted formats (lines 46, 54, 58)

**Acceptance Criteria:**
- [ ] `"20260101"` still accepted (backward compatible)
- [ ] `"intraday_20260101"` accepted
- [ ] `"fresh_20260101"` accepted
- [ ] `"streaming_20260101"` rejected (unknown prefix)
- [ ] `"intraday_20241332"` rejected (invalid date with prefix)
- [ ] `"fresh_20241008"` rejected (below minimum with prefix)
- [ ] All existing validation tests still pass

**Risks:**
- None — the regex change is additive and all existing valid inputs remain valid.

### Milestone 2: Add tests
**Goal:** Add validation and pre-operations tests for the new formats.
**Estimated:** ~40 LOC tests
**Duration:** Minutes

**Tasks:**
1. Add ~6 validation tests in `tests/inputValidation.test.js` after line 569:
   - accepts `"intraday_20260101"`
   - accepts `"fresh_20260101"`
   - accepts `"intraday_20241009"` (minimum date with prefix)
   - rejects `"streaming_20260101"` (unknown prefix)
   - rejects `"intraday_20241332"` (invalid date with prefix)
   - rejects `"fresh_20241008"` (below minimum with prefix)
2. Add ~2 pre-operations tests in `tests/preOperations.test.js` after line 364:
   - intraday schemaLock generates CREATE with correct `LIKE` table
   - fresh schemaLock generates CREATE with correct `LIKE` table

**Acceptance Criteria:**
- [ ] All new tests pass
- [ ] All existing tests pass (`npm test`)
- [ ] Pre-operations tests verify correct table name in generated SQL

**Risks:**
- None — additive test changes only.

### Milestone 3: Update README
**Goal:** Update documentation to reflect the new accepted formats.
**Estimated:** ~5 LOC
**Duration:** Minutes

**Tasks:**
1. Update the config table row for `schemaLock` at `README.md:294` — change type from `string (YYYYMMDD)` to `string` and expand the description
2. Update the example comment at `README.md:207` to mention intraday/fresh formats

**Acceptance Criteria:**
- [ ] Config table reflects all three accepted formats
- [ ] Example shows new format options
- [ ] `npm run readme` succeeds (if it validates README)

**Risks:**
- None.

## Success Metrics
- All existing tests pass (`npm test`)
- 8 new tests pass (6 validation + 2 pre-operations)
- README updated with new format documentation

## Dependencies
- None

## Open Questions
- None

## Notes
- The SQL generation in `createSchemaLockTable` (`preOperations.js:157`) constructs `events_${config.schemaLock}`, which already produces the correct BigQuery table names for all three formats. No SQL changes needed.
- The validation change is backward compatible — the new regex is a superset of the old one.
