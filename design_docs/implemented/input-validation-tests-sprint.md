# Sprint Plan: Input Validation Tests

## Summary
Add dedicated unit tests for the validation layer (`inputValidation.js` and `tables/ga4EventsEnhanced/validation.js`), covering all error paths, boundary conditions, and the `skipDataformContextFields` option.

**Duration:** 1 session (~3 hours)
**Dependencies:** None
**Risk Level:** Low

## Current Status Analysis

### Completed Recently
- Modular table structure refactor: ~600 LOC across 6 files in 1 day (2026-04-09)
- Integration test improvements: ~60 LOC in 1 day (2026-04-10)
- Pre-operations tests: 545 LOC (35 tests) — reference for test pattern
- Documentation tests: 410 LOC (45 tests) — reference for test pattern

### Velocity
- Recent average: ~400-600 LOC/day for test files
- Test-to-source ratio in this project: ~2:1 (280 LOC source -> ~500-600 LOC tests expected)

### Remaining from Design Doc
- Milestone 1: validateBaseConfig tests (~200 LOC, ~25 tests)
- Milestone 2: validateEnhancedEventsConfig tests (~350 LOC, ~45 tests)
- Milestone 3: Wire up npm scripts (~5 LOC)

## Proposed Milestones

### Milestone 1: Test scaffolding + validateBaseConfig tests
**Goal:** Create test file with helpers and full coverage of `inputValidation.js` (8 field groups, ~20 error paths)
**Estimated:** ~200 LOC (~25 tests)

**Tasks:**
- Create `tests/inputValidation.test.js` with test runner and `validBaseConfig()` factory
- Tests for config object type (null, undefined, array, string)
- Tests for `self` and `incremental` (Dataform context fields)
- Tests for `skipDataformContextFields` option
- Tests for `test` field (optional boolean)
- Tests for `testConfig` (optional object, dateRangeStart/dateRangeEnd)
- Tests for `preOperations` (required object, all sub-fields)

**Acceptance Criteria:**
- [ ] Every error path in `validateBaseConfig` has at least one test
- [ ] `skipDataformContextFields` option tested
- [ ] Valid config passes without error
- [ ] All tests passing

### Milestone 2: validateEnhancedEventsConfig tests
**Goal:** Full coverage of `tables/ga4EventsEnhanced/validation.js` (12+ field groups, ~30 error paths)
**Estimated:** ~350 LOC (~45 tests)

**Tasks:**
- Create `validEnhancedConfig()` factory extending `validBaseConfig()`
- Tests for `Config validation:` error message prefix
- Tests for `sourceTable` (missing, empty, wrong format, valid string, valid Dataform ref)
- Tests for `schemaLock` (optional, format, date validity, minimum date)
- Tests for `includedExportTypes` (missing, wrong type, missing keys, all-false)
- Tests for `timezone` (missing, empty, valid)
- Tests for `dataIsFinal` (missing, invalid method, DAY_THRESHOLD without dayThreshold, EXPORT_TYPE without daily)
- Tests for `bufferDays` (missing, non-integer, negative, valid)
- Tests for string array fields (6 arrays: missing, non-array, empty strings)
- Tests for `eventParamsToColumns` (missing, invalid items, valid types, columnName)
- Test that a fully valid config passes

**Acceptance Criteria:**
- [ ] Every error path in `validateEnhancedEventsConfig` has at least one test
- [ ] Error message prefix `Config validation:` verified
- [ ] Valid full config passes without error
- [ ] All tests passing

### Milestone 3: Wire up npm scripts
**Goal:** Add test script and verify all tests pass together
**Estimated:** ~5 LOC

**Tasks:**
- Add `test:validation` script to `package.json`
- Add to the main `test` script chain
- Run `npm test` to verify everything passes together

**Acceptance Criteria:**
- [ ] `npm run test:validation` works standalone
- [ ] `npm test` runs all test suites including validation
- [ ] All 118+ existing tests still pass

## Success Metrics
- ~70 new test cases covering all validation error paths
- Zero BigQuery calls (pure Node.js)
- Sub-second execution
- All existing tests unaffected

## Dependencies
- None — pure Node.js tests, no external services

## Notes
- Test file pattern follows `preOperations.test.js` conventions (raw `assert` + custom runner)
- No test framework dependency needed
- Both source files total 280 LOC — expected test file ~550 LOC
