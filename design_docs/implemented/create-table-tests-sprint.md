# Sprint Plan: createTable.js Unit Tests

## Summary
Add dedicated unit tests for `createTable.js` using mocked `publish()` and table module interfaces, covering table naming, config merging, description generation, validation wiring, and mutation safety.

**Duration:** 1 session (~2 hours)
**Dependencies:** None
**Risk Level:** Low

## Current Status Analysis

### Completed Recently
- Input validation tests: 150 tests, ~550 LOC in 1 session (2026-04-10)
- Integration test improvements: ~60 LOC (2026-04-10)

### Velocity
- Recent average: ~500-600 LOC/session for test files
- `createTable.js` is 58 LOC — expected test file ~250-300 LOC

### Remaining from Design Doc
- Milestone 1: Mock infrastructure + publish wiring tests (~100 LOC, ~10 tests)
- Milestone 2: Config merging, naming, description tests (~120 LOC, ~15 tests)
- Milestone 3: Mutation safety + npm scripts (~80 LOC, ~5 tests + wiring)

## Proposed Milestones

### Milestone 1: Mock infrastructure + publish wiring tests
**Goal:** Create test file with mock `publish()` and table module, test basic wiring
**Estimated:** ~100 LOC (~10 tests)

**Tasks:**
- Create `tests/createTable.test.js` with `mockPublish()` and `mockTableModule()` helpers
- Tests that `publish()` is called with correct name and config
- Tests that `preOps` callback delegates to `setPreOperations`
- Tests that `query` callback delegates to `generateSql`
- Tests that `validate` is called with `skipDataformContextFields: true`

**Acceptance Criteria:**
- [ ] Mock infrastructure captures publish arguments and callbacks
- [ ] Publish/preOps/query wiring verified
- [ ] Validation option verified
- [ ] All tests passing

### Milestone 2: Config merging, naming, and description tests
**Goal:** Test table naming, config merge order, and description fallback logic
**Estimated:** ~120 LOC (~15 tests)

**Tasks:**
- Tests for table naming (analytics_ prefix stripping, default name composition)
- Tests for dataset extraction and schema assignment
- Tests for config merge order (defaults → dynamic fields → user overrides)
- Tests for auto-generated description fallback
- Tests for user-provided description preservation
- Tests for column descriptions passed through correctly

**Acceptance Criteria:**
- [ ] Table naming with analytics_ prefix stripping tested
- [ ] Config merge order tested (user overrides win)
- [ ] Description fallback vs user override tested
- [ ] Column descriptions wiring tested

### Milestone 3: Mutation safety + npm scripts
**Goal:** Test deep-clone behavior and wire up npm scripts
**Estimated:** ~80 LOC (~5 tests + wiring)

**Tasks:**
- Tests that multiple `createTable` calls don't share nested config objects
- Tests that `mergedConfig` is not mutated by `getTableDescription`
- Add `test:createTable` script to `package.json`
- Add to main `test` script chain
- Run `npm test` to verify everything passes

**Acceptance Criteria:**
- [ ] Deep-clone prevents cross-call mutation
- [ ] `npm run test:createTable` works standalone
- [ ] `npm test` runs all suites
- [ ] All existing tests still pass

## Success Metrics
- ~30 new test cases covering all createTable behaviors
- Zero BigQuery calls (pure Node.js)
- Sub-second execution
- All existing tests unaffected

## Dependencies
- None — pure Node.js mock-based tests

## Notes
- `createTable.js` is only 58 LOC but has subtle behaviors (deep clone, merge order, frozen object workaround)
- Mock approach avoids needing Dataform runtime — tests the orchestration logic in isolation
- The `preOps` and `query` callbacks receive a mock `ctx` object simulating Dataform's context
