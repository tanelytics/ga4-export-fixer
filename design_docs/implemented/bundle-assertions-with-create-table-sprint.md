# Sprint Plan: Bundle Assertions with createTable

## Summary

Add an optional `options` parameter to `createTable` so that passing `{ assert }` automatically creates Dataform assertions (dailyQuality, itemRevenue) alongside the table -- eliminating config duplication, manual table ref resolution, and assertion boilerplate for JS deployment users.

**Duration:** 2 days
**Dependencies:** None (builds on implemented assertion infrastructure and multi-table module architecture)
**Risk Level:** Low

## Current Status Analysis

### Completed Recently
- Daily quality assertion: ~250 LOC (implementation + tests) in 1 day
- Item revenue assertion: ~320 LOC (implementation + tests) in 1 day
- Integration test assertion support: ~80 LOC in 1 day
- Query builder formatting: ~60 LOC in 1 day

### Velocity
- Recent average (last 14 days): ~150-200 implementation LOC/active day (excluding version bumps and package-lock)
- Feature LOC estimate for this sprint: ~170 LOC
- Estimated capacity: comfortable within 2 days

### Remaining from Design Doc
- All 5 phases from the design doc, condensed into 3 milestones below

## Proposed Milestones

### Milestone 1: Assertion interface + createTable extension
**Goal:** Export internal assertion generators from assertion modules, add `assertions` property to the ga4EventsEnhanced table module, and extend `createTable.js` to wire assertions when `options.assert` is provided.
**Estimated:** ~60 LOC implementation
**Duration:** 0.5 days

**Tasks:**
- Export `_generateDailyQualityAssertionSql` from `dailyQuality.js` and `_generateItemRevenueAssertionSql` from `itemRevenue.js`
- Add `assertions` property to `tableModule` in `tables/ga4EventsEnhanced/index.js` with `{ dailyQuality: { generate, defaultName }, itemRevenue: { generate, defaultName } }`
- Update `createEnhancedEventsTable` to accept and forward `options` parameter
- Extend `createTable` in `createTable.js` with optional fourth `options` parameter
- Implement assertion creation loop: iterate `tableModule.assertions`, check enable/disable via `options.assertions`, build assertion name (`{tableName}_{defaultName}`), call `options.assert()` with inherited schema/tags, resolve `sourceTable` via `ctx.ref()` in `.query()` callback

**Key implementation details:**
- Assertion `.query()` callback must resolve `sourceTable` from Dataform ref object via `ctx.ref()` when needed (eliminates the manual re-resolution pain point)
- `tableRef` is resolved via `ctx.ref(tableName)` inside the assertion's `.query()` callback
- `mergedConfig` is reused from the table creation step -- no re-merge or re-validation
- When `options.assertions[key]` is an object, its properties override the default assertion Dataform config (name, schema, tags)

**Files:**
| File | Change |
|------|--------|
| `tables/ga4EventsEnhanced/assertions/dailyQuality.js` | Export `_generateDailyQualityAssertionSql` |
| `tables/ga4EventsEnhanced/assertions/itemRevenue.js` | Export `_generateItemRevenueAssertionSql` |
| `tables/ga4EventsEnhanced/index.js` | Add `assertions` to `tableModule`, forward `options` in `createEnhancedEventsTable` |
| `createTable.js` | Add `options` parameter, assertion creation loop |

**Acceptance Criteria:**
- [ ] `createTable(publish, config, tableModule, { assert })` calls `assert()` for each assertion in `tableModule.assertions`
- [ ] `createTable(publish, config, tableModule)` works identically to today (no `assert` calls)
- [ ] Assertions inherit table name, schema, and tags from `dataformTableConfig`
- [ ] `options.assertions: { dailyQuality: false }` skips dailyQuality
- [ ] `options.assertions: { dailyQuality: { schema: 'custom' } }` overrides assertion config
- [ ] sourceTable Dataform ref objects are resolved via `ctx.ref()` in assertion `.query()` context
- [ ] All existing tests pass (no regressions)

**Risks:**
- Dataform's `assert()` API may have undocumented quirks when called programmatically alongside `publish()` in the same file - Mitigation: verify behavior in integration test (Milestone 3)

---

### Milestone 2: Unit tests
**Goal:** Add comprehensive mock-based tests to `createTable.test.js` covering assertion wiring, backward compatibility, selective enable/disable, config overrides, and ref resolution.
**Estimated:** ~80 LOC tests
**Duration:** 0.5 days

**Tasks:**
- Add a `mockAssert` helper that captures assertion name, config, and `.query()` callback (same pattern as existing `mockPublish`)
- Test section 7: "Assertion wiring"
  - Test: `createTable` without options creates no assertions
  - Test: `createTable` with `{ assert }` creates assertions with correct names (e.g. `ga4_events_enhanced_298233330_daily_quality`)
  - Test: assertions inherit schema and tags from `dataformTableConfig`
  - Test: `assertions: { dailyQuality: false }` disables dailyQuality assertion
  - Test: `assertions: { dailyQuality: { schema: 'custom' } }` overrides assertion config
  - Test: assertion `.query()` callback resolves sourceTable via `ctx.ref()` when it's a Dataform ref object
  - Test: assertion `.query()` callback uses string sourceTable as-is when it's already a string
  - Test: assertion `.query()` callback passes correct `tableRef` via `ctx.ref(tableName)`
  - Test: table module without `assertions` property works fine with `{ assert }` (no-op)
- Run existing test suite to confirm no regressions

**Files:**
| File | Change |
|------|--------|
| `tests/createTable.test.js` | Add `mockAssert` helper + section 7 tests |

**Acceptance Criteria:**
- [ ] All new tests pass
- [ ] All existing tests pass (no regressions)
- [ ] Backward compatibility explicitly verified: createTable without options, with empty options, and with `{ assert: undefined }`
- [ ] Both Dataform ref object and string sourceTable paths are tested

---

### Milestone 3: Documentation + integration test verification
**Goal:** Update README with the assertions option, update the integration test workspace to use bundled assertions, and verify end-to-end.
**Estimated:** ~30 LOC documentation
**Duration:** 0.5 days (+ integration test run time)

**Tasks:**
- Add an **Assertions** subsection to README under Configuration Object, documenting the `options` parameter
- Update the "Using Defaults" and "With Custom Configuration" JS examples to show `{ assert }` option
- Update the integration test Dataform workspace to use `createTable(publish, config, { assert })` instead of separate assertion files
- Run integration tests to verify assertions are compiled and pass
- Run BigQuery dry-run validation of assertion SQL generated through the createTable path

**Files:**
| File | Change |
|------|--------|
| `README.md` | Add assertions documentation |
| Integration test workspace files | Update to use bundled assertions |

**Acceptance Criteria:**
- [ ] README documents the `options` parameter with examples (minimal, selective, override)
- [ ] Integration test compiles and runs with bundled assertions
- [ ] Assertion results are collected and pass in the integration test
- [ ] BigQuery dry-run validates assertion SQL syntax

**Risks:**
- Integration test environment availability - Mitigation: BigQuery dry-run validation covers SQL correctness independently

---

## Pause Point

After Milestone 2 (unit tests passing), pause for review before proceeding to documentation and integration testing. This ensures the implementation is solid before updating public-facing docs.

## Success Metrics
- All existing tests pass (no regressions)
- New unit tests cover: assertion creation, backward compatibility, selective enable/disable, config overrides, ref resolution
- Integration tests verify end-to-end assertion wiring
- README documents the new feature with examples
- Standalone assertion API (`ga4EventsEnhanced.assertions.dailyQuality(tableRef, config)`) remains unchanged

## Dependencies
- BigQuery access for dry-run validation and integration tests
- Dataform API access for integration tests

## Notes
- The total LOC estimate (~170) is conservative. The complexity is in the wiring logic within `createTable.js`, not in raw LOC count.
- The existing `mockPublish` and `mockCtx` patterns in `createTable.test.js` provide a clear blueprint for `mockAssert`.
- The internal `_generate*` functions already exist and are tested via the exported wrappers -- this sprint only needs to re-export them and wire them into `createTable`.
