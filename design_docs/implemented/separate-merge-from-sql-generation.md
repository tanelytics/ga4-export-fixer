# Separate Merge/Validation from SQL Generation

**Status**: Planned
**Priority**: P1 (Medium)
**Estimated**: 2 hours
**Dependencies**: None

## Problem Statement

In `tables/ga4EventsEnhanced.js`, `mergeSQLConfigurations` runs redundantly when `createEnhancedEventsTable` is called:

1. `createEnhancedEventsTable` merges config at line 322
2. Its `.query()` callback calls `generateEnhancedEventsSQL`, which merges the same config again at line 150
3. The local `setPreOperations` wrapper (line 370) also merges independently when called externally

**Current call graph for `createTable`:**
```
createEnhancedEventsTable(config)
  └─ mergeSQLConfigurations(defaultConfig, config)        ← 1st merge
  └─ .preOps(ctx => preOperations.setPreOperations(...))  ← no merge (correct)
  └─ .query(ctx => generateEnhancedEventsSQL(...))
       └─ mergeSQLConfigurations(defaultConfig, config)   ← 2nd merge (redundant)
       └─ validateEnhancedEventsConfig(mergedConfig)
```

Additionally, `createEnhancedEventsTable` does not call `validateEnhancedEventsConfig` — validation only happens when the `.query()` callback fires inside `generateEnhancedEventsSQL`. This delays error detection.

**Impact:**
- `mergeSQLConfigurations` runs twice per `createTable` call with identical inputs
- Config validation is deferred instead of failing fast at the `createTable` entry point
- The merge/validate concern is entangled with SQL generation logic

## Goals

**Primary Goal:** Each exported function merges and validates exactly once; internal functions receive pre-merged config.

**Success Metrics:**
- `createTable` calls `mergeSQLConfigurations` exactly 1 time (down from 2)
- `createTable` validates config at entry, before any Dataform callbacks
- No breaking changes to the public API (`generateSql`, `createTable`, `setPreOperations`)
- All existing tests pass without modification

## Solution Design

### Overview

Split each function into an **exported wrapper** (merge + validate + delegate) and an **internal base function** (no merge, no validate). When `createTable` calls the internal versions, no redundant work occurs.

### Implementation Plan

**Phase 1: Refactor** (~1.5 hours)

- [ ] Extract the body of `generateEnhancedEventsSQL` (lines 155-297) into `_generateEnhancedEventsSQL(mergedConfig)`
- [ ] Remove `mergeSQLConfigurations` + `validateEnhancedEventsConfig` + redundant `sourceTable` check from the internal function
- [ ] Rewrite `generateEnhancedEventsSQL` as a thin wrapper: merge, validate, delegate to `_generateEnhancedEventsSQL`
- [ ] Rewrite `setPreOperations` as a thin wrapper: merge, validate, delegate to `preOperations.setPreOperations`
- [ ] Update `createEnhancedEventsTable` to: merge once, validate once, call `_generateEnhancedEventsSQL` and `preOperations.setPreOperations` directly

**Phase 2: Verify** (~0.5 hours)

- [ ] Run `npm test` — all 4 test suites pass
- [ ] Run `npm run test:integration` — full suite passes
- [ ] Trace `createTable` call path to confirm single merge

### Files to Modify

**Modified files:**
- `tables/ga4EventsEnhanced.js` — refactor 3 functions (~30 lines changed)

**No other files change.** `index.js`, `preOperations.js`, `utils.js`, and all tests remain untouched.

## Examples

### createTable call path

**Before:**
```javascript
const createEnhancedEventsTable = (dataformPublish, config) => {
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);  // merge #1
    // ... build dataformTableConfig ...
    return dataformPublish(...).preOps(ctx => {
        return preOperations.setPreOperations(utils.setDataformContext(ctx, mergedConfig));
    }).query(ctx => {
        return generateEnhancedEventsSQL(utils.setDataformContext(ctx, mergedConfig));
        //     ↑ merges again internally (merge #2) + validates
    });
};
```

**After:**
```javascript
const createEnhancedEventsTable = (dataformPublish, config) => {
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);  // merge (only)
    inputValidation.validateEnhancedEventsConfig(mergedConfig);               // validate (early)
    // ... build dataformTableConfig ...
    return dataformPublish(...).preOps(ctx => {
        return preOperations.setPreOperations(utils.setDataformContext(ctx, mergedConfig));
    }).query(ctx => {
        return _generateEnhancedEventsSQL(utils.setDataformContext(ctx, mergedConfig));
        //     ↑ internal version — no merge, no validate
    });
};
```

### generateSql (standalone usage)

**Before:**
```javascript
const generateEnhancedEventsSQL = (config) => {
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);
    inputValidation.validateEnhancedEventsConfig(mergedConfig);
    // ... 140 lines of SQL generation ...
};
```

**After:**
```javascript
// Internal: no merge, no validate
const _generateEnhancedEventsSQL = (mergedConfig) => {
    // ... 140 lines of SQL generation ...
};

// Exported: merge + validate + delegate
const generateEnhancedEventsSQL = (config) => {
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);
    inputValidation.validateEnhancedEventsConfig(mergedConfig);
    return _generateEnhancedEventsSQL(mergedConfig);
};
```

## Success Criteria

- [ ] `createTable` merges config exactly once
- [ ] `createTable` validates config before any Dataform callbacks fire
- [ ] `generateSql` standalone usage still merges + validates
- [ ] `setPreOperations` standalone usage still merges + validates
- [ ] `module.exports` unchanged — no breaking API changes
- [ ] All existing tests pass (`npm test`)
- [ ] Integration tests pass (`npm run test:integration`)

## Testing Strategy

**Existing tests cover all paths:**
- `tests/ga4EventsEnhanced.test.js` — calls `generateSql` with various configs
- `tests/preOperations.test.js` — calls `preOperations.setPreOperations` directly
- `tests/mergeSQLConfigurations.test.js` — validates merge behavior
- `tests/integration/integration.test.js` — end-to-end via Dataform `createTable`

No new tests needed — this is a pure refactoring that preserves behavior.

## Non-Goals

**Not in this feature:**
- Refactoring `index.js` `setPreOperations` — uses `baseConfig` (not `ga4EventsEnhancedConfig`) and calls `preOperations.setPreOperations` directly; independent path
- Refactoring `preOperations.js` — already expects pre-merged config
- Multi-table module architecture — separate design doc exists (`multi-table-module-architecture.md`)

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Dataform runtime freezes config objects returned by callbacks | Low | `setDataformContext` creates a new object with spread; internal functions don't mutate config |
| `sourceTable` check removal in `_generateEnhancedEventsSQL` | Low | `validateEnhancedEventsConfig` already validates `sourceTable` more thoroughly |

## References

- `tables/ga4EventsEnhanced.js` — primary file being refactored
- `design_docs/planned/multi-table-module-architecture.md` — this refactoring aligns with the module architecture plan

---

**Document created**: 2026-04-08
**Last updated**: 2026-04-08
