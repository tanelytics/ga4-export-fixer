# Input Validation Tests

**Status**: Planned
**Priority**: P0 (High)
**Estimated**: 3-4 hours
**Dependencies**: None

## Problem Statement

The validation layer (`inputValidation.js` and `tables/ga4EventsEnhanced/validation.js`) has **no dedicated tests**. These modules are only exercised transitively through `ga4EventsEnhanced.test.js`, which passes valid configs to `generateSql()`. This means:

- **Error paths are untested** — invalid configs, missing required fields, wrong types
- **Boundary conditions are untested** — edge values for integers, empty strings, null vs undefined
- **Error messages are unverified** — users rely on these to debug their config; any regression in message quality is invisible
- **The `skipDataformContextFields` option is untested** — used by documentation functions to skip `self`/`incremental` validation

Since validation is the first thing users hit when misconfiguring the package, broken validation directly impacts user experience.

## Scope

### In scope
- `inputValidation.js`: `validateBaseConfig()` — 8 field groups, ~20 error paths
- `tables/ga4EventsEnhanced/validation.js`: `validateEnhancedEventsConfig()` — 12+ field groups, ~30 error paths
- Both valid (no-throw) and invalid (throws with correct message) cases
- The `skipDataformContextFields` option
- The `Config validation:` error message prefix from `validateEnhancedEventsConfig`

### Out of scope
- Testing config merging (`mergeSQLConfigurations`) — already covered by `mergeSQLConfigurations.test.js`
- Testing validation indirectly through `generateSql()` — already covered by `ga4EventsEnhanced.test.js`

## Solution Design

### Test file structure

One new test file: `tests/inputValidation.test.js`

Follows existing conventions (raw `assert` + custom runner, no test framework). Same `test()` helper pattern used in `preOperations.test.js` and `mergeSQLConfigurations.test.js`.

### Test organization

```
1. validateBaseConfig — config object type
   - rejects null, undefined, array, string, number

2. validateBaseConfig — self and incremental (Dataform context fields)
   - rejects missing self when test !== true
   - rejects non-string self
   - rejects self without backtick format
   - accepts valid self
   - skips self/incremental when test === true
   - rejects non-boolean incremental
   - skips self/incremental with skipDataformContextFields option

3. validateBaseConfig — test field
   - accepts undefined (optional)
   - accepts boolean
   - rejects non-boolean

4. validateBaseConfig — testConfig
   - accepts undefined (optional)
   - rejects non-object (null, array, string)
   - rejects non-string dateRangeStart
   - rejects empty dateRangeStart
   - accepts valid dateRangeStart/dateRangeEnd

5. validateBaseConfig — preOperations
   - rejects missing preOperations
   - rejects non-object
   - rejects missing numberOfPreviousDaysToScan
   - rejects negative, float, NaN, string numberOfPreviousDaysToScan
   - rejects missing/empty dateRangeStartFullRefresh
   - rejects missing/empty dateRangeEnd
   - rejects invalid numberOfDaysToProcess (0, negative, float)
   - accepts valid numberOfDaysToProcess
   - accepts/rejects incrementalStartOverride and incrementalEndOverride edge cases

6. validateEnhancedEventsConfig — prefixes errors with "Config validation:"

7. validateEnhancedEventsConfig — sourceTable
   - rejects missing/null/undefined
   - rejects empty string
   - rejects string without backtick format
   - accepts valid backtick string
   - accepts Dataform reference object

8. validateEnhancedEventsConfig — schemaLock
   - accepts undefined (optional)
   - rejects non-YYYYMMDD format
   - rejects invalid date (20241332)
   - rejects date before 20241009
   - accepts valid date

9. validateEnhancedEventsConfig — includedExportTypes
   - rejects missing
   - rejects non-object
   - rejects missing daily/fresh/intraday keys
   - rejects non-boolean values
   - rejects all-false (at least one must be true)

10. validateEnhancedEventsConfig — timezone
    - rejects missing/empty
    - accepts valid string

11. validateEnhancedEventsConfig — dataIsFinal
    - rejects missing
    - rejects invalid detectionMethod
    - rejects missing dayThreshold when DAY_THRESHOLD
    - rejects EXPORT_TYPE when daily is disabled
    - accepts valid configs

12. validateEnhancedEventsConfig — bufferDays
    - rejects missing/non-integer/negative
    - accepts 0 and positive integers

13. validateEnhancedEventsConfig — string array fields
    - rejects missing, non-array, arrays with empty strings
    - accepts valid string arrays
    - covers: excludedEventParams, sessionParams, excludedEvents, excludedColumns,
      defaultExcludedEventParams, defaultExcludedEvents

14. validateEnhancedEventsConfig — eventParamsToColumns
    - rejects missing, non-array
    - rejects item without name
    - rejects invalid type
    - accepts item without type (optional)
    - rejects invalid columnName
    - accepts valid items

15. validateEnhancedEventsConfig — valid full config passes without error
```

### Helper: valid base config factory

```js
const validBaseConfig = (overrides = {}) => ({
    self: '`project.dataset.table`',
    incremental: false,
    test: false,
    testConfig: { dateRangeStart: 'current_date()-1', dateRangeEnd: 'current_date()' },
    preOperations: {
        dateRangeStartFullRefresh: 'date(2000, 1, 1)',
        dateRangeEnd: 'current_date()',
        numberOfPreviousDaysToScan: 10,
    },
    ...overrides,
});
```

A similar `validEnhancedConfig` factory extends `validBaseConfig` with all table-specific required fields from `tables/ga4EventsEnhanced/config.js` defaults.

### Test pattern

Each test either:
1. **Expects success**: calls validate, no assertion needed (no throw = pass)
2. **Expects failure**: uses `assert.throws()` with a message substring match

```js
test('rejects null config', () => {
    assert.throws(
        () => validateBaseConfig(null),
        /config must be a non-null object/
    );
});

test('accepts valid config', () => {
    validateBaseConfig(validBaseConfig()); // no throw = pass
});
```

## Files to Create/Modify

| File | Action |
|------|--------|
| `tests/inputValidation.test.js` | **Create** — all validation tests |
| `package.json` | **Modify** — add `test:validation` script, add to `test` script |

## npm scripts

```json
"test:validation": "node tests/inputValidation.test.js",
"test": "node tests/ga4EventsEnhanced.test.js && node tests/mergeSQLConfigurations.test.js && node tests/preOperations.test.js && node tests/documentation.test.js && node tests/inputValidation.test.js"
```

## Success Criteria

- [ ] Every error path in `validateBaseConfig` has at least one test
- [ ] Every error path in `validateEnhancedEventsConfig` has at least one test
- [ ] `skipDataformContextFields` option is tested
- [ ] Error message prefix `Config validation:` is tested
- [ ] A fully valid config passes without errors
- [ ] All existing tests still pass (`npm test`)
- [ ] No BigQuery calls needed — pure Node.js tests

## Testing Strategy

```bash
# Run just validation tests
npm run test:validation

# Run all tests including validation
npm test
```

Expected: ~60-80 test cases, all pure Node.js (no network, no BigQuery). Sub-second execution.
