# Schema Lock: Intraday and Fresh Export Type Support

**Status**: Planned
**Priority**: P1 (Medium)
**Estimated**: 1-2 hours
**Dependencies**: None

## Problem Statement

The `schemaLock` config option currently only accepts a daily export table suffix in `YYYYMMDD` format (e.g., `"20260101"`), which resolves to `events_20260101` in the `CREATE TABLE ... LIKE` statement. Users who don't have daily exports enabled — or who want to lock to an intraday or fresh export specifically — cannot use this feature.

GA4 exports three table types with different naming conventions:
- **Daily**: `events_20260101`
- **Intraday**: `events_intraday_20260101`
- **Fresh**: `events_fresh_20260101`

The schema lock should support all three.

## Scope

### In scope
- Extend validation to accept `"intraday_YYYYMMDD"` and `"fresh_YYYYMMDD"` formats alongside the existing `"YYYYMMDD"`
- Update validation error messages and comments
- Update tests for validation and pre-operations
- Update README documentation

### Out of scope
- Changing the SQL generation logic in `createSchemaLockTable` — it already works correctly for all three formats because it constructs `events_${config.schemaLock}`, which naturally produces `events_intraday_20260101` or `events_fresh_20260101`
- Changing the config default or merge behavior

## Solution Design

### Key insight: no SQL changes needed

The `createSchemaLockTable` function in `preOperations.js:157` builds the source table name as:

```js
const copySchemaFromTable = `...events_${config.schemaLock}...`
```

This means:
- `"20260101"` → `events_20260101` (daily)
- `"intraday_20260101"` → `events_intraday_20260101` (intraday)
- `"fresh_20260101"` → `events_fresh_20260101` (fresh)

All three already produce the correct BigQuery table name. The only blocker is the validation regex.

### 1. Validation (`tables/ga4EventsEnhanced/validation.js:43-60`)

**Current**: Regex `/^\d{8}$/` accepts only `YYYYMMDD`.

**New**: Accept three formats via a regex like `/^(?:(?:intraday|fresh)_)?\d{8}$/`. Then extract the date portion (last 8 characters) for the date validity check and minimum-date check.

```js
// schemaLock - optional; must be undefined or a valid export table suffix
if (typeof config.schemaLock !== 'undefined') {
    if (typeof config.schemaLock !== 'string' || !/^(?:(?:intraday|fresh)_)?\d{8}$/.test(config.schemaLock)) {
        throw new Error(`config.schemaLock must be a string in "YYYYMMDD", "intraday_YYYYMMDD", or "fresh_YYYYMMDD" format. Received: ${JSON.stringify(config.schemaLock)}`);
    }
    // Extract the date portion (last 8 characters)
    const datePart = config.schemaLock.slice(-8);
    const year = parseInt(datePart.slice(0, 4), 10);
    const month = parseInt(datePart.slice(4, 6), 10);
    const day = parseInt(datePart.slice(6, 8), 10);
    const date = new Date(year, month - 1, day);
    if (date.getFullYear() !== year || date.getMonth() !== month - 1 || date.getDate() !== day) {
        throw new Error(`config.schemaLock must contain a valid date. Received: ${JSON.stringify(config.schemaLock)}`);
    }
    if (datePart < "20241009") {
        throw new Error(`config.schemaLock date must be equal to or greater than "20241009". Received: ${JSON.stringify(config.schemaLock)}`);
    }
}
```

### 2. No changes to `preOperations.js`

`createSchemaLockTable` (line 153-165) and the condition at line 232 work correctly for all three formats without modification.

### 3. Validation tests (`tests/inputValidation.test.js:527-569`)

Existing tests (6 tests) remain valid — `"20260101"` is still accepted. Add new tests:

| Test | Input | Expected |
|------|-------|----------|
| accepts intraday prefix | `"intraday_20260101"` | passes |
| accepts fresh prefix | `"fresh_20260101"` | passes |
| rejects unknown prefix | `"streaming_20260101"` | throws format error |
| rejects prefix with invalid date | `"intraday_20241332"` | throws valid date error |
| rejects prefix with date before minimum | `"fresh_20241008"` | throws minimum date error |
| accepts intraday with minimum date | `"intraday_20241009"` | passes |

### 4. Pre-operations tests (`tests/preOperations.test.js:335-364`)

Add tests to verify the CREATE statement generates the correct `LIKE` table name for intraday and fresh:

| Test | schemaLock value | Expected `LIKE` table |
|------|-----------------|----------------------|
| intraday schema lock generates correct CREATE | `"intraday_20260101"` | `events_intraday_20260101` |
| fresh schema lock generates correct CREATE | `"fresh_20260101"` | `events_fresh_20260101` |

These tests confirm the end-to-end behavior even though the SQL generation doesn't change, serving as regression protection.

### 5. README (`README.md:294`)

Update the config table row:

**Current**:
```
| `schemaLock` | string (YYYYMMDD) | `undefined` | Lock the table schema to a specific date. Must be a valid date >= `"20241009"` |
```

**New**:
```
| `schemaLock` | string | `undefined` | Lock the table schema to a specific GA4 export table suffix. Accepts `"YYYYMMDD"` (daily), `"intraday_YYYYMMDD"`, or `"fresh_YYYYMMDD"`. Date must be >= `"20241009"` |
```

Also update the example in the quick start section (line 207) to show the new formats are possible:

```js
schemaLock: '20260101', // daily export; also supports 'intraday_20260101' or 'fresh_20260101'
```

## Files to Modify

| File | Action | Lines |
|------|--------|-------|
| `tables/ga4EventsEnhanced/validation.js` | **Modify** — update regex, date extraction, error messages | 43-60 |
| `tests/inputValidation.test.js` | **Modify** — add ~6 new tests for intraday/fresh formats | after 569 |
| `tests/preOperations.test.js` | **Modify** — add ~2 new tests for intraday/fresh CREATE statements | after 364 |
| `README.md` | **Modify** — update schemaLock type, description, and example | 207, 294 |

## Success Criteria

- [ ] `"20260101"` still accepted (backward compatible)
- [ ] `"intraday_20260101"` accepted and produces correct `events_intraday_20260101` in SQL
- [ ] `"fresh_20260101"` accepted and produces correct `events_fresh_20260101` in SQL
- [ ] Invalid prefixes like `"streaming_20260101"` rejected
- [ ] Date validation still enforced for prefixed formats (valid date, >= 20241009)
- [ ] All existing tests pass (`npm test`)
- [ ] README reflects new accepted formats

## Testing Strategy

```bash
npm run test:validation   # schemaLock validation tests
npm run test:preops       # schema lock pre-operations tests
npm test                  # full suite
```

Expected: ~8 new test cases, no BigQuery calls needed.
