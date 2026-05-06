# createTable.js Unit Tests

**Status**: Planned
**Priority**: P1 (Medium)
**Estimated**: 2-3 hours
**Dependencies**: None

## Problem Statement

`createTable.js` is the Dataform `publish()` orchestration layer — the core function that wires together config merging, validation, table naming, schema/description generation, pre-operations, and SQL generation. It has **zero direct unit tests** and is only exercised by integration tests that run the full Dataform pipeline against real BigQuery.

This function contains several non-trivial behaviors that are invisible to the existing test suite:

- **Table naming**: derives name from `defaultTableName` + dataset, stripping `analytics_` prefix
- **Deep-clone of dataformTableConfig**: prevents Dataform's `publish()` from mutating nested objects across calls
- **Config merge order**: static defaults → dynamic fields (name, schema, columns) → user overrides
- **Description fallback**: only sets auto-generated description when user hasn't provided one
- **Frozen object workaround**: creates a new object for `getTableDescription` to avoid mutating `mergedConfig`
- **Validation with `skipDataformContextFields`**: skips `self`/`incremental` validation since those come from Dataform context

A bug in any of these would only surface in the integration test (minutes, requires GCP credentials), not in the fast unit test suite.

## Scope

### In scope
- `createTable.js`: the single `createTable()` function
- Mock-based testing — mock `dataformPublish`, the table module, and verify correct wiring
- All behaviors listed above

### Out of scope
- Testing `mergeSQLConfigurations` or `mergeDataformTableConfigurations` — already covered
- Testing actual SQL generation or pre-operations — already covered
- Testing validation logic — covered by `inputValidation.test.js`

## Solution Design

### Approach: Mock `publish()` and the table module

`createTable` takes three arguments: `dataformPublish`, `userConfig`, and `tableModule`. All three can be controlled in tests. The mock `publish()` returns a chainable object (`.preOps().query()`) that captures what was passed, so tests can assert on:

1. What `publish()` was called with (table name, config object)
2. What the `.preOps()` callback returns when invoked with a mock context
3. What the `.query()` callback returns when invoked with a mock context

### Mock structure

```js
const mockPublish = () => {
    const captured = {};
    const publish = (name, config) => {
        captured.name = name;
        captured.config = config;
        return {
            preOps: (fn) => {
                captured.preOpsFn = fn;
                return {
                    query: (fn) => {
                        captured.queryFn = fn;
                        return captured;
                    }
                };
            }
        };
    };
    return { publish, captured };
};
```

### Mock table module

```js
const mockTableModule = (overrides = {}) => ({
    defaultConfig: { /* minimal valid config with dataformTableConfig */ },
    defaultTableName: 'ga4_events_enhanced',
    validate: () => {},  // no-op by default, spy-able
    generateSql: () => 'SELECT 1',
    getColumnDescriptions: () => ({ event_date: 'The event date' }),
    getTableDescription: () => 'Test table description',
    ...overrides,
});
```

### Test organization

```
1. Table naming
   - derives name from defaultTableName + dataset (analytics_123 → ga4_events_enhanced_123)
   - strips analytics_ prefix from dataset
   - uses dataset as schema

2. Config merging and deep clone
   - deep-clones defaultConfig.dataformTableConfig (mutation safety)
   - merges user dataformTableConfig overrides on top
   - user overrides win for conflicting keys (e.g. partitionBy)
   - user tags are concatenated with default tags

3. Column descriptions and table description
   - passes mergedConfig to getColumnDescriptions
   - sets columns on dataformTableConfig from getColumnDescriptions return value
   - auto-generates description via getTableDescription when not provided
   - preserves user-provided description (does not overwrite)

4. Validation
   - calls validate with skipDataformContextFields: true
   - propagates validation errors

5. Publish wiring
   - calls publish with correct name and config
   - preOps callback returns setPreOperations result
   - query callback returns generateSql result

6. Mutation safety
   - multiple createTable calls don't share nested objects
   - mergedConfig is not mutated by getTableDescription
```

## Files to Create/Modify

| File | Action |
|------|--------|
| `tests/createTable.test.js` | **Create** — all createTable tests |
| `package.json` | **Modify** — add `test:createTable` script, add to `test` chain |

## Success Criteria

- [ ] Table naming logic (analytics_ prefix stripping) tested
- [ ] Config merge order (defaults → dynamic → user) tested
- [ ] Deep-clone mutation safety tested
- [ ] Description fallback vs user override tested
- [ ] Validation called with skipDataformContextFields tested
- [ ] Publish/preOps/query wiring tested
- [ ] All existing tests still pass (`npm test`)
- [ ] No BigQuery calls — pure Node.js tests

## Testing Strategy

```bash
npm run test:createTable   # standalone
npm test                    # full suite including createTable
```

Expected: ~25-35 test cases, pure Node.js, sub-second execution.
