# Bundle Assertions with createTable

**Status**: Planned
**Priority**: P1 (Medium)
**Estimated**: 1.5 days
**Dependencies**: Table assertions (implemented), multi-table module architecture (implemented)

## Problem Statement

Assertions for ga4_events_enhanced (dailyQuality, itemRevenue) exist as standalone SQL generators, but users must wire them up manually:

```js
// Current: user creates table + assertion separately
ga4EventsEnhanced.createTable(publish, config);

assert('daily_quality_check', {
    schema: 'analytics_123456789',
    tags: ['ga4_export_fixer'],
}).query(ctx => {
    return ga4EventsEnhanced.assertions.dailyQuality(
        ctx.ref('ga4_events_enhanced_123456789'),
        { ...config, sourceTable: ctx.ref(config.sourceTable) }
    );
});
```

**Pain points:**

1. **Config duplication** -- the assertion call repeats the same config that was already passed to createTable
2. **Manual table ref resolution** -- the user must know the computed table name (e.g. `ga4_events_enhanced_123456789`) and pass it via `ctx.ref()`, but that name is already computed inside `createTable` at [createTable.js:34](createTable.js#L34)
3. **sourceTable re-resolution** -- the assertion's `sourceTable` must be resolved from a Dataform ref object to a string via `ctx.ref()` because assertions don't have their own Dataform context. The user must handle this manually (`{ ...config, sourceTable: ctx.ref(config.sourceTable) }`)
4. **Boilerplate** -- even for the simplest case, the user writes ~10 lines of assertion wiring per assertion type
5. **Discoverability** -- assertions are exported but undocumented; users don't know they exist

## Goals

- Users can opt into bundled assertions by passing `assert` alongside `publish` in a single `createTable` call
- Zero additional configuration required -- the assertion inherits the table's merged config, resolved table ref, and resolved sourceTable
- Fully backward compatible -- existing `createTable(publish, config)` calls continue to work unchanged
- The standalone assertion API (`ga4EventsEnhanced.assertions.dailyQuality(tableRef, config)`) remains available for SQLX deployment and advanced use cases
- Assertions are created with sensible defaults (name, schema, tags) that can be overridden

## Solution Design

### API

Add an optional third `options` parameter to `createTable`:

```js
// Minimal -- enable all bundled assertions
ga4EventsEnhanced.createTable(publish, config, { assert });

// Selective -- only dailyQuality
ga4EventsEnhanced.createTable(publish, config, {
    assert,
    assertions: { dailyQuality: true, itemRevenue: false },
});

// Override assertion Dataform config
ga4EventsEnhanced.createTable(publish, config, {
    assert,
    assertions: {
        dailyQuality: {
            schema: 'custom_schema',
            tags: ['custom_tag'],
        },
    },
});

// No assertions (backward compatible, same as today)
ga4EventsEnhanced.createTable(publish, config);
```

When `options.assert` is provided:
1. All assertions are enabled by default
2. `options.assertions` optionally controls which assertions are created and their Dataform config overrides
3. Setting an assertion to `false` disables it

### Architecture

The change spans two layers:

**1. Shared `createTable.js`** -- the lifecycle function that all table modules use

Currently:
```js
const createTable = (dataformPublish, userConfig, tableModule) => { ... };
```

After:
```js
const createTable = (dataformPublish, userConfig, tableModule, options) => { ... };
```

The `options` parameter is optional. When `options.assert` is provided, `createTable` calls the assertion generators after publishing the table and creates Dataform assertions using the `assert()` function.

**2. Table module interface** -- add an optional `assertions` property

Currently each table module exposes:
```js
{
    defaultConfig,
    defaultTableName,
    validate,
    generateSql,
    getColumnDescriptions,
    getTableDescription,
}
```

Add:
```js
{
    ...existing,
    assertions: {
        dailyQuality: { generate: (tableRef, mergedConfig) => sql, defaultName: 'daily_quality' },
        itemRevenue: { generate: (tableRef, mergedConfig) => sql, defaultName: 'item_revenue' },
    },
}
```

Each assertion entry provides:
- `generate(tableRef, mergedConfig)` -- the internal SQL generator (the `_generate*` function, not the exported wrapper that does its own merge+validate)
- `defaultName` -- default assertion name suffix (combined with the table name to form e.g. `ga4_events_enhanced_123456789_daily_quality`)

### How createTable wires assertions

Inside `createTable`, after the `publish()` call, the function iterates over `tableModule.assertions` and for each enabled assertion:

```js
// Pseudocode inside createTable.js
if (options?.assert && tableModule.assertions) {
    const tableName = dataformTableConfig.name; // already computed
    const schema = dataformTableConfig.schema;  // already computed

    for (const [key, assertionDef] of Object.entries(tableModule.assertions)) {
        // Check if this assertion is enabled
        const assertionOption = options.assertions?.[key];
        if (assertionOption === false) continue;

        // Build assertion Dataform config
        const assertionName = `${tableName}_${assertionDef.defaultName}`;
        const assertionDataformConfig = {
            schema,
            tags: dataformTableConfig.tags || [],
            // user overrides from options.assertions[key] (if it's an object)
            ...(typeof assertionOption === 'object' ? assertionOption : {}),
        };

        // Create the assertion
        options.assert(assertionName, assertionDataformConfig).query(ctx => {
            // Resolve sourceTable through ctx if it's a Dataform ref object
            const resolvedConfig = { ...mergedConfig };
            if (utils.isDataformTableReferenceObject(resolvedConfig.sourceTable)) {
                resolvedConfig.sourceTable = ctx.ref(resolvedConfig.sourceTable);
            }
            return assertionDef.generate(ctx.ref(tableName), resolvedConfig);
        });
    }
}
```

Key points:
- **tableRef** is resolved via `ctx.ref(tableName)` inside the assertion's `.query()` callback -- this is the Dataform context of the assertion, not the table
- **sourceTable** is resolved via `ctx.ref()` if it's a Dataform ref object -- this eliminates the manual re-resolution the user had to do
- **mergedConfig** is reused from the table creation step -- no re-merge or re-validation needed
- The assertion generators receive the internal `_generate*` functions (already validated config), not the exported wrappers

### Assertion naming

Default assertion names follow the pattern: `{tableName}_{assertionDefaultName}`

Examples:
- `ga4_events_enhanced_123456789_daily_quality`
- `ga4_events_enhanced_123456789_item_revenue`

This ensures:
- No collisions when multiple GA4 properties are processed in the same Dataform repository
- Clear association between the table and its assertions
- Consistent with how Dataform names dependent objects

Users can override the name via the assertion Dataform config:

```js
ga4EventsEnhanced.createTable(publish, config, {
    assert,
    assertions: {
        dailyQuality: { name: 'custom_assertion_name' },
    },
});
```

### Assertion schema and tags

By default, assertions inherit the table's `schema` and `tags` from `dataformTableConfig`. This ensures:
- Assertions appear in the same dataset as the table
- Assertions are included when running by tag (e.g. `ga4_export_fixer`)

### SQLX deployment

The bundled assertion feature is only available for JS deployment (where `createTable` is used). SQLX users continue using the standalone assertion API:

```js
// In a separate .sqlx or .js file
assert('daily_quality_check', { ... }).query(ctx => {
    return ga4EventsEnhanced.assertions.dailyQuality(ctx.ref('table'), config);
});
```

This is acceptable because:
- SQLX deployment already requires more manual wiring (the user writes the SQLX config block, pre_operations, etc.)
- The standalone API remains fully supported and documented

### Why `options` is a separate parameter (not in `config`)

The `assert` function is a Dataform runtime global, not a user configuration value. Mixing it into `config` would:
- Pollute the config object with Dataform API references
- Make config serialization/cloning unreliable (functions don't clone)
- Break the clean separation between "what to build" (config) and "how to publish" (publish, assert)

The `options` object follows the same pattern as `publish` -- it's a Dataform API capability passed into createTable.

## Implementation Plan

### Phase 1: Extend table module interface (~2 hours)

- [ ] Add `assertions` property to the ga4EventsEnhanced table module in [index.js](tables/ga4EventsEnhanced/index.js)
- [ ] Wire `_generateDailyQualityAssertionSql` and `_generateItemRevenueAssertionSql` as internal generators
- [ ] Export `_generate*` functions from the assertion modules (currently only the wrapper is exported)

### Phase 2: Extend createTable (~3 hours)

- [ ] Add optional `options` parameter to `createTable` in [createTable.js](createTable.js)
- [ ] Implement assertion creation loop with Dataform `assert()` wiring
- [ ] Handle sourceTable resolution via `ctx.ref()` inside assertion `.query()` callback
- [ ] Handle assertion naming (default + user override)
- [ ] Handle assertion schema and tags inheritance
- [ ] Handle `options.assertions` for selective enable/disable and config overrides

### Phase 3: Update ga4EventsEnhanced wrapper (~1 hour)

- [ ] Update `createEnhancedEventsTable` in [index.js](tables/ga4EventsEnhanced/index.js) to forward `options` to `createTable`
- [ ] Update JSDoc for the new parameter

### Phase 4: Testing (~4 hours)

- [ ] Unit test: createTable with `{ assert }` creates assertions with correct names, schema, tags
- [ ] Unit test: createTable without options works unchanged (backward compatibility)
- [ ] Unit test: `assertions: { dailyQuality: false }` disables dailyQuality
- [ ] Unit test: `assertions: { dailyQuality: { schema: 'custom' } }` overrides assertion config
- [ ] Unit test: sourceTable Dataform ref object is resolved via ctx.ref() in assertion context
- [ ] Unit test: assertion receives the correct tableRef via ctx.ref(tableName)
- [ ] BigQuery dry-run validation of generated assertion SQL
- [ ] Integration test: verify assertions are discovered and pass in the test Dataform repository

### Phase 5: Documentation (~1 hour)

- [ ] Add assertions section to README under the Configuration Object section
- [ ] Update the "Using Defaults" and "With Custom Configuration" examples to show `{ assert }` option
- [ ] Document the standalone assertion API for SQLX users

## Files to Modify/Create

| File | Change | Est. LOC |
|------|--------|----------|
| [createTable.js](createTable.js) | Add `options` parameter, assertion creation loop | +40 |
| [tables/ga4EventsEnhanced/index.js](tables/ga4EventsEnhanced/index.js) | Add `assertions` to tableModule, forward `options` | +15 |
| [tables/ga4EventsEnhanced/assertions/dailyQuality.js](tables/ga4EventsEnhanced/assertions/dailyQuality.js) | Export `_generateDailyQualityAssertionSql` | +1 |
| [tables/ga4EventsEnhanced/assertions/itemRevenue.js](tables/ga4EventsEnhanced/assertions/itemRevenue.js) | Export `_generateItemRevenueAssertionSql` | +1 |
| [tests/createTable.test.js](tests/createTable.test.js) | Add assertion-related test cases | +80 |
| [README.md](README.md) | Document assertions option | +30 |

## Examples

### Minimal (enable all assertions)

```js
const { ga4EventsEnhanced } = require('ga4-export-fixer');

const config = {
    sourceTable: constants.GA4_TABLES.MY_GA4_EXPORT,
};

ga4EventsEnhanced.createTable(publish, config, { assert });
```

This single call creates:
- `ga4_events_enhanced_123456789` (the table)
- `ga4_events_enhanced_123456789_daily_quality` (assertion)
- `ga4_events_enhanced_123456789_item_revenue` (assertion)

### Selective assertions

```js
ga4EventsEnhanced.createTable(publish, config, {
    assert,
    assertions: { dailyQuality: true, itemRevenue: false },
});
```

### Custom assertion config

```js
ga4EventsEnhanced.createTable(publish, config, {
    assert,
    assertions: {
        dailyQuality: {
            tags: ['data_quality', 'ga4_export_fixer'],
        },
    },
});
```

### Without assertions (backward compatible)

```js
// These are equivalent:
ga4EventsEnhanced.createTable(publish, config);
ga4EventsEnhanced.createTable(publish, config, {});
ga4EventsEnhanced.createTable(publish, config, { assert: undefined });
```

### SQLX deployment (standalone API, unchanged)

```js
// Assertions still available for manual wiring
assert('daily_quality_check', {
    schema: 'analytics_123456789',
    tags: ['ga4_export_fixer'],
}).query(ctx => {
    return ga4EventsEnhanced.assertions.dailyQuality(
        ctx.ref('ga4_events_enhanced_123456789'),
        { ...config, sourceTable: ctx.ref(config.sourceTable) }
    );
});
```

## Success Criteria

- [ ] `createTable(publish, config, { assert })` creates both the table and all assertions
- [ ] `createTable(publish, config)` works identically to today (no behavioral change)
- [ ] Assertions use the correct resolved table ref and sourceTable (no manual ctx.ref() needed by the user)
- [ ] Assertion naming follows `{tableName}_{assertionDefaultName}` pattern
- [ ] Assertions inherit schema and tags from the table's dataformTableConfig
- [ ] Individual assertions can be disabled via `assertions: { key: false }`
- [ ] Assertion Dataform config can be overridden via `assertions: { key: { ...overrides } }`
- [ ] All existing tests pass (no regressions)
- [ ] Integration tests verify assertions are created and pass

## Testing Strategy

**Unit tests (mock-based):**
- Mock `assert()` to capture calls and verify assertion names, config, and SQL
- Verify backward compatibility: no assertion calls when options is omitted
- Verify selective enable/disable
- Verify config overrides are applied
- Verify sourceTable Dataform ref resolution in assertion context

**BigQuery dry-run:**
- Generate assertion SQL through the createTable path and validate against BigQuery

**Integration test:**
- Update the test Dataform workspace to use `createTable(publish, config, { assert })`
- Verify assertions are compiled and pass during workflow invocation

## Non-Goals

- Configurable assertion lookback windows (hardcoded to 5 days, same as today)
- Custom assertion SQL generators passed by the user (only built-in assertions)
- Assertion dependencies (e.g. run assertion only after table succeeds -- Dataform handles this)
- SQLX deployment support for bundled assertions (SQLX users use the standalone API)
- Assertion severity levels or warning-only mode

## Resolved Questions

1. **Why not put `assert` in the config object?** The `assert` function is a Dataform runtime API, not a configuration value. Keeping it in a separate `options` parameter maintains the clean separation between configuration (serializable data) and runtime capabilities (functions). This matches how `publish` is already a separate parameter.

2. **Should `createTable` return the assertion objects?** No. The return value stays as the Dataform publish object (for backward compatibility and potential chaining). Assertions are fire-and-forget -- Dataform manages their lifecycle.

3. **What about the existing standalone assertion API?** It remains unchanged and fully supported. The bundled approach is a convenience layer on top. The standalone API is still needed for SQLX deployment and for users who want full control over assertion naming/placement.

4. **Should all assertions be enabled by default when `assert` is passed?** Yes. The intent of passing `assert` is "I want assertions." Requiring an additional `assertions: { dailyQuality: true }` flag would add unnecessary friction. Users who want only specific assertions can disable the others.

5. **Where does the assertion creation logic live -- in `createTable.js` or in the table module?** In `createTable.js`. The assertion wiring (calling `assert()`, resolving refs, building names) is part of the shared lifecycle, not table-specific. The table module provides the SQL generators and default names; the shared lifecycle handles the Dataform plumbing.

---

**Document created**: 2026-04-17
**Last updated**: 2026-04-17
