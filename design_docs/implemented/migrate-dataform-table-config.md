# Migrate dataformTableConfig Defaults

**Status**: Planned
**Priority**: P1 (Medium)
**Estimated**: 1 day
**Dependencies**: None

## Problem Statement

The default `dataformTableConfig` is hardcoded inside `createEnhancedEventsTable()` in [tables/ga4EventsEnhanced.js:327-342](tables/ga4EventsEnhanced.js#L327-L342), separate from all other defaults which live in [defaultConfig.js](defaultConfig.js). This creates two problems:

1. **Split defaults** — configuration defaults are spread across two files, making it harder to understand and maintain
2. **Only used in JS deployment** — `dataformTableConfig` is only relevant when using `createTable()` (JS deployment), not `.sqlx` deployment. Yet it currently lives inside the table creation function mixed with dynamic logic

## Current Architecture

```
User Config
    ↓
createEnhancedEventsTable(publish, config)
    ↓
mergeSQLConfigurations(defaultConfig, config)          ← merges SQL config (defaultConfig.js)
    ↓                                                     dataformTableConfig passes through untouched
mergedConfig
    ↓
Build defaultDataformTableConfig inline (hardcoded)    ← STATIC defaults + DYNAMIC fields (description, columns, name, schema)
    ↓
setDefaults()                                          ← derives name/schema from sourceTable
    ↓
mergeDataformTableConfigurations(defaults, mergedConfig.dataformTableConfig)
    ↓
Final dataformTableConfig → Dataform publish()
```

### Why simply moving defaults to defaultConfig.js is risky

If `dataformTableConfig` defaults were added to `ga4EventsEnhancedConfig` in `defaultConfig.js`:

1. **Double merge** — `mergeSQLConfigurations` would deep-merge `dataformTableConfig` from defaults with the user's `dataformTableConfig`. Then `createEnhancedEventsTable` would merge again via `mergeDataformTableConfigurations`. The defaults would appear as if they were user input in the second merge.

2. **Dynamic fields can't be static defaults** — `description`, `columns`, `name`, and `schema` are computed at runtime from the merged SQL config. These can never live in static defaults.

3. **Merge side effects** — `mergeSQLConfigurations` processes date fields, fixes sourceTable format, and merges default/counterpart arrays. These transformations should not touch `dataformTableConfig` fields.

## Goals

- Move static `dataformTableConfig` defaults to `defaultConfig.js` for consistency
- Keep dynamic fields (`description`, `columns`, `name`, `schema`) computed in `createEnhancedEventsTable`
- Ensure `mergeSQLConfigurations` does not process or transform `dataformTableConfig`
- Ensure the merge chain is idempotent — calling `createTable` with the same config always produces the same result regardless of internal merge ordering

## Solution Design

### Approach: Exclude `dataformTableConfig` from SQL merge, use it directly in `createTable`

**Phase 1: Add static defaults to `defaultConfig.js`**

Add the static (non-dynamic) portion of the Dataform table config to `ga4EventsEnhancedConfig`:

```javascript
// defaultConfig.js
const ga4EventsEnhancedConfig = {
    ...baseConfig,
    // ... existing fields ...
    dataformTableConfig: {
        type: 'incremental',
        bigquery: {
            partitionBy: 'event_date',
            clusterBy: ['event_name', 'session_id', 'page_location', 'data_is_final'],
            labels: {
                'ga4_export_fixer': 'true'
            }
        },
        onSchemaChange: 'EXTEND',
        tags: ['ga4_export_fixer'],
    },
};
```

Note: `name`, `schema`, `description`, and `columns` are NOT included — they are dynamic.

**Phase 2: Extract `dataformTableConfig` before SQL merge**

In `createEnhancedEventsTable`, extract `dataformTableConfig` from the user config before passing it to `mergeSQLConfigurations`, so it doesn't get processed by SQL-specific transformations:

```javascript
const createEnhancedEventsTable = (dataformPublish, config) => {
    // Extract dataformTableConfig before SQL merge to avoid double processing
    const userDataformTableConfig = config.dataformTableConfig;

    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);
    // mergedConfig.dataformTableConfig now contains defaults merged with user input
    // from the SQL merge, but we use the separately tracked references instead

    // ...
};
```

**Phase 3: Build final config from static defaults + dynamic fields + user overrides**

```javascript
    // Static defaults from defaultConfig (via mergedConfig)
    const staticDefaults = mergedConfig.dataformTableConfig || {};

    // Dynamic fields computed from merged SQL config
    const dynamicFields = {
        name: computedName,      // from setDefaults()
        schema: computedSchema,  // from setDefaults()
        description: tableDescription,
        columns: documentation.getColumnDescriptions(mergedConfig),
    };

    // Merge: static defaults → dynamic fields → user overrides
    const dataformTableConfig = utils.mergeDataformTableConfigurations(
        { ...staticDefaults, ...dynamicFields },
        userDataformTableConfig
    );
```

This ensures:
- Static defaults come from `defaultConfig.js` (single source of truth)
- Dynamic fields are computed and applied
- User overrides win last
- `mergeSQLConfigurations` processes `dataformTableConfig` as a regular nested object (deep merge only, no date processing or array counterpart logic applies to it)
- No double-merge issue — user overrides are applied exactly once via `mergeDataformTableConfigurations`

## Files to Modify

| File | Change |
|------|--------|
| [defaultConfig.js](defaultConfig.js) | Add static `dataformTableConfig` defaults to `ga4EventsEnhancedConfig` |
| [tables/ga4EventsEnhanced.js](tables/ga4EventsEnhanced.js) | Extract user `dataformTableConfig` before SQL merge; refactor the merge chain |
| [inputValidation.js](inputValidation.js) | Add validation for `dataformTableConfig` fields if needed |

## Examples

### Before (current)

```javascript
// defaultConfig.js — no dataformTableConfig
// tables/ga4EventsEnhanced.js — all defaults hardcoded inline
const defaultDataformTableConfig = {
    name: constants.DEFAULT_EVENTS_TABLE_NAME,
    type: 'incremental',
    schema: 'ga4_export_fixer',
    description: tableDescription,
    bigquery: { partitionBy: 'event_date', clusterBy: [...], labels: {...} },
    onSchemaChange: 'EXTEND',
    tags: ['ga4_export_fixer'],
    columns: documentation.getColumnDescriptions(mergedConfig)
};
```

### After (proposed)

```javascript
// defaultConfig.js — static defaults centralized
const ga4EventsEnhancedConfig = {
    ...baseConfig,
    // ...
    dataformTableConfig: {
        type: 'incremental',
        bigquery: { partitionBy: 'event_date', clusterBy: [...], labels: {...} },
        onSchemaChange: 'EXTEND',
        tags: ['ga4_export_fixer'],
    },
};

// tables/ga4EventsEnhanced.js — only dynamic fields computed inline
const dynamicFields = { name, schema, description, columns };
const dataformTableConfig = utils.mergeDataformTableConfigurations(
    { ...staticDefaults, ...dynamicFields },
    userDataformTableConfig
);
```

## Success Criteria

- [ ] Static `dataformTableConfig` defaults live in `defaultConfig.js`
- [ ] Dynamic fields (name, schema, description, columns) are still computed in `createTable`
- [ ] User `dataformTableConfig` overrides are applied exactly once
- [ ] `mergeSQLConfigurations` does not apply SQL-specific transformations to `dataformTableConfig` fields
- [ ] Calling `createTable` with the same config produces identical output as before
- [ ] All existing tests pass
- [ ] SQLX deployment is unaffected (does not use `dataformTableConfig`)

## Testing Strategy

- **Unit tests**: Verify `createTable` produces the same Dataform table config as before with default, partial, and full user overrides
- **Integration**: Run existing `npm test` suite
- **Manual**: Deploy a test table in Dataform with both default and custom `dataformTableConfig` and verify table properties (partitioning, clustering, schema, description)

## Non-Goals

- Changing the public API or config shape users pass in
- Adding new `dataformTableConfig` fields
- Modifying `mergeDataformTableConfigurations` behavior
- Changing SQLX deployment behavior

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Double merge alters defaults | Medium | Extract user `dataformTableConfig` before SQL merge; apply user overrides once |
| Dynamic fields overwritten by static defaults | High | Spread dynamic fields after static defaults in the merge base |
| Breaking change for existing users | High | Ensure the final merged config is byte-identical to current behavior for all existing config patterns |
