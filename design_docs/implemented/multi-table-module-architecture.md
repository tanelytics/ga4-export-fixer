# Multi-Table Module Architecture

**Status**: Planned (partially addressed by incremental improvements)
**Priority**: P0 (High)
**Estimated**: 1-2 sprints (Phases 1-3 active; Phases 4-5 deferred)
**Dependencies**: None

## Problem Statement

The package is architected around a single table type (`ga4_events_enhanced`). Adding new table modules — downstream tables like sessions, ecommerce aggregation, page views — currently requires:

- **Copy-pasting ~45 lines** of createTable lifecycle code from `tables/ga4EventsEnhanced.js`
- **Adding config** to `defaultConfig.js` (which houses both `baseConfig` and `ga4EventsEnhancedConfig`)
- **Adding validation** to `inputValidation.js` (houses both `validateBaseConfig` and `validateEnhancedEventsConfig`)
- **Duplicating documentation logic** because `documentation.js` hardcodes GA4-events-enhanced column metadata
- **Working around hardcoded constants** in `constants.js` that mix shared and GA4-specific values

Each new table type requires touching 4-5 shared files, increasing the risk of regressions and making the codebase harder to navigate.

## Goals

- A new table module can be added by creating a self-contained directory under `tables/` with 5-7 files
- No changes to shared infrastructure files are required when adding a new table module
- The public API (`ga4EventsEnhanced.createTable`, `generateSql`, `setPreOperations`, `validateBaseConfig`, `validateEnhancedEventsConfig`, `helpers`) remains unchanged
- The `createAll` wrapper pattern is architecturally supported for future use
- All existing tests pass at each phase boundary

## Current Architecture (as of v0.4.6)

```
index.js                    — exports { helpers, ga4EventsEnhanced, setPreOperations }
defaultConfig.js            — baseConfig + ga4EventsEnhancedConfig (mixed generic + specific)
constants.js                — mixed shared + GA4-specific constants
inputValidation.js          — validateBaseConfig() + validateEnhancedEventsConfig() (mixed)
utils.js                    — fully generic (no changes needed)
preOperations.js            — partially generic (sourceTableType dispatch exists)
documentation.js            — hardcodes require('./columns/*.json') for GA4 events enhanced
tables/ga4EventsEnhanced.js — createTable lifecycle + SQL generation (mixed)
helpers/                    — split by context: params, dateTime, dateFilters, urlParsing, aggregation, ga4Transforms
columns/                    — JSON metadata for GA4 events enhanced only
```

### Recent improvements already completed

Several incremental changes have already reduced coupling and improved the path toward multi-table support:

1. **Helpers refactored into contextual files** (v0.4.2) — `helpers.js` split into `helpers/{params, dateTime, dateFilters, urlParsing, aggregation, ga4Transforms}.js`. Generic vs GA4-specific helpers are now in separate files.

2. **`index.js` simplified** (v0.4.6) — No longer a flat re-export of everything. Now exports only `{ helpers, ga4EventsEnhanced, setPreOperations }`. The `setPreOperations` export wraps config merge + base validation independently.

3. **Internal SQL generation separated from exported wrappers** (v0.4.6) — `_generateEnhancedEventsSQL(mergedConfig)` is now an internal function. Exported `generateEnhancedEventsSQL(config)` handles config merge + validation once, then delegates. Same pattern for `setPreOperations`.

4. **`dataformTableConfig` migrated to `defaultConfig.js`** (v0.4.3) — Previously hardcoded in `createEnhancedEventsTable`. Now part of `ga4EventsEnhancedConfig`, making table-specific config more cohesive.

5. **Input validation supports `skipDataformContextFields`** (v0.4.6) — `validateBaseConfig` and `validateEnhancedEventsConfig` accept `options.skipDataformContextFields` for JS deployment where `self`/`incremental` aren't yet set. This makes validation reusable across deployment contexts.

6. **`preOperations.js` returns empty string gracefully** (v0.4.5) — Returns `''` when no preops are needed, making it safe to call from any table module.

7. **Integration tests added** (v0.4.5) — BigQuery-validated integration tests exist alongside unit tests.

### The createTable lifecycle (lines 319-365 of `tables/ga4EventsEnhanced.js`)

```
1. mergeSQLConfigurations(defaultConfig, config)           — generic
2. validateEnhancedEventsConfig(mergedConfig, {skip...})   — table-specific validation
3. Compute dynamic fields (dataset, table name)            — generic pattern, specific values
4. mergeDataformTableConfigurations(defaults, overrides)   — generic
5. getTableDescription(mergedConfig)                       — generic function, specific data
6. getColumnDescriptions(mergedConfig)                     — generic function, specific data
7. dataformPublish(name, config)                           — generic
8.   .preOps(ctx => setPreOperations(...))                 — generic
9.   .query(ctx => _generateEnhancedEventsSQL(...))        — TABLE-SPECIFIC
```

Steps 1, 3-8 are generic patterns; steps 2 and 9 are table-specific. The entire sequence is still embedded in one function.

### What's already generic or shared (no changes needed)

| File | Functions |
|------|-----------|
| `utils.js` | `mergeSQLConfigurations`, `mergeDataformTableConfigurations`, `queryBuilder`, `processDate`, `setDataformContext`, `selectOtherColumns`, `isDataformTableReferenceObject` |
| `helpers/aggregation.js` | `filterEventParams`, `aggregateSessionParams`, `aggregateValue`, `aggregateValues` |
| `helpers/urlParsing.js` | All URL extraction functions |
| `helpers/params.js` | `unnestEventParam`, `unnestSessionParam` |
| `helpers/dateTime.js` | `eventDate`, `getEventTimestampMicros`, `getEventDateTime` |
| `helpers/ga4Transforms.js` | `sessionId`, `fixEcommerceStruct`, `isFinalData`, `isGa4ExportColumn`, `getGa4ExportType` (GA4-specific but shared from `helpers/`) |
| `helpers/dateFilters.js` | `ga4ExportDateFilter`, `ga4ExportDateFilters` (GA4-specific but shared from `helpers/`) |
| `inputValidation.js` | `validateBaseConfig` (with `skipDataformContextFields` option) |

### What's GA4-events-enhanced specific (needs to be moved or parameterized)

| File | What's specific | Action |
|------|----------------|--------|
| `constants.js` | `DEFAULT_EVENTS_TABLE_NAME`, `DATE_COLUMN`, `INTRADAY_*`, `FRESH_*` variable names | Move to `tables/ga4EventsEnhanced/constants.js` |
| `columns/*.json` | All column metadata (descriptions, lineage, typical use, agent instructions) | Move to `tables/ga4EventsEnhanced/columns/` |
| `documentation.js` | Hardcoded `require('./columns/*.json')` on lines 1-4; `getTableDescription` event vocabulary and table features sections | Parameterize to accept column metadata |
| `tables/ga4EventsEnhanced.js` | `_generateEnhancedEventsSQL`, `createEnhancedEventsTable` | Stays; delegates lifecycle to shared `createTable.js` |
| `inputValidation.js` | `validateEnhancedEventsConfig` | Move to `tables/ga4EventsEnhanced/validation.js` |
| `defaultConfig.js` | `ga4EventsEnhancedConfig` (including `dataformTableConfig` defaults) | Move to `tables/ga4EventsEnhanced/config.js` |

### Helpers stay in `helpers/` as-is

`helpers/ga4Transforms.js` and `helpers/dateFilters.js` contain GA4-specific functions (`sessionId`, `fixEcommerceStruct`, `isFinalData`, `ga4ExportDateFilter`, etc.), but they remain in `helpers/` alongside the generic helper files. The `helpers/` directory is a flat shared library — table modules import what they need from it. New table modules may add new helper files here if needed, but existing files stay put.

## Solution Design

### Target File Structure

```
index.js                          — public API (backward compatible, exports { helpers, ga4EventsEnhanced, setPreOperations })
createTable.js                    — NEW: shared createTable lifecycle
defaultConfig.js                  — baseConfig only
constants.js                      — unchanged (all constants remain here)
inputValidation.js                — validateBaseConfig only
utils.js                          — unchanged (+ getDatasetName moved here from ga4EventsEnhanced.js)
preOperations.js                  — unchanged
documentation.js                  — parameterized (no hardcoded JSON imports); shared utilities (composeDescription, getLineageText, buildConfigNotes)
helpers/                          — unchanged (already split: params, dateTime, dateFilters, urlParsing, aggregation, ga4Transforms)
tables/
  ga4EventsEnhanced/
    index.js                      — exports { createTable, generateSql, setPreOperations }
    config.js                     — ga4EventsEnhancedConfig (including dataformTableConfig defaults)
    validation.js                 — validateEnhancedEventsConfig
    generateSql.js                — _generateEnhancedEventsSQL + getFinalColumnOrder
    tableDescription.js           — GA4-specific description sections (event vocabulary, table features)
    columns/                      — column metadata JSON (moved from top-level)
      columnDescriptions.json
      columnLineage.json
      columnTypicalUse.json
      tableAgentInstructions.json
  sessions/                       — FUTURE: same structure
  ecommerce/                      — FUTURE: same structure
```

### Table Module Interface

Each table module exports an object conforming to this interface:

```javascript
{
  defaultConfig: Object,              // extends baseConfig (includes dataformTableConfig defaults)
  defaultTableName: string,           // e.g. 'ga4_events_enhanced'
  validate: Function,                 // (mergedConfig, options?) => void (throws on invalid)
                                      //   options.skipDataformContextFields: skip self/incremental validation
  generateSql: Function,             // (mergedConfig) => string
  getColumnDescriptions: Function,    // (mergedConfig) => Dataform columns object
  getTableDescription: Function,      // (mergedConfig) => string
  setPreOperations: Function,         // (mergedConfig) => string (return '' if none needed)
}
```

### Phase 1: Extract shared `createTable` infrastructure

**Goal**: Extract the generic lifecycle from `createEnhancedEventsTable` into a reusable function.

**What's already done**:
- `dataformTableConfig` defaults already live in `ga4EventsEnhancedConfig` within `defaultConfig.js`
- Config merge + validation are already separated from internal logic (`_generateEnhancedEventsSQL` vs `generateEnhancedEventsSQL`)
- `getDatasetName` helper is already implemented within `createEnhancedEventsTable`
- The `setPreOperations` wrapper in `index.js` already demonstrates the pattern of independent config merge + validation + delegation

**What remains**:
- Extract the `createTable` lifecycle (lines 319-365 of `tables/ga4EventsEnhanced.js`) into a shared function
- Move `getDatasetName` to `utils.js` (it's generic — extracts dataset from sourceTable)

**New file: `createTable.js`**

```javascript
const utils = require('./utils.js');

/**
 * Shared createTable lifecycle for all table modules.
 *
 * @param {Function} dataformPublish - Dataform publish() function
 * @param {Object} userConfig - User-provided configuration
 * @param {Object} tableModule - Table module definition (see interface above)
 */
const createTable = (dataformPublish, userConfig, tableModule) => {
    const mergedConfig = utils.mergeSQLConfigurations(tableModule.defaultConfig, userConfig);
    tableModule.validate(mergedConfig, { skipDataformContextFields: true });

    const dataset = utils.getDatasetName(mergedConfig.sourceTable);

    const dataformTableConfig = utils.mergeDataformTableConfigurations(
        {
            ...JSON.parse(JSON.stringify(tableModule.defaultConfig.dataformTableConfig || {})),
            name: `${tableModule.defaultTableName}_${dataset.replace('analytics_', '')}`,
            schema: dataset,
            columns: tableModule.getColumnDescriptions(mergedConfig),
        },
        userConfig.dataformTableConfig
    );

    const tableDescription = tableModule.getTableDescription({
        ...mergedConfig,
        dataformTableConfig
    });

    if (!dataformTableConfig.description) {
        dataformTableConfig.description = tableDescription;
    }

    return dataformPublish(dataformTableConfig.name, dataformTableConfig)
        .preOps(ctx => tableModule.setPreOperations(utils.setDataformContext(ctx, mergedConfig)))
        .query(ctx => tableModule.generateSql(utils.setDataformContext(ctx, mergedConfig)));
};
```

**Changes:**
| File | Change |
|------|--------|
| `createTable.js` | NEW — shared lifecycle (~40 LOC) |
| `utils.js` | Move `getDatasetName` from `tables/ga4EventsEnhanced.js` (~10 LOC) |
| `tables/ga4EventsEnhanced.js` | Replace inline lifecycle with `createTable(publish, config, module)` |
| `package.json` | Add `createTable.js` to `files` array |

**Backward compatibility**: `ga4EventsEnhanced.createTable(publish, config)` signature unchanged. Internally delegates to `createTable`. The `skipDataformContextFields` option (added in v0.4.6) is used automatically.

### Phase 2: Make `documentation.js` table-agnostic

**Goal**: Remove hardcoded JSON imports. Parameterize by column metadata.

**What's already done**:
- `documentation.js` already has well-structured helper functions: `composeDescription`, `getLineageText`, `buildConfigNotes`
- `getColumnDescriptions` already handles dynamic promoted event params via config
- `getTableDescription` is logically sectioned (overview, key fields, synonyms, filtering, vocabulary, features, attribution, config dump)

**What remains**:
- Remove hardcoded `require('./columns/*.json')` on lines 1-4
- Parameterize `getColumnDescriptions` and `getTableDescription` to accept column metadata
- Move GA4-specific description sections (event vocabulary, table features) into the table module

**Current** (lines 1-4 of `documentation.js`):
```javascript
const columnDescriptions = require('./columns/columnDescriptions.json');
const columnLineage = require('./columns/columnLineage.json');
const columnTypicalUse = require('./columns/columnTypicalUse.json');
const tableAgentInstructions = require('./columns/tableAgentInstructions.json');
```

**After**: Functions accept column metadata as a parameter:

```javascript
// getColumnDescriptions(config, columnMetadata) where columnMetadata = {
//   descriptions, lineage, typicalUse
// }

// getTableDescription becomes a generic builder that accepts section providers
// buildTableDescription(config, { columnMetadata, agentInstructions, customSections })
```

Each table module provides its own `getTableDescription` function that calls shared utilities from `documentation.js` and adds table-specific sections (event vocabulary, table features).

The existing helper functions (`composeDescription`, `getLineageText`, `buildConfigNotes`) remain in `documentation.js` as shared utilities — they are already generic.

**Changes:**
| File | Change |
|------|--------|
| `documentation.js` | Remove hardcoded imports; parameterize `getColumnDescriptions`, extract generic description builder from `getTableDescription` |
| `tables/ga4EventsEnhanced/tableDescription.js` | NEW — GA4-specific description sections (event vocabulary, table features) |
| `columns/` → `tables/ga4EventsEnhanced/columns/` | MOVE column metadata JSON files |
| `tables/ga4EventsEnhanced/index.js` | Load local column metadata, wire into table module interface |
| `tests/documentation.test.js` | Pass column metadata to parameterized functions |

### Phase 3: Per-table config and validation

**Goal**: Move table-specific config and validation into the table module directory.

**What's already done**:
- `validateBaseConfig` already supports `skipDataformContextFields` option, making it reusable as a foundation for any table module's validation
- `validateEnhancedEventsConfig` already calls `validateBaseConfig` as its first step (line 111)
- `ga4EventsEnhancedConfig` already extends `baseConfig` with spread (line 35 of `defaultConfig.js`)
- `dataformTableConfig` defaults (partitioning, clustering, labels) already colocated in `ga4EventsEnhancedConfig`

**What remains**:
- Move `ga4EventsEnhancedConfig` from `defaultConfig.js` to `tables/ga4EventsEnhanced/config.js`
- Move `validateEnhancedEventsConfig` from `inputValidation.js` to `tables/ga4EventsEnhanced/validation.js`
- Leave only `baseConfig` in `defaultConfig.js` and `validateBaseConfig` in `inputValidation.js`

**Changes:**
| File | Change |
|------|--------|
| `tables/ga4EventsEnhanced/config.js` | NEW — `ga4EventsEnhancedConfig` (moved from `defaultConfig.js`) |
| `tables/ga4EventsEnhanced/validation.js` | NEW — `validateEnhancedEventsConfig` (moved from `inputValidation.js`) |
| `defaultConfig.js` | Remove `ga4EventsEnhancedConfig`; keep only `baseConfig` |
| `inputValidation.js` | Remove `validateEnhancedEventsConfig`; keep only `validateBaseConfig` |
| `index.js` | Update require paths for re-exports |

**Backward compatibility**: `validateEnhancedEventsConfig` is not part of the public API (`index.js`), so moving it is an internal change only. `index.js` require paths for `ga4EventsEnhanced` update to point at the new module directory.

### ~~Phase 4: Reorganize constants~~ (skipped)

Constants remain in a single `constants.js` file. Downstream tables will likely reuse existing constants rather than introduce new ones, so splitting adds complexity without benefit. Can be revisited if the file grows significantly.

### Phase 4: `createAll` wrapper (deferred)

**Goal**: Single function to create all tables from one config.

```javascript
const createAll = (dataformPublish, config) => {
    const tables = {};
    tables.ga4EventsEnhanced = ga4EventsEnhanced.createTable(dataformPublish, config);
    // Future: tables.sessions = sessions.createTable(dataformPublish, {
    //     sourceTable: tables.ga4EventsEnhanced,
    //     sourceTableType: 'DOWNSTREAM',
    //     ...config.sessions
    // });
    return tables;
};
```

Deferred until the first downstream table is ready. The architecture from Phases 1-3 supports this without changes.

### Phase 5: Pre-operations evolution (deferred)

**Goal**: Make pre-operations pluggable per table module.

Currently `preOperations.js` uses `sourceTableType === 'GA4_EXPORT'` checks. Future approach: the table module interface includes an optional `getPreOperationItems()` function that returns table-specific pre-operation components.

Deferred until the second table type is built. The current `sourceTableType` dispatch works for two types (GA4_EXPORT + DOWNSTREAM).

## Phase Dependencies

```
Phase 1 → Phase 2 (can run in parallel with Phase 3)
Phase 1 → Phase 3 (can run in parallel with Phase 2)
Phases 1-3 → Phase 4 (deferred)
Phase 4 → Phase 5 (deferred)
```

## What adding a new table module looks like (post-refactoring)

Create `tables/sessions/` with:

1. `config.js` — extends `baseConfig` with sessions-specific defaults
2. `validation.js` — calls `validateBaseConfig` + sessions-specific checks
3. `generateSql.js` — session aggregation SQL using shared `queryBuilder` and importing helpers as needed from `helpers/`
4. `columns/` — column description, lineage, typical use JSONs
5. `tableDescription.js` — sessions-specific description sections
6. `index.js` — wire into table module interface, call shared `createTable`

**Zero changes** to: `utils.js`, `preOperations.js`, `documentation.js`, `inputValidation.js`, `constants.js`, `helpers/`.

## Success Criteria

- [ ] A new table module requires only files under `tables/<name>/`
- [ ] No shared infrastructure changes needed when adding a table module
- [ ] Public API backward compatible (same exports from `index.js`)
- [ ] All existing tests pass at each phase boundary
- [ ] Generated SQL is byte-identical to pre-refactoring output
- [ ] Dataform table config (description, columns, partitioning) is identical

## Testing Strategy

- **Per phase**: Run `npm test` (covers SQL generation, config merge, pre-operations, documentation)
- **Per phase**: Run `npm run test:integration` (BigQuery-validated integration tests, added in v0.4.5)
- **Phase 1**: Comparison test — verify createTable output is identical before/after extraction
- **Phase 2**: Update `tests/documentation.test.js` to pass column metadata explicitly
- **Phases 3-4**: Verify public API exports unchanged via require path tests
- **Integration**: Deploy to Dataform test environment after each phase

## Non-Goals

- Changing the public API or config shape users pass in
- Adding new table types in this refactoring (that's a follow-up)
- Modifying SQL generation logic
- Changing Dataform deployment behavior (JS or SQLX)
- Refactoring `preOperations.js` internals (deferred to Phase 6)

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking `defaultConfig.js` imports for users who require it directly | Low | Neither `ga4EventsEnhancedConfig` nor `defaultConfig` are part of the public API (`index.js` exports only `helpers`, `ga4EventsEnhanced`, `setPreOperations`). Internal path change only. |
| Column JSON file path changes break tests | Low | Only used internally; update test imports in Phase 2 |
| `package.json` `files` array becomes stale | Low | Update `files` array in each phase; verify with `npm pack --dry-run`. Note: `files` currently lists `"tables"` as a directory, which will automatically include subdirectories |
| Dataform runtime compatibility with new file structure | Medium | Test via integration tests (`npm run test:integration`) and deploy to test environment after each phase |
