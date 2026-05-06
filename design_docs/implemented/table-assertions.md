# Table Assertions

**Status**: Planned
**Priority**: P1 (Medium)
**Estimated**: 1 day
**Dependencies**: None (builds on existing table module architecture)

## Problem Statement

The ga4_events_enhanced table transforms raw GA4 export data through multiple SQL steps (event extraction, session aggregation, joins, column reordering). There is currently no automated way to verify that the transformation preserves data integrity -- for example, that item revenue totals in the enhanced table match the raw export data.

**Current State:**
- No assertion infrastructure exists in the package
- Users must manually write and maintain separate Dataform assertion files to validate their enhanced tables
- Assertions need access to the same configuration (sourceTable, date filters, excluded events, timezone, etc.) to query raw data correctly, leading to duplicated logic

**Impact:**
- Silent data quality issues can go undetected (e.g., revenue discrepancies due to ecommerce struct fixes, item list attribution, or event filtering)
- Users with ecommerce data have no easy way to validate that transformations preserve revenue totals

## Goals

**Primary Goal:** Provide exportable assertion query generators that reuse the table's configuration, starting with an item_revenue reconciliation assertion for ga4_events_enhanced.

**Success Metrics:**
- Assertion SQL generators are co-located with the table module they validate
- Assertions reuse the same config (sourceTable, date filters, excluded events, timezone) as the table SQL
- Assertions are exported but not documented in the public API yet (hidden feature)
- The item_revenue reconciliation assertion correctly detects mismatches between enhanced and raw data

## Solution Design

### Overview

Add an `assertions/` directory inside each table module folder. Each assertion is a function that takes the merged table config and returns a SQL query string. The assertions are exported from the table module's `index.js` but not advertised in documentation.

### Architecture

```
tables/ga4EventsEnhanced/
  assertions/
    index.js                    -- re-exports all assertion generators
    itemRevenue.js              -- item_revenue reconciliation assertion
  columns/
  config.js
  index.js                      -- adds assertions to module exports
  tableDescription.js
  validation.js
```

Each assertion module exports a function with this signature:

```js
/**
 * @param {string} tableRef - Fully qualified reference to the table being asserted against
 *   (e.g., ctx.ref('ga4_events_enhanced_123456789') in Dataform, or a backtick-quoted string).
 * @param {Object} config - Table configuration (same shape as generateSql receives).
 *   Uses config.sourceTable for the raw export tables.
 * @returns {string} SQL query that returns violating rows (0 rows = assertion passes)
 */
const generateAssertionSql = (tableRef, config) => { ... }
```

`tableRef` is a separate parameter rather than part of the config object because:
- It's not a table configuration concern -- it's an assertion-specific input
- All assertions for a given table share the same `tableRef`, so it stays consistent across assertion types
- When assertions are eventually bundled with `createTable`, the table ref is already known from the publish step and can be injected automatically
- The table might be created via SQLX (where the name is in the SQLX config block), so it can't be derived from the JS config

The table module's `index.js` exports assertion generators alongside the existing API:

```js
module.exports = {
    createTable: createEnhancedEventsTable,
    generateSql: generateEnhancedEventsSQL,
    setPreOperations,
    getColumnDescriptions,
    getTableDescription,
    // hidden -- not in docs yet
    assertions: {
        itemRevenue: itemRevenueAssertion,
    }
}
```

Users can consume them in a Dataform assertion file:

```js
// definitions/assertions/item_revenue_check.js
const { ga4EventsEnhanced } = require('ga4-export-fixer');

const config = {
    sourceTable: '`my-project.analytics_123456789.events_*`',
    excludedEvents: ['session_start', 'first_visit'],
    timezone: 'Europe/Helsinki',
};

// assert() is a Dataform global
assert('item_revenue_check', {
    schema: 'analytics_123456789',
    tags: ['ga4_assertions'],
}).query(ctx => {
    return ga4EventsEnhanced.assertions.itemRevenue(
        ctx.ref('ga4_events_enhanced_123456789'),
        config
    );
});
```

### Item Revenue Reconciliation Assertion

**Logic:**

1. **Enhanced table side**: Query item_revenue grouped by (event_date, item_id) from the last 5 days where `data_is_final = true`, using the `tableRef` parameter
2. **Raw export side**: Query the same data from the raw GA4 export tables (`config.sourceTable`) for the same days, applying the same row-level filters that determine which data ends up in the enhanced table
3. **Compare**: FULL OUTER JOIN on (event_date, item_id), return rows where the revenue totals don't match

The assertion query returns mismatched rows. If 0 rows are returned, the assertion passes.

#### Config fields used by the raw side query

The raw side needs to replicate which rows ended up in the enhanced table. Only three config fields affect row selection for revenue data:

| Config field | Why it matters | How the assertion uses it |
|---|---|---|
| `includedExportTypes` | Determines which raw tables (`events_*`, `events_intraday_*`, `events_fresh_*`) are queried. If `fresh: false`, no fresh data is in the enhanced table. | Builds `_table_suffix` filters using `ga4ExportDateFilter()` for each enabled export type |
| `excludedEvents` | Removes events from the pipeline. If an ecommerce event were excluded, it would affect revenue totals. | Applied as `event_name NOT IN (...)` on the raw side, same as the enhanced table pipeline |
| `dataIsFinal` | Determines which rows are marked `data_is_final = true` in the enhanced table. The assertion only checks final data. | Applied using the same `isFinalData()` helper to filter raw rows to the same finality condition |

Config fields that do **not** affect the assertion:
- `excludedColumns`, `excludedEventParams`, `eventParamsToColumns` -- structural (column-level), not row-level
- `fixEcommerceStruct` -- modifies `ecommerce.purchase_revenue`, not `items[].item_revenue`
- `itemListAttribution` -- modifies `item_list_name/id/index`, not revenue
- `bufferDays` -- adds extra days for session overlap; doesn't affect which rows exist at the (event_date, item_id) grain for final data
- `sessionParams`, `timezone`, `customTimestampParam` -- don't affect which rows exist or their revenue values

#### Date filtering on the raw side

The raw side does **not** reuse `ga4ExportDateFilters()` from the main pipeline. That helper is designed for the incremental processing workflow -- it resolves BigQuery variables (`date_range_start`, `intraday_date_range_start`), applies `bufferDays`, and handles fresh/intraday overlap deduplication.

The assertion needs a simpler, self-contained date filter:
- Uses `ga4ExportDateFilter()` (the single-export-type helper) for each enabled export type in `includedExportTypes`
- Date range is hardcoded: `date_sub(current_date(), interval 5 day)` to `current_date()`
- Combined with the `data_is_final` condition to select only stable data

This avoids any dependency on pre-operation variables or incremental state.

**Key design decisions:**
- The assertion queries the enhanced table via the `tableRef` parameter (a fully qualified table reference passed by the user, e.g., from `ctx.ref()`)
- The raw side builds its own date filter using the low-level `ga4ExportDateFilter()` helper per export type, rather than the high-level `ga4ExportDateFilters()` pipeline helper
- The 5-day lookback window with `data_is_final = true` filter avoids asserting against data that is still subject to change
- Revenue is compared at the (event_date, item_id) grain -- granular enough to catch issues, aggregated enough to tolerate row-level reordering
- Item list attribution does not modify item_revenue, so the assertion should be unaffected. If a discrepancy at the item_id level is detected when attribution is enabled, the assertion should surface it -- that would indicate a bug in the attribution logic

**SQL sketch:**

```sql
with enhanced_revenue as (
    select
        event_date,
        item.item_id,
        sum(item.item_revenue) as total_item_revenue,
        count(*) as item_count
    from
        {tableRef},
        unnest(items) as item
    where
        data_is_final = true
        and event_date >= date_sub(current_date(), interval 5 day)
        and event_name in ({ecommerce_events})
    group by event_date, item.item_id
),
raw_revenue as (
    select
        cast(event_date as date format 'YYYYMMDD') as event_date,
        item.item_id,
        sum(item.item_revenue) as total_item_revenue,
        count(*) as item_count
    from
        {sourceTable},
        unnest(items) as item
    where
        -- date filter: per-export-type _table_suffix filters built from includedExportTypes
        -- using ga4ExportDateFilter() with start = date_sub(current_date(), interval 5 day), end = current_date()
        ({export_type_date_filters})
        -- same excluded events as the enhanced table
        {excluded_events_filter}
        and event_name in ({ecommerce_events})
        -- same data_is_final logic applied to raw data
        and {data_is_final_condition}
        and cast(event_date as date format 'YYYYMMDD') >= date_sub(current_date(), interval 5 day)
    group by event_date, item.item_id
)
select
    coalesce(e.event_date, r.event_date) as event_date,
    coalesce(e.item_id, r.item_id) as item_id,
    e.total_item_revenue as enhanced_revenue,
    r.total_item_revenue as raw_revenue,
    e.item_count as enhanced_count,
    r.item_count as raw_count
from
    enhanced_revenue e
full outer join
    raw_revenue r using(event_date, item_id)
where
    -- rows where revenue doesn't match (with tolerance for floating point)
    round(coalesce(e.total_item_revenue, 0), 2) != round(coalesce(r.total_item_revenue, 0), 2)
    or e.item_count != r.item_count
    or e.event_date is null
    or r.event_date is null
```

### Implementation Plan

**Phase 1: Assertion infrastructure + first assertion** (~4 hours)
- [ ] Create `tables/ga4EventsEnhanced/assertions/` directory
- [ ] Implement `itemRevenue.js` with the reconciliation query generator
- [ ] Create `assertions/index.js` re-exporting all assertions
- [ ] Export assertions from `tables/ga4EventsEnhanced/index.js`
- [ ] Handle the config merge + validation pattern (same as `generateSql` wrapper)

**Phase 2: Testing** (~2 hours)
- [ ] Unit test: assertion SQL generation with various configs
- [ ] Unit test: excluded events and date filters are correctly applied in raw query
- [ ] Unit test: assertion works with different `dataIsFinal` detection methods
- [ ] Validate generated SQL against BigQuery dry-run (manual)

### Files to Modify/Create

**New files:**
- `tables/ga4EventsEnhanced/assertions/index.js` - Re-exports assertion generators (~5 LOC)
- `tables/ga4EventsEnhanced/assertions/itemRevenue.js` - Item revenue reconciliation assertion (~80 LOC)

**Modified files:**
- `tables/ga4EventsEnhanced/index.js` - Add assertions to exports (~5 LOC)

## Examples

### Using the assertion in Dataform JS

```js
const { ga4EventsEnhanced } = require('ga4-export-fixer');

const config = {
    sourceTable: '`my-project.analytics_123456789.events_*`',
    excludedEvents: ['session_start', 'first_visit'],
    timezone: 'Europe/Helsinki',
};

// Create the table
ga4EventsEnhanced.createTable(publish, config);

// Create assertion against it
assert('item_revenue_reconciliation', {
    schema: 'analytics_123456789',
    tags: ['ga4_data_quality'],
}).query(ctx => {
    return ga4EventsEnhanced.assertions.itemRevenue(
        ctx.ref('ga4_events_enhanced_123456789'),
        config
    );
});
```

### Using the assertion as standalone SQL (test mode)

```js
const { ga4EventsEnhanced } = require('ga4-export-fixer');

const sql = ga4EventsEnhanced.assertions.itemRevenue(
    '`my-project.analytics_123456789.ga4_events_enhanced_123456789`',
    {
        sourceTable: '`my-project.analytics_123456789.events_*`',
        test: true,
        testConfig: {
            dateRangeStart: '2026-04-10',
            dateRangeEnd: '2026-04-13',
        },
    }
);
console.log(sql); // can paste into BigQuery console
```

## Success Criteria

- [ ] `ga4EventsEnhanced.assertions.itemRevenue(tableRef, config)` generates valid SQL
- [ ] The raw-side query applies the same date filters and event exclusions as the enhanced table
- [ ] Floating point differences are handled (round to 2 decimal places)
- [ ] The assertion correctly returns 0 rows when data matches
- [ ] The assertion returns mismatched rows with both enhanced and raw values for debugging
- [ ] All existing tests pass (no regressions)

## Testing Strategy

**Unit tests:**
- SQL generation with minimal config
- SQL generation with different `includedExportTypes` combinations (daily only, daily+intraday, all three)
- SQL generation with excluded events that overlap with ecommerce events
- SQL generation with different `dataIsFinal` detection methods (`DAY_THRESHOLD` vs `EXPORT_TYPE`)
- Verify raw side uses `ga4ExportDateFilter()` (per-export-type) not `ga4ExportDateFilters()` (pipeline helper)

**Manual validation:**
- Run generated SQL against a real BigQuery dataset with known-good data
- Intentionally corrupt enhanced table data and verify the assertion catches it

## Non-Goals

**Not in this feature:**
- Integrating assertions into `createTable` -- deferred until the delivery mechanism is decided
- Providing a Dataform `assert()` wrapper -- users call Dataform's `assert()` themselves
- Documenting assertions in the public API -- hidden feature until the pattern is validated
- Adding assertions for other table modules -- only ga4_events_enhanced for now
- Building a generic assertion framework -- start concrete, generalize later if needed

## Resolved Questions

1. **Table reference for assertions**: Passed as a separate `tableRef` parameter, not inside the config object. This keeps the config generic (same object used for table creation and assertions), avoids assertion-specific fields leaking into the config, and naturally supports future bundling with `createTable` (where the table ref is already known from the publish step). The table reference cannot be derived from the JS config because the table might be created via SQLX. In Dataform, users pass `ctx.ref('table_name')`; for standalone use, a backtick-quoted string works.

2. **Lookback window**: Hardcoded to 5 days. This provides a comfortable margin beyond the default `dataIsFinal.dayThreshold` of 3, ensuring only stable data is asserted against.

3. **Item list attribution impact**: Item list attribution modifies `item_list_name/id/index` but not `item_revenue`. The assertion compares revenue at the (event_date, item_id) grain, so attribution should not cause false positives. If it does, the assertion correctly flags it as a bug in the attribution logic.

## Future Work

- Additional assertions: row count reconciliation, event_name distribution checks, session_id null rate
- Generic assertion infrastructure that table modules can plug into
- Integration with `createTable` for one-call table+assertions creation
- Configurable assertion severity (warning vs. failure)

---

**Document created**: 2026-04-15
**Last updated**: 2026-04-15
