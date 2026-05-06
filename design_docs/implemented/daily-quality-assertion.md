# Daily Quality Assertion

**Status**: Planned
**Priority**: P1 (Medium)
**Estimated**: 0.5 days
**Dependencies**: Table assertions infrastructure (implemented in `tables/ga4EventsEnhanced/assertions/`)

## Problem Statement

The existing `itemRevenue` assertion validates revenue totals at the (event_date, item_id) grain, but there is no assertion that validates **overall daily data completeness and consistency** between the enhanced events table and the raw GA4 export.

Three classes of data quality issues are currently undetected:

1. **Event or session count drift** -- The enhanced table could silently drop or duplicate events relative to the raw data, without affecting item revenue totals.
2. **Missing day partitions** -- A day with raw data could be absent from the enhanced table entirely (e.g., due to a partial run or pipeline failure), with no automated alert.
3. **Non-final data inflation** -- For days where `data_is_final = false`, the enhanced table could have *more* events than raw data, indicating a duplication bug in incremental processing.

## Goals

- A single assertion query that validates all three checks at the day level
- Reuses the same table configuration as the enhanced table (sourceTable, excludedEvents, includedExportTypes, dataIsFinal)
- Follows the same architecture as the existing `itemRevenue` assertion (same function signature, same config merge pattern)
- Returns violating rows with a clear `violation_type` column for debugging

## Solution Design

### Can all three checks be in one query?

**Yes.** All three checks operate at the same grain: **(event_date, data_is_final)**. A single query can:

1. Aggregate both enhanced and raw data by (event_date, data_is_final)
2. FULL OUTER JOIN the two sides
3. Apply different violation conditions in the WHERE clause

This is more efficient than three separate assertions (one compilation result, one BigQuery scan per side) and produces a single result set where each violating row is labeled with its violation type.

### Architecture

New file in the existing assertions directory:

```
tables/ga4EventsEnhanced/assertions/
  index.js                    -- add dailyQuality export
  itemRevenue.js              -- existing
  dailyQuality.js             -- NEW
```

### Function signature

Same pattern as `itemRevenue`:

```js
/**
 * @param {string} tableRef - Fully qualified reference to the enhanced table
 * @param {Object} config - Table configuration (same shape as generateSql receives)
 * @returns {string} SQL query returning violating rows (0 rows = assertion passes)
 */
const generateDailyQualityAssertionSql = (tableRef, config) => { ... }
```

Exported as `assertions.dailyQuality`.

### Config fields used

Same three fields that affect row selection on the raw side (identical to `itemRevenue`):

| Config field | Usage |
|---|---|
| `includedExportTypes` | Builds `_table_suffix` filters via `ga4ExportDateFilter()` per enabled export type |
| `excludedEvents` | `event_name NOT IN (...)` on the raw side |
| `dataIsFinal` | `isFinalData()` helper to compute `data_is_final` on raw side |

### SQL Design

```sql
with enhanced_daily as (
    select
        event_date,
        data_is_final,
        count(distinct session_id) as session_count,
        count(*) as event_count,
        coalesce(sum((select sum(item.item_revenue) from unnest(items) as item)), 0) as total_item_revenue
    from
        {tableRef}
    where
        event_date >= date_sub(current_date(), interval 5 day)
    group by event_date, data_is_final
),
raw_daily as (
    select
        cast(event_date as date format 'YYYYMMDD') as event_date,
        {data_is_final_condition} as data_is_final,
        count(distinct concat(user_pseudo_id, cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string))) as session_count,
        count(*) as event_count,
        coalesce(sum((select sum(item.item_revenue) from unnest(items) as item)), 0) as total_item_revenue
    from
        {sourceTable}
    where
        ({export_type_date_filters})
        {excluded_events_filter}
        and cast(event_date as date format 'YYYYMMDD') >= date_sub(current_date(), interval 5 day)
    group by event_date, data_is_final
)
select
    coalesce(e.event_date, r.event_date) as event_date,
    coalesce(e.data_is_final, r.data_is_final) as data_is_final,
    e.session_count as enhanced_sessions,
    r.session_count as raw_sessions,
    e.event_count as enhanced_events,
    r.event_count as raw_events,
    round(e.total_item_revenue, 2) as enhanced_revenue,
    round(r.total_item_revenue, 2) as raw_revenue,
    case
        when e.event_date is null and r.event_count > 0
            then 'MISSING_DAY'
        when coalesce(e.data_is_final, r.data_is_final) = true
            and e.session_count != r.session_count
            then 'SESSION_COUNT_MISMATCH'
        when coalesce(e.data_is_final, r.data_is_final) = true
            and e.event_count != r.event_count
            then 'EVENT_COUNT_MISMATCH'
        when coalesce(e.data_is_final, r.data_is_final) = true
            and round(coalesce(e.total_item_revenue, 0), 2) != round(coalesce(r.total_item_revenue, 0), 2)
            then 'REVENUE_MISMATCH'
        when coalesce(e.data_is_final, r.data_is_final) = false
            and coalesce(e.event_count, 0) > coalesce(r.event_count, 0)
            then 'NON_FINAL_EXCESS_EVENTS'
    end as violation_type
from
    enhanced_daily e
full outer join
    raw_daily r using(event_date, data_is_final)
where
    -- Check 1: Final data metrics must match exactly
    (coalesce(e.data_is_final, r.data_is_final) = true and (
        e.session_count != r.session_count
        or e.event_count != r.event_count
        or round(coalesce(e.total_item_revenue, 0), 2) != round(coalesce(r.total_item_revenue, 0), 2)
        or e.event_date is null
    ))
    or
    -- Check 2: Missing day — raw has data but enhanced doesn't (any data_is_final value)
    (e.event_date is null and r.event_count > 0)
    or
    -- Check 3: Non-final data should never have more events than raw
    (coalesce(e.data_is_final, r.data_is_final) = false
        and coalesce(e.event_count, 0) > coalesce(r.event_count, 0))
```

### Violation types

| Violation | Severity | Meaning |
|---|---|---|
| `MISSING_DAY` | High | Raw data has events for this (date, data_is_final) combination but the enhanced table has none. Covers user's check 2. |
| `SESSION_COUNT_MISMATCH` | High | Final data: distinct session count differs. Possible session_id derivation bug. |
| `EVENT_COUNT_MISMATCH` | High | Final data: event count differs. Events were dropped or duplicated. |
| `REVENUE_MISMATCH` | High | Final data: total item_revenue differs (rounded to 2 decimal places). Revenue integrity issue. |
| `NON_FINAL_EXCESS_EVENTS` | Medium | Non-final data: enhanced table has more events than raw. Indicates duplication in incremental processing. Covers user's check 3. |

### Session count on the raw side

The raw export table does not have a `session_id` column. The assertion derives it using the same logic as the enhanced table pipeline:

```sql
concat(user_pseudo_id, cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string))
```

This matches the `helpers.sessionId` expression used in the enhanced table's `event_data` step. The `cast(...as string)` is needed because `concat` with a null int would return null, but casting to string first makes the behavior match the enhanced table's session_id derivation.

**Note:** The enhanced table uses `concat(user_pseudo_id, (select value.int_value ...))` which relies on BigQuery's implicit int-to-string cast in `concat()`. The assertion uses the same expression. Sessions where `ga_session_id` is null will have `session_id = null` on both sides, and `count(distinct ...)` excludes nulls, so they cancel out correctly.

### Date filter on the raw side

Same approach as `itemRevenue`: uses `ga4ExportDateFilter()` per enabled export type with a fixed 5-day window. Does **not** use `ga4ExportDateFilters()` (the pipeline helper that depends on incremental state).

Unlike `itemRevenue`, this assertion queries **all** data (both `data_is_final = true` and `false`), so the raw side does not filter by `data_is_final`. Instead, `data_is_final` is computed as a grouping column using `isFinalData()`.

### Relationship to itemRevenue assertion

| | `itemRevenue` | `dailyQuality` |
|---|---|---|
| **Grain** | (event_date, item_id) | (event_date, data_is_final) |
| **Metrics** | item_revenue, item_count | session_count, event_count, item_revenue |
| **Scope** | Only `data_is_final = true` | Both true and false |
| **Events** | Only ecommerce events | All events (minus excluded) |
| **Purpose** | Deep revenue accuracy | Broad data completeness |

These are complementary. `dailyQuality` catches structural issues (missing days, count mismatches, duplicates). `itemRevenue` catches revenue-specific issues at a finer grain that daily totals might mask (e.g., revenue shifting between item_ids within the same day).

## Files to Modify/Create

| File | Change | Est. LOC |
|------|--------|----------|
| `tables/ga4EventsEnhanced/assertions/dailyQuality.js` | NEW -- daily quality assertion generator | ~120 |
| `tables/ga4EventsEnhanced/assertions/index.js` | MODIFY -- add dailyQuality export | +2 |
| `tests/assertions.test.js` | MODIFY -- add dailyQuality test configurations | +30 |

## Examples

### Using in Dataform

```js
const { ga4EventsEnhanced } = require('ga4-export-fixer');

const config = {
    sourceTable: '`my-project.analytics_123456789.events_*`',
    excludedEvents: ['session_start', 'first_visit'],
    timezone: 'Europe/Helsinki',
};

// Table
ga4EventsEnhanced.createTable(publish, config);

// Daily quality assertion
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

### Assertion output (violating rows)

```
| event_date | data_is_final | enhanced_sessions | raw_sessions | enhanced_events | raw_events | enhanced_revenue | raw_revenue | violation_type          |
|------------|---------------|-------------------|--------------|-----------------|------------|------------------|-------------|-------------------------|
| 2026-04-14 | true          | 1,205             | 1,207        | 8,432           | 8,432      | 4521.50          | 4521.50     | SESSION_COUNT_MISMATCH  |
| 2026-04-15 | false         | null              | 342          | null            | 2,891      | null             | 1205.00     | MISSING_DAY             |
| 2026-04-16 | false         | 89                | 85           | 512             | 498        | 205.00           | 205.00      | NON_FINAL_EXCESS_EVENTS |
```

## Testing Strategy

### BigQuery dry-run validation

Same approach as `itemRevenue`: generate SQL with various configurations and validate via BigQuery dry-run.

The `testTableRef` subquery needs an additional `session_id` column compared to the existing `itemRevenue` tests:

```js
const testTableRef = `(select * replace(cast(event_date as date format 'YYYYMMDD') as event_date), true as data_is_final, concat(user_pseudo_id, cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string)) as session_id from ${testConfig.sourceTable} where _table_suffix >= cast(current_date()-5 as string format 'YYYYMMDD'))`;
```

### Test configurations

Same 8 configurations as `itemRevenue` (default, daily only, all three export types, fresh only, EXPORT_TYPE detection, custom threshold, excluded events, custom timezone).

### Integration test

Already covered -- the integration test suite collects assertion results for any assertion tagged `ga4_export_fixer`. Once this assertion is added to the test Dataform repository, it will be automatically validated.

## Success Criteria

- [ ] `ga4EventsEnhanced.assertions.dailyQuality(tableRef, config)` generates valid SQL
- [ ] Check 1: Session count, event count, and item_revenue mismatches are detected for final data
- [ ] Check 2: Missing days (raw has data, enhanced doesn't) are detected for both final and non-final data
- [ ] Check 3: Non-final data with more enhanced events than raw events is detected
- [ ] The raw-side query applies the same date filters, event exclusions, and data_is_final logic as the enhanced table
- [ ] Session count uses the same derivation as the enhanced table (`concat(user_pseudo_id, ga_session_id)`)
- [ ] All BigQuery dry-run tests pass
- [ ] All existing tests pass (no regressions)

## Non-Goals

- Validating event_params or user_properties content (structural, not count-level)
- Comparing session-level aggregated fields (landing_page, traffic_source, etc.)
- Alerting on non-final data having *fewer* events than raw (expected -- incremental may not have processed all data yet)
- Tolerance thresholds for non-final data counts (hard pass/fail for now)

## Resolved Questions

1. **One query or multiple?** One query. All three checks share the same (event_date, data_is_final) grain and the same raw-side scan. Splitting would triple the BigQuery cost for no benefit.

2. **Session count feasibility on raw side?** Yes. The session_id is derived from `concat(user_pseudo_id, ga_session_id)` which is available in the raw export's `event_params`. The excluded events filter is applied before counting, so sessions with only excluded events are correctly omitted from both sides.

3. **Why not filter non-final data from the raw side?** Unlike `itemRevenue` (which only checks final data), this assertion specifically needs to validate non-final data behavior (checks 2 and 3). The raw side computes `data_is_final` as a grouping column rather than a filter.

---

**Document created**: 2026-04-16
**Last updated**: 2026-04-16
