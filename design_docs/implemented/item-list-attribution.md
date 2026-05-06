# Item List Attribution

**Status**: Planned
**Priority**: P1 (Medium)
**Estimated**: 4-6 hours
**Dependencies**: None

## Problem Statement

GA4's ecommerce `items` array loses `item_list_name` and `item_list_index` attribution downstream of `view_item_list` and `select_item` events. When a user views an item in a list and later purchases it, the `item_list_name` on the purchase event is empty. This makes it impossible to attribute conversions back to the product list that originated them.

**Current state:**
- `items` is passed through as-is in `event_data` (`items: 'items'` at `tables/ga4EventsEnhanced/index.js:216`)
- No attribution logic exists — item list fields are empty on non-list events

**Impact:**
- Ecommerce sites cannot attribute revenue to product lists
- Common GA4 analysis pattern (which list drives the most revenue) is impossible without manual SQL

## Goals

**Primary goal:** Add an optional CTE that attributes `item_list_name`, `item_list_index`, and `item_list_id` from list/promotion events (`view_item_list`, `select_item`, `view_promotion`, `select_promotion`) to downstream events (e.g., `add_to_cart`, `purchase`) using a configurable lookback window.

**Success metrics:**
- Feature disabled by default — no change to existing SQL output
- When enabled with session lookback: items on purchase events carry `item_list_name` from the most recent `select_item`/`select_promotion` in the same session
- When enabled with time lookback: attribution window uses microsecond range, `bufferDays` auto-adjusts

## Solution Design

### Overview

A new optional `item_list_data` CTE is inserted between `event_data` and `session_data`. When enabled, it reads from `event_data`, unnests items, applies attribution via a `LAST_VALUE` window function, rebuilds the items array per event, and replaces the `items` column in `event_data` via a self-join.

When disabled (`itemListAttribution: undefined`), no CTE is added and no SQL changes.

### Configuration

New config property in `tables/ga4EventsEnhanced/config.js`:

```javascript
itemListAttribution: undefined, // disabled by default; set to an object to enable
```

When enabled:

```javascript
itemListAttribution: {
    lookbackType: 'SESSION',     // 'SESSION' (default) or 'TIME'
    lookbackTimeMs: 86400000,    // 24h in ms, only used with 'TIME' lookbackType
}
```

### bufferDays auto-adjustment

When `lookbackType: 'TIME'`, the lookback window may span more days than `bufferDays` provides. During SQL generation, compute the effective buffer:

```javascript
const lookbackDays = Math.ceil(config.itemListAttribution.lookbackTimeMs / (24*60*60*1000));
const effectiveBufferDays = Math.max(config.bufferDays, lookbackDays);
```

Use `effectiveBufferDays` only in the `event_data` CTE's date filter (via `ga4ExportDateFilters`). The main incremental date filter in `final` is unchanged — it's event-level, not buffer-related.

### SQL: Attribution window function

The core attribution logic uses a single `LAST_VALUE` window function over a struct containing all three fields (`item_list_name`, `item_list_id`, `item_list_index`). This is more performant than three separate window functions — one window sort instead of three.

Attribution sources are `select_item` and `select_promotion` — these are the "click" events that carry the definitive list context. The passthrough events (`view_item_list`, `view_promotion`) keep their original values but don't feed the lookback window.

**Step 1: Build the attribution struct and apply the window function:**

The window function uses `event_custom_timestamp` (when `customTimestampParam` is configured) or `event_timestamp` otherwise — matching the `timestampColumn` logic already used in `_generateEnhancedEventsSQL` for session aggregation.

**Session-based lookback** (`lookbackType: 'SESSION'`):

```sql
last_value(
  if(event_name in ('select_item', 'select_promotion'),
    struct(item.item_list_name, item.item_list_id, item.item_list_index),
    null
  ) ignore nulls
) over(
  partition by session_id, item.item_id
  order by <timestampColumn> asc
  rows between unbounded preceding and current row
) as _item_list_attribution
```

**Time-based lookback** (`lookbackType: 'TIME'`):

```sql
last_value(
  if(event_name in ('select_item', 'select_promotion'),
    struct(item.item_list_name, item.item_list_id, item.item_list_index),
    null
  ) ignore nulls
) over(
  partition by user_pseudo_id, item.item_id
  order by <timestampColumn> asc
  range between <lookbackTimeMs * 1000> preceding and current row
) as _item_list_attribution
```

Where `<timestampColumn>` is `event_custom_timestamp` if `config.customTimestampParam` is set, otherwise `event_timestamp` (reuses the existing `timestampColumn` variable from `_generateEnhancedEventsSQL`).

**Step 2: Extract fields from the struct in the REPLACE:**

```sql
coalesce(
  if(event_name in ('view_item_list', 'select_item', 'view_promotion', 'select_promotion'), item.item_list_name, _item_list_attribution.item_list_name),
  '(not set)'
) as item_list_name
```

Same pattern for `item_list_id` (string, `'(not set)'` default) and `item_list_index` (integer, `null` default).

### SQL: New CTE `item_list_data`

A single CTE handles both attribution and re-aggregation. A subquery unnests items and computes the window function, then the outer SELECT applies the struct REPLACE inside `array_agg()` and groups by `_event_row_id`:

```sql
item_list_data as (
  select
    _event_row_id,
    array_agg(
      select as struct item.* replace(
        coalesce(
          if(event_name in ('view_item_list', 'select_item', 'view_promotion', 'select_promotion'), item.item_list_name, _attr.item_list_name),
          '(not set)'
        ) as item_list_name,
        coalesce(
          if(event_name in ('view_item_list', 'select_item', 'view_promotion', 'select_promotion'), item.item_list_id, _attr.item_list_id),
          '(not set)'
        ) as item_list_id,
        coalesce(
          if(event_name in ('view_item_list', 'select_item', 'view_promotion', 'select_promotion'), item.item_list_index, _attr.item_list_index)
        ) as item_list_index
      )
    ) as items
  from (
    select
      _event_row_id,
      event_name,
      item,
      <window_function> as _attr
    from
      event_data,
      unnest(items) as item
    where
      event_name in (<ga4EcommerceEvents excluding 'refund'>)
  )
  group by
    _event_row_id
)
```

**Item struct reconstruction**: Uses `SELECT AS STRUCT item.* REPLACE(...)` to override only the three attributed fields (`item_list_name`, `item_list_id`, `item_list_index`) while passing through all other item fields unchanged. This avoids listing every GA4 item field explicitly and is robust to Google adding new fields.

**Join key**: `_event_row_id` — a `ROW_NUMBER() OVER()` assigned in `event_data`, guaranteed unique per event row within the query. The `item_list_data` CTE groups by it for a reliable 1:1 join back to `event_data`. Excluded from the final output.

### Unique event ID

The join between `event_data` and `item_list_data` needs a reliable 1:1 key. The combination `(user_pseudo_id, event_timestamp, event_name)` is not unique — multiple events can share the same triple (e.g., two `add_to_cart` events at the same microsecond).

A hybrid approach combines a deterministic hash with a non-deterministic tiebreaker:

```sql
farm_fingerprint(concat(
  user_pseudo_id, cast(event_timestamp as string), event_name,
  to_json_string(items), cast(row_number() over() as string)
)) as _event_row_id
```

**Why not plain `row_number() over()`?** BigQuery CTEs are not guaranteed to be materialized. When `event_data` is referenced by both `item_list_data` and the final SELECT, BigQuery may re-evaluate the CTE independently for each reference. `row_number() over()` without a deterministic ORDER BY can assign different numbers to the same rows across re-evaluations, breaking the 1:1 join.

**Why not plain `farm_fingerprint(...)` without `row_number()`?** Batched GA4 events with identical `(user_pseudo_id, event_timestamp, event_name, items)` produce the same hash. The `GROUP BY _event_row_id` in `item_list_data` would merge items from multiple events, and the LEFT JOIN would map the merged array back to multiple event rows.

**Why the hybrid is safe:** The `row_number() over()` inside the hash breaks ties between otherwise-identical rows, guaranteeing uniqueness. Although `row_number()` is non-deterministic across CTE re-evaluations, the only rows affected by a swap are those with identical deterministic hash parts — which means identical items (since `to_json_string(items)` is in the hash). Swapping identical items between rows produces the same final result.

When `itemListAttribution` is disabled, `_event_row_id` is not added — no change to existing behavior.

### Integration into the step pipeline

In `tables/ga4EventsEnhanced/index.js` (`_generateEnhancedEventsSQL`):

1. When `itemListAttribution` is enabled, build the `item_list_data` step (unnest + attribute + re-aggregate)
2. Insert it into the steps array between `eventDataStep` and `sessionDataStep`
3. In `finalStep`, replace `items` with a reference to `item_list_data.items`, add a LEFT JOIN to `item_list_data` on `_event_row_id`, and exclude `_event_row_id` from the final output.

```javascript
// When itemListAttribution is enabled:
const steps = [
    eventDataStep,
    itemListDataStep,  // unnest items, attribute, re-aggregate
    sessionDataStep,
    finalStep,         // LEFT JOIN to item_list_data for items column
];
```

### Ecommerce event list

A shared constant in `helpers/ga4Transforms.js` listing all official GA4 ecommerce events that carry item data:

```javascript
const ga4EcommerceEvents = [
    'view_item_list', 'select_item',
    'view_promotion', 'select_promotion',
    'view_item', 'add_to_wishlist',
    'add_to_cart', 'remove_from_cart',
    'view_cart', 'begin_checkout',
    'add_shipping_info', 'add_payment_info',
    'purchase', 'refund',
];
```

The `item_list_data` CTE uses this list but **excludes `refund`** — refunds typically happen with a long delay, making item list attribution unreliable for them.

### New helper function

Add `itemListAttribution()` to `helpers/ga4Transforms.js` — generates the window function SQL snippet for a given field, lookback type, and lookback time. This follows the existing pattern where `ga4Transforms.js` contains ecommerce helpers (`fixEcommerceStruct`, etc.).

```javascript
/**
 * Generates a SQL expression that attributes item_list_name or item_list_index
 * from select_item events to downstream events using a lookback window.
 *
 * @param {string} field - 'item_list_name' or 'item_list_index'
 * @param {'SESSION'|'TIME'} lookbackType
 * @param {number} [lookbackTimeMs] - Required when lookbackType is 'TIME'
 * @returns {string} SQL expression
 */
const itemListAttributionExpr = (field, lookbackType, lookbackTimeMs) => { ... };
```

## Files to Modify

| File | Change |
|------|--------|
| `tables/ga4EventsEnhanced/config.js` | Add `itemListAttribution: undefined` default |
| `tables/ga4EventsEnhanced/validation.js` | Add validation rules for `itemListAttribution` object |
| `tables/ga4EventsEnhanced/index.js` | Conditionally build `item_list_data` CTE, insert into steps, adjust final join |
| `helpers/ga4Transforms.js` | Add `itemListAttributionExpr()` helper function |
| `helpers/index.js` | No change needed — already re-exports all of `ga4Transforms.js` |
| `tests/inputValidation.test.js` | Validation tests for the new config object |
| `tests/ga4EventsEnhanced.test.js` | SQL generation tests (CTE present/absent, correct window function) |
| `README.md` | Document the new config option |

## Examples

### Example 1: Feature disabled (default)

**Config:**
```javascript
itemListAttribution: undefined
```

**Result:** No `item_list_data` CTE in SQL. `items` passes through unchanged. Identical to current behavior.

### Example 2: Session-based attribution

**Config:**
```javascript
itemListAttribution: {
    lookbackType: 'SESSION',
}
```

**Result:** SQL includes `item_list_data` CTE with `partition by session_id, item.item_id` and `rows between unbounded preceding and current row`.

### Example 3: Time-based attribution with bufferDays adjustment

**Config:**
```javascript
itemListAttribution: {
    lookbackType: 'TIME',
    lookbackTimeMs: 172800000, // 48h
}
bufferDays: 1
```

**Result:** SQL includes `item_list_data` CTE with `partition by user_pseudo_id, item.item_id` and `range between 172800000000 preceding and current row`. `bufferDays` effectively becomes 2 for the `event_data` date filter.

## Success Criteria

- [ ] `itemListAttribution: undefined` produces identical SQL to current output
- [ ] Session-based lookback generates correct `LAST_VALUE` window with session partition
- [ ] Time-based lookback generates correct `LAST_VALUE` window with microsecond range
- [ ] `bufferDays` auto-adjusts when time-based lookback exceeds it
- [ ] Item struct uses `REPLACE` syntax — no hardcoded field list
- [ ] All existing tests pass (`npm test`)
- [ ] README documents the new config option
- [ ] Validation rejects invalid configurations

## Testing Strategy

**Unit tests (`tests/inputValidation.test.js`):**
- `itemListAttribution: undefined` passes (disabled)
- `itemListAttribution: {}` fails (missing `lookbackType`)
- `itemListAttribution: { lookbackType: 'SESSION' }` passes
- `itemListAttribution: { lookbackType: 'TIME', lookbackTimeMs: 86400000 }` passes
- `itemListAttribution: { lookbackType: 'TIME' }` fails (missing `lookbackTimeMs`)
- `itemListAttribution: { lookbackType: 'SESSION', lookbackTimeMs: 86400000 }` fails (extra field)
- `itemListAttribution: { lookbackType: 'invalid' }` fails

**SQL generation tests (`tests/ga4EventsEnhanced.test.js`):**
- Disabled: no `item_list_data` in SQL
- Session: CTE present with correct window function
- Time: CTE present with correct microsecond range
- Time with low bufferDays: verify effective bufferDays is adjusted

```bash
npm run test:validation   # config validation tests
npm run test:events       # SQL generation tests
npm test                  # full suite
```

## Non-Goals

- **Item-level output table**: This feature attributes items within the existing event-level table. A separate item-level ecommerce table is out of scope.
- **Custom attribution events**: Only `view_item_list` and `select_item` are used as attribution sources. Custom event support is deferred.
- **Attribution for fields other than `item_list_name`/`item_list_id`/`item_list_index`**: Other item fields (e.g., `item_brand`) are not attributed.

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| `SELECT AS STRUCT item.* REPLACE(...)` may not work with `array_agg` on unnested arrays | High | Validate in BigQuery before implementation; fall back to explicit field list if needed |
| Performance: window function over large datasets is expensive | Medium | Feature is opt-in and off by default; document performance impact |
| `array_agg(SELECT AS STRUCT ... REPLACE)` syntax may not work in BigQuery | Medium | Validate syntax before implementation; fall back to two-CTE approach if needed |

## References

- [GA4 BigQuery Ecommerce Table (tanelytics.com)](https://tanelytics.com/ga4-bigquery-ecommerce-table/) — attribution SQL approach
- `tables/ga4EventsEnhanced/index.js` — step pipeline
- `helpers/ga4Transforms.js` — existing ecommerce helpers

---

**Document created**: 2026-04-12
**Last updated**: 2026-04-12
