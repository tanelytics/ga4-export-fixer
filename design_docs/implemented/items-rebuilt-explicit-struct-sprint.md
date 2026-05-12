# Sprint Plan: Items-Rebuilt Explicit Struct Construction

## Summary

Refactor `items_rebuilt` from `array_agg(select as struct item.* replace(...))` to explicit `array_agg(struct(<expr> as col, ...))` seeded by a new `helpers.ga4ItemStructFields` constant. Eliminates the last wildcard from the package's generated SQL and establishes the `preItemExpressions` spread mechanic that Sprint B2 (item-level enrichments) consumes. Pure refactor — no new user-facing behavior.

**Duration:** Single session (~1 hour)
**Dependencies:** None active. Follows the same explicit-listing pattern shipped by `event-data-explicit-columns` and `enhanced-events-explicit-columns`.
**Risk Level:** Low-to-medium — items_rebuilt's emitted column SET and VALUES must be byte-identical to today's output for any existing `itemListAttribution` config.
**Design doc:** [enrichment-cte-generation-item-level.md](enrichment-cte-generation-item-level.md) — Sprint B2's design doc that calls out the items_rebuilt refactor as bundled work. Q17 in [data-enrichments.md](data-enrichments.md) covers the rendered SQL pattern.

## Current Status Analysis

### Recent comparable refactors

| Sprint | LOC | Sessions |
|---|---|---|
| `enhanced-events-explicit-columns` (M2: wildcard removal + utility extraction) | +35 / −96 | 1 |
| `event-data-explicit-columns` (M1: replace `* except (...)` with pass-through builder) | +168 / −81 | 1 |
| `enrichment-coalesce-on-overlap` (M1: preEnrichmentExpressions spread) | +83 / −46 | 1 |

This sprint sits at the same shape — drop a BigQuery wildcard, replace with explicit per-field listing seeded by a new exported constant. Smaller scope than the event-data refactor (one CTE vs. two paths).

### Velocity

- **Recent:** single-session refactors at ~100 LOC net consistently land in one session with full test coverage.
- **Estimated capacity:** ~80 LOC (+50 utility/refactor / +30 helper / minor test rewrites). Single-session.

## Proposed Milestones

Two milestones with M1 → M2 dependency. M1 adds the helper without touching the SQL generation; M2 consumes it to rewrite `items_rebuilt`.

### Milestone 1: M1_ADD_GA4_ITEM_STRUCT_FIELDS

**Goal:** Add `helpers.ga4ItemStructFields` array + `isGa4ItemStructField` predicate to [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js). Export both. Add unit tests. No SQL-generation changes — utility callable but unused.

**Estimated:** ~30 LOC helper + ~30 LOC tests = ~60 LOC
**Duration:** ~15 minutes

**Tasks:**

1. Add `ga4ItemStructFields` const in [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js), mirroring the `ga4ExportColumns` pattern. List in GA4 source order:

   ```
   item_id, item_name, item_brand, item_variant, item_category,
   item_category2, item_category3, item_category4, item_category5,
   price_in_usd, price, quantity,
   item_revenue_in_usd, item_revenue,
   item_refund_in_usd, item_refund,
   coupon, affiliation, location_id,
   item_list_id, item_list_name, item_list_index,
   promotion_id, promotion_name, creative_name, creative_slot,
   item_params
   ```

2. Add `isGa4ItemStructField` predicate (one-liner: `ga4ItemStructFields.includes(fieldName)`).
3. Export both from `module.exports`.
4. Add `// list updated YYYY-MM-DD` comment above the array.
5. Add unit tests in [tests/eventDataColumns.test.js](../../tests/eventDataColumns.test.js) (or a new `tests/ga4Helpers.test.js` if that's the cleaner home — see Q1 below):
   - `helpers.ga4ItemStructFields` is exported as a non-empty array
   - Contains expected fields (`item_id`, `item_revenue`, `item_params`, etc.)
   - `isGa4ItemStructField` returns true for known, false for unknown
   - Source order preserved (`item_id` first, `item_params` last)

**Acceptance criteria:**
- [ ] `helpers.ga4ItemStructFields` exported and importable.
- [ ] `helpers.isGa4ItemStructField` exported and behaves correctly.
- [ ] All ~4 new unit tests pass.
- [ ] All existing tests pass unchanged (no SQL generation touched).
- [ ] Linting clean.

**Risks:** none notable. Pure additive change.

### Milestone 2: M2_REWRITE_ITEMS_REBUILT_EXPLICIT

**Goal:** Replace `items_rebuilt`'s `array_agg(select as struct item.* replace(...))` with `array_agg(struct(<expr> as col, ...))` seeded by `helpers.ga4ItemStructFields`. Item-list-attribution's three field overrides become entries in a `preItemExpressions` map (parallel to `preEnrichmentExpressions` in `enhanced_events`). Verify SQL emits the same column set and values; only the surrounding syntax changes.

**Estimated:** ~50 LOC implementation + ~20 LOC of test rewrites/additions = ~70 LOC
**Duration:** ~45 minutes
**Dependencies:** M1_ADD_GA4_ITEM_STRUCT_FIELDS

**Tasks:**

1. **Build `preItemExpressions` at the `items_rebuilt` construction site** in [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js):

   ```js
   const preItemExpressions = {};
   for (const f of helpers.ga4ItemStructFields) {
       preItemExpressions[f] = `item.${f}`;
   }
   // Item-list-attribution overrides (when active).
   preItemExpressions.item_list_name = `coalesce(if(${passthroughEvents}, item.item_list_name, _item_list_attr.item_list_name), '(not set)')`;
   preItemExpressions.item_list_id = `coalesce(if(${passthroughEvents}, item.item_list_id, _item_list_attr.item_list_id), '(not set)')`;
   preItemExpressions.item_list_index = `coalesce(if(${passthroughEvents}, item.item_list_index, _item_list_attr.item_list_index))`;
   ```

2. **Rewrite the `items_rebuilt.select.columns.items` value** to render an explicit struct:

   ```js
   const itemStructFields = preItemExpressions;
   const fieldClauses = Object.entries(itemStructFields)
       .map(([col, expr]) => `${expr} as ${col}`)
       .join(',\n      ');
   const itemsExpression = `array_agg(struct(\n      ${fieldClauses}\n    ))`;
   ```

   Then `rebuiltStep.select.columns.items = itemsExpression`.

3. **Verify SQL byte-equivalence** for representative `itemListAttribution` configs (lookback by SESSION; lookback by TIME). Capture pre-refactor SQL baselines; regenerate after M2; assert the emitted items struct values are identical. The SQL form changes (from `select as struct item.* replace(...)` to `struct(...)`) but the rendered output is the same.

4. **Update any tests** that match the old wildcard form. Most existing tests probably check final emitted columns rather than SQL form, but some may grep for `select as struct` / `replace(`. Update to match `struct(`.

5. **Run BigQuery dry-run** if accessible for an `itemListAttribution`-on config to verify the explicit struct compiles and emits the expected schema.

**Acceptance criteria:**
- [ ] `items_rebuilt`'s items expression renders as `array_agg(struct(<expr> as col, ...))` with every entry from `helpers.ga4ItemStructFields`.
- [ ] `item.* replace(...)` does not appear anywhere in generated SQL — grep should return zero matches in any package output.
- [ ] All existing tests pass — `tests/enrichments.test.js` item-list-attribution coverage; `tests/eventDataColumns.test.js`; `tests/ga4EventsEnhanced.test.js` if it includes attribution-relevant integration cases.
- [ ] SQL byte-equivalence verified for at least two representative `itemListAttribution` configs (different lookback types).
- [ ] Linting clean.

**Risks:**

- **Field-order shift in the emitted struct schema.** BigQuery struct field order is determined by the SELECT/struct constructor. The current `select as struct item.* replace(...)` preserves GA4's source order. Sprint B1's explicit listing follows the order in `helpers.ga4ItemStructFields` — must match GA4's source order to keep downstream schemas stable. Mitigation: the helper's list IS in GA4 source order (see M1). Verification: SQL diff plus an INFORMATION_SCHEMA check on a test deployment to confirm field order.
- **`item_params` nested REPEATED RECORD.** `item.item_params as item_params` should project the whole REPEATED RECORD through unchanged. Verify via SQL dry-run that BigQuery accepts the explicit projection of a REPEATED struct field.
- **Hidden field references.** Some code path may rely on a specific items struct emitting a specific field set. If `helpers.ga4ItemStructFields` accidentally omits a field GA4 emits today, the refactor silently drops it. Mitigation: SQL byte-equivalence diff catches this on first run.

## Success Metrics

- All existing tests pass.
- New M1 unit tests pass (~4 new).
- Generated SQL diff for `itemListAttribution`-on configs: items struct values byte-identical, only the surrounding syntax changes.
- `helpers.ga4ItemStructFields` is callable and used.
- Zero `item.* replace(` in the package's generated SQL.

## Dependencies

None active. The `itemListAttribution` feature shipped long ago and is the only feeder into `items_rebuilt` today.

## Resolved Questions

### Q1. Where do the `helpers.ga4ItemStructFields` unit tests live? (RESOLVED)

**Resolution:** Extend [tests/eventDataColumns.test.js](../../tests/eventDataColumns.test.js). The existing `ga4ExportColumns` tests already live there; the two lists are siblings and co-locating their tests is clearer than creating a new file for a small addition. The file name is slightly misleading after the addition, but that's a minor nit — a future rename to `ga4SchemaConstants.test.js` is a one-line follow-up if it ever bothers anyone.

## Open Questions

None.

## Notes

- Pure refactor. After this sprint ships, the package emits no `* replace(...)` syntax anywhere in its generated SQL — completing the "no wildcards anywhere" property the recent explicit-listing refactors built toward.
- Sprint B2 ([item-level-enrichments-sprint.md](item-level-enrichments-sprint.md)) consumes the `preItemExpressions` mechanic established here. B2's diff is much smaller as a result.

---

**Document created**: 2026-05-12
