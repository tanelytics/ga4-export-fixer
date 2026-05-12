# Item-Level Enrichment CTE Generation

**Status**: Planned (Draft)
**Target**: v0.9.x (Sprint B of data-enrichments)
**Priority**: P1 — implements the structurally distinct half of the data-enrichments feature
**Estimated**: 1 session (~1.5 hours)
**Dependencies**: [enrichment-cte-generation](../implemented/enrichment-cte-generation.md) shipped (defines the event-level utility shape this doc extends); [event-data-explicit-columns](../implemented/event-data-explicit-columns.md), [enhanced-events-explicit-columns](../implemented/enhanced-events-explicit-columns.md), [enrichment-coalesce-on-overlap](../implemented/enrichment-coalesce-on-overlap-sprint.md) shipped (established coalesce-then-add patterns and explicit-column conventions Sprint B inherits).

## Context

Sprint A ([event-level-enrichments-sprint](event-level-enrichments-sprint.md), shipped in `0.9.0-dev.2`) shipped event-level enrichments via `utils.buildEnrichments` plus call-site post-processing for coalesce-then-add overlap behavior. Item-level enrichments (`level: 'item'`) currently throw "not yet supported" inside the utility — that throw is the Layer 2 deferral hatch documented in [data-enrichments.md](data-enrichments.md) Q15 and Q17. Sprint B implements them.

The feature design (when to use `items_unnested` / `items_rebuilt`, how the struct construction handles overlap, what `helpers.ga4ItemStructFields` enumerates) is in [data-enrichments.md](data-enrichments.md) Q14–Q19. **This doc is the refactor spec for the utility-level API change** — how `utils.buildEnrichments` routes item-level entries differently from event-level entries, and what the call site in `tables/ga4EventsEnhanced/index.js` does with the new outputs.

## Problem Statement

Today `utils.buildEnrichments(enrichments)` returns a flat shape suitable only for event-level enrichments:

```js
{ steps, joins, columns, columnNames, columnOwner }
```

`joins` are LEFT JOIN clauses attached to `enhanced_events`. `columns` are spread into the outer SELECT. The shape doesn't accommodate item-level enrichments, which route differently:

- **Source CTEs are the same.** `enrich_<name>` CTEs go at the top of the pipeline regardless of level.
- **JOINs land in a different CTE.** Event-level JOINs attach to `enhanced_events.joins`; item-level JOINs attach to `items_rebuilt.joins`.
- **Columns are not spread into `enhanced_events.select.columns`.** Item-level enrichment columns flow into the `items` struct constructed by `items_rebuilt`, alongside the standard GA4 item-struct fields and any item-list-attribution overrides.
- **Item-level enrichment columns must NOT propagate to `enhanced_events.select.columns`.** They live inside the `items` struct, accessed via `items[OFFSET(0)].my_enriched_field` in downstream SQL.

A flat return shape can't carry these two routing distinctions cleanly. The utility needs to separate event from item.

Today `items_rebuilt` also constructs its struct via a wildcard pattern (`array_agg(select as struct item.* replace(...))`). With `helpers.ga4ItemStructFields` becoming available for item-level enrichment classification anyway, the wildcard can be replaced with explicit `struct(...)` construction — same direction as the just-shipped `event-data-explicit-columns` and `enhanced-events-explicit-columns` refactors. Sprint B bundles this items-rebuilt refactor with the new item-level enrichment feature, since both touch the same code block and rely on the same standard-fields list.

## Goals

Extend `utils.buildEnrichments` to handle both levels with a nested return shape that makes routing explicit at the call site. Refactor `items_rebuilt` to explicit `struct(...)` construction using `helpers.ga4ItemStructFields` as the canonical field list. Keep the utility table-agnostic — no `ga4EventsEnhanced`-specific knowledge.

**Success criteria:**

- `utils.buildEnrichments` no longer throws on `level: 'item'`; it routes item-level entries through the `item.*` output channel.
- New helper export `helpers.ga4ItemStructFields` enumerates the GA4 standard item-struct fields.
- `items_rebuilt` constructs its struct via BigQuery's `struct(<expr> as col, ...)` constructor with every standard item field listed explicitly. No `array_agg(select as struct item.* replace(...))` wildcard remains in the package's generated SQL.
- The call site in `tables/ga4EventsEnhanced/index.js` consumes the nested outputs without conditionals — event-level outputs feed `enhanced_events`, item-level outputs feed `items_rebuilt` via a `preItemExpressions` spread mechanic that mirrors the `preEnrichmentExpressions` pattern from `enhanced-events-explicit-columns`.
- Existing event-level enrichment tests pass unchanged (the utility's event-channel output is identical in content; just nested under `.event`).
- Existing `itemListAttribution` tests pass unchanged after `items_rebuilt`'s rewrite (the SQL form changes but the emitted column set and values are identical).
- New unit tests in `tests/utils.test.js` cover the item-level paths and the level-routing logic.

## Proposed shape

### Utility return shape (nested)

```js
buildEnrichments(enrichments) → {
    steps: [/* source-CTE step definitions for both event and item enrichments */],
    event: {
        joins: [/* LEFT JOIN clauses to attach to enhanced_events */],
        columns: { [col]: 'enrich_<name>.<col>' },   // spread into enhanced_events.select.columns
        columnNames: Set<string>,                     // for caller's coalesce post-processing
    },
    item: {
        joins: [/* LEFT JOIN clauses to attach to items_rebuilt */],
        columns: { [col]: 'enrich_<name>.<col>' },   // spread into items_rebuilt struct (call site applies coalesce wrap)
        columnNames: Set<string>,
    },
    columnOwner: { [col]: { i: <index>, name: <enrichment name>, level: 'event'|'item' } },
}
```

Notes:

- `steps` stays a single flat array because all source CTEs land in the same place (top of the pipeline). Splitting by level would force the call site to merge them back.
- `event.columns` and `item.columns` are the same shape — a `{ col: 'enrich_<name>.<col>' }` map. The caller wraps overlap columns in `coalesce(<enrichExpr>, <originalExpr>)` per its level-specific original-expression source (event-level looks at `preEnrichmentExpressions`; item-level looks at `preItemExpressions`).
- No `isStandardField` flag on item columns. The classification (standard-field-overlap vs additive) happens at the call site implicitly via the spread mechanic — overlapping keys overwrite entries in `preItemExpressions`; non-overlapping keys are appended. No special syntax distinction needed because `items_rebuilt` constructs its struct explicitly.
- `columnOwner` gains a `level` field so collision diagnostics (when triggered) can clarify which level a column came from. Same-level collisions throw inside the utility (existing event-level mechanic). Cross-level same-name is NOT a collision — see Q1.

### Collision rules

Same-level enrichment-vs-enrichment column-collision throw as today: two event-level enrichments writing the same column → throw; two item-level → throw. Cross-level same-name is independent (Q1 resolution below).

### `helpers.ga4ItemStructFields` export

```js
// helpers/ga4Transforms.js
// list updated <date> — GA4 items-struct source order
const ga4ItemStructFields = [
    'item_id',
    'item_name',
    'item_brand',
    'item_variant',
    'item_category',
    'item_category2',
    'item_category3',
    'item_category4',
    'item_category5',
    'price_in_usd',
    'price',
    'quantity',
    'item_revenue_in_usd',
    'item_revenue',
    'item_refund_in_usd',
    'item_refund',
    'coupon',
    'affiliation',
    'location_id',
    'item_list_id',
    'item_list_name',
    'item_list_index',
    'promotion_id',
    'promotion_name',
    'creative_name',
    'creative_slot',
    'item_params',
];

const isGa4ItemStructField = (fieldName) => ga4ItemStructFields.includes(fieldName);

module.exports = {
    /* ... existing exports ... */
    ga4ItemStructFields,
    isGa4ItemStructField,
};
```

Mirror the `ga4ExportColumns` / `isGa4ExportColumn` pair shipped in [event-data-explicit-columns](../implemented/event-data-explicit-columns.md). Update cadence: a comment `// list updated YYYY-MM-DD` plus a minor package release when GA4 adds new standard fields. Same discipline as the existing column list.

The order matters — `items_rebuilt`'s explicit `struct(...)` construction emits the items-struct fields in the order they appear here, and consumers may reasonably depend on the items-struct schema field order matching GA4's own. `item_params` is a nested `REPEATED RECORD`; it projects through unchanged as a single struct entry, no per-key handling needed (the items-rebuilt refactor preserves its current pass-through behavior).

### Call-site changes (in `tables/ga4EventsEnhanced/index.js`)

After the utility call, destructure both channels:

```js
const {
    steps: enrichmentSteps,
    event: eventEnrichments,
    item: itemEnrichments,
} = utils.buildEnrichments(mergedConfig.enrichments);
```

Event-level routing is unchanged from today (the coalesce post-processing, the `alreadyMapped` build, the spread into `enhanced_events.select.columns`). The shape change is from `enrichmentColumns` → `eventEnrichments.columns` and from `enrichmentColumnNames` → `eventEnrichments.columnNames`. Mechanical rename.

Item-level routing is new:

1. **Activation:** the items-scaffold construction guard becomes `if (itemListAttribution || itemEnrichments.joins.length > 0)`.
2. **JoinKey validation + `items_unnested` column extension** (Layer 2; lives at the call site per Q3):
   - For each item-level enrichment's joinKey column `c`: classify it as an event_data column or an item-struct field per Q3's two-source rule.
   - Throw with a clear message if neither matches.
   - Extend `items_unnested.select.columns` with the joinKey columns so the downstream `USING(...)` clause has top-level identifiers to bind to: `<c>: '<c>'` for event_data columns, `<c>: 'item.<c>'` for item-struct fields. Skip if already present.
3. **`items_unnested` body:** conditional on `itemListAttribution` — emit the `LAST_VALUE` attribution window when configured; emit a plain `unnest(items)` when only item enrichments are active. Per Q16's activation rule.
4. **`items_rebuilt.joins`:** append `itemEnrichments.joins` to the existing scaffold's joins array.
5. **`items_rebuilt` explicit struct construction** — the items-rebuilt refactor bundled into this sprint. The mechanic mirrors how `enhanced_events` handles enrichments via `preEnrichmentExpressions`:

   ```js
   // Seed with the canonical item-struct fields.
   const preItemExpressions = {};
   for (const f of helpers.ga4ItemStructFields) {
     preItemExpressions[f] = `item.${f}`;
   }

   // Item-list-attribution overrides (when active).
   if (itemListAttribution) {
     preItemExpressions.item_list_name = `coalesce(if(${passthroughEvents}, item.item_list_name, _item_list_attr.item_list_name), '(not set)')`;
     preItemExpressions.item_list_id = `coalesce(if(${passthroughEvents}, item.item_list_id, _item_list_attr.item_list_id), '(not set)')`;
     preItemExpressions.item_list_index = `coalesce(if(${passthroughEvents}, item.item_list_index, _item_list_attr.item_list_index))`;
   }

   // Wrap overlapping enrichment columns in coalesce against the pre-existing expression.
   const wrappedItemEnrichmentColumns = {};
   for (const [col, enrichExpr] of Object.entries(itemEnrichments.columns)) {
     const originalExpr = preItemExpressions[col];
     wrappedItemEnrichmentColumns[col] = originalExpr
       ? `coalesce(${enrichExpr}, ${originalExpr})`
       : enrichExpr;
   }

   // Final struct: standard fields, then enrichment overrides spread on top.
   // Spread order means overlap entries overwrite preItemExpressions entries;
   // additive enrichment columns are appended as new keys.
   const itemStructFields = {
     ...preItemExpressions,
     ...wrappedItemEnrichmentColumns,
   };

   // Rendered SQL: `array_agg(struct(<expr1> as <col1>, <expr2> as <col2>, ...))`
   ```

6. **Items struct construction syntax: `struct(...)`, not `(select as struct ...)`.** With explicit field listing, there's no need for the `select as struct item.* replace(...)` pattern — BigQuery's `struct(<expr> as col, ...)` constructor is the simpler form and is what the package emits. One less level of nesting in the rendered SQL.

## Resolved Questions

### Q1. Cross-level same-name collision (RESOLVED)

**Resolution:** **Independent — not a collision.** An event-level enrichment writing `cohort` and an item-level enrichment also writing `cohort` are allowed to coexist. They target structurally distinct output slots — `enhanced_events.cohort` vs `items[].cohort` (a field inside the items struct) — and there's no real ambiguity in the rendered SQL.

**Implications:**

- The enrichment-vs-enrichment collision check still fires *within* a single level (two event-level enrichments writing the same column → throw; two item-level → throw).
- Cross-level collision is detected at the column-name level but not raised as an error. `columnOwner` carries the `level` field to keep the two ownership entries distinguishable for diagnostics.
- Documentation note: README and the data-enrichments worked examples should call this out so users naming an item-level column the same as an event-level one is a deliberate choice, not an accident.

### Q2. Auto-descriptions for item-level enrichment columns (RESOLVED)

**Resolution:** **Defer.** Sprint B does not generate auto-descriptions for item-level enrichment columns. BigQuery doesn't have a clean way to attach per-field descriptions to STRUCT-array fields through Dataform's column-description mechanism, and faking it via prose on the parent `items` column adds rendering complexity for marginal value. Revisit if BigQuery exposes struct-field descriptions in a future release.

**Implications:**

- The `documentation.js` extension shipped in Sprint A (auto-description generation for enrichment columns) checks `level === 'event'` before generating; item-level columns are skipped. This guard already exists in [documentation.js:172-220](../../documentation.js#L172-L220) from Sprint A and stays untouched.
- The README data-enrichments section should note that item-level enrichment columns don't receive auto-generated descriptions.

### Q3. Item-level joinKey validation (RESOLVED)

**Resolution:** Both `event_data` columns and item-struct fields are valid joinKey values for item-level enrichments. The package validates each joinKey column against two sources and dynamically extends `items_unnested.select.columns` with the columns it needs to carry up so the JOIN inside `items_rebuilt` can `USING(<keys>)` on top-level identifiers.

**Validation rule** (Layer 2, at the call site in `tables/ga4EventsEnhanced/index.js`):

For each item-level enrichment's joinKey column `c`:

1. If `c` is a key in `eventDataStep.select.columns` with a non-undefined value → valid. The package adds `<c>: '<c>'` to `items_unnested.select.columns` (event_data is in the unnest's FROM, so `c` resolves directly).
2. Else if `c` is in `helpers.ga4ItemStructFields` → valid. The package adds `<c>: 'item.<c>'` to `items_unnested.select.columns` (the item-struct field is exposed at top level via the `item` unnest alias).
3. Else → throw with a clear message identifying the invalid joinKey, the enrichment name, and the two valid sources.

The joinKey columns are added to `items_unnested.select.columns` IF NOT ALREADY PRESENT (first writer wins). This keeps the SELECT minimal — `event_date` and `_item_row_id` are already there from the existing `items_unnested` shape, so they don't get duplicated.

**Where validation lives:** at the call site in `tables/ga4EventsEnhanced/index.js`, not inside `utils.buildEnrichments`. Rationale: the rule combines knowledge of two GA4-specific sources (`eventDataStep.select.columns` shape + `helpers.ga4ItemStructFields`). Keeping the utility table-agnostic preserves its reusability for future modeled tables that may have different valid joinKey sources. The utility throws only on per-level collisions (which are level-internal, not source-dependent).

**Implications for the utility shape:** `utils.buildEnrichments` does NOT need an `eventDataStep` or `itemStructFields` parameter. It produces the per-enrichment `item.joins` clauses, but the dynamic extension of `items_unnested.select.columns` happens at the call site.

## Files affected

| File | Change | Size |
|---|---|---|
| [utils.js](../../utils.js) | Extend `buildEnrichments`: route entries by `level`, return nested `{ steps, event, item, columnOwner }` shape, drop the `level: 'item'` throw. `item.columns` is a `{ col: 'expr' }` map (same shape as `event.columns`); no `isStandardField` classification — the call site handles overlap via spread. | ~+50 / −20 |
| [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js) | Add `ga4ItemStructFields` array + `isGa4ItemStructField` predicate; export both | ~+30 |
| [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js) | Destructure new utility return shape (rename `enrichmentColumns` → `eventEnrichments.columns`, etc.); change item-CTE activation guard; conditionally emit `LAST_VALUE` window in `items_unnested`; **rewrite `items_rebuilt` from `array_agg(select as struct item.* replace(...))` to explicit `array_agg(struct(<expr> as col, ...))` seeded by `preItemExpressions`**; fold item-level joins + enrichment columns into the same spread mechanic. | ~+60 / −30 |
| [tests/utils.test.js](../../tests/utils.test.js) | Section `4. buildEnrichments item-level routing` with cases for: single item-level enrichment (additive), single (overlap), mixed item+event, cross-level same-name (allowed per Q1), composite joinKey, same-level item-vs-item collision throws | ~+100 |
| [tests/enrichments.test.js](../../tests/enrichments.test.js) | New end-to-end cases: item-level additive column appears as additive struct field; item-level overlap with standard field emits `coalesce(<expr>, item.<col>) as <col>` inside `struct(...)`; combined with `itemListAttribution`; scaffold conditional window (no attribution); existing `itemListAttribution` tests continue to pass against the new `struct(...)` form. | ~+80 |
| [tests/eventDataColumns.test.js](../../tests/eventDataColumns.test.js) | None (item-level enrichments don't propagate to event_data) | 0 |

## Verification

1. `npm run test:summary` — expect all existing tests to pass unchanged (the event-level path's behavior and SQL output is identical). New unit + end-to-end tests cover the item-level mechanics.
2. **SQL byte-equivalence** for event-level-only configs against pre-refactor baselines — confirm Sprint A's output is byte-identical after the utility return-shape change (renames only; no behavior shift).
3. **BigQuery dry-run** for representative item-level configs:
   - One item-level enrichment, additive column (`margin_bucket`), no `itemListAttribution`.
   - One item-level enrichment, overlap with `item_category`, no `itemListAttribution`. Verify `replace(coalesce(enrich_X.item_category, item.item_category) as item_category)` in the struct construction.
   - Mixed: `itemListAttribution` ON + one item-level enrichment overlapping `item_list_name` (rare but legal). Verify the combined `replace(...)` block carries both the attribution clauses and the enrichment coalesce clauses without duplicate-column errors.
   - Composite joinKey `['event_date', 'item_id']` — verify `items_unnested.event_date` exists (it does per `0.9.0-dev.0`) and the JOIN compiles to `using(event_date, item_id)`.
4. **README + table-description updates** — extend the data-enrichments section with item-level examples; add the Sprint B item-level use case to the worked examples.

## Risk

Medium.

Two coupled changes ship together: the `utils.buildEnrichments` extension (mechanical) and the `items_rebuilt` rewrite from `select as struct item.* replace(...)` to explicit `struct(<expr> as col, ...)`. Both are well-specified by [data-enrichments.md](data-enrichments.md) Q14–Q19 and parallel patterns already shipped (`event-data-explicit-columns`, `enhanced-events-explicit-columns`). Existing tests pin the behavior of both feeders — the event-level enrichment surface (under the renamed `.event` channel) and `itemListAttribution` (against the new `struct(...)` form).

**Notable risks:**

- **`items_rebuilt` byte-equivalence against the existing `itemListAttribution` SQL.** The rewrite is a pure refactor for users who only configure attribution. The struct's emitted field set and values must be identical to today's output (column SET unchanged; column ORDER controlled by the `struct(<expr> as col, ...)` field order). Verification: SQL diff against pre-refactor baselines for representative `itemListAttribution`-only configs. If field order shifts, the consuming SQL probably doesn't care (struct fields are accessed by name), but downstream schemas might.
- **`helpers.ga4ItemStructFields` schema drift.** Becomes load-bearing — every field listed is what `items_rebuilt` emits, and unlisted fields are dropped. The list must stay current with the GA4 item-struct schema. Same trade-off as `helpers.ga4ExportColumns`; the existing `// list updated YYYY-MM-DD` discipline carries over.
- **Conditional `LAST_VALUE` window in `items_unnested`.** Adding the conditional to support item-enrichment-only requires care to not break the existing `itemListAttribution` path. Tests at [tests/enrichments.test.js](../../tests/enrichments.test.js) and existing item-list-attribution coverage catch regressions.
- **`struct(...)` constructor inside `array_agg`.** BigQuery accepts `array_agg(struct(<expr> as col, ...))` — verify in a dry-run during M1 before relying on it across all configs.

## Sequencing

This is the second-to-last piece of the data-enrichments feature. After Sprint B lands, the only outstanding work in [data-enrichments.md](data-enrichments.md) is its "Future Work" section (out of scope here).

A sprint plan will follow once this design doc is approved.

## References

- [data-enrichments.md](data-enrichments.md) — feature design (Q14–Q19 cover the item-level half).
- [enrichment-cte-generation.md](../implemented/enrichment-cte-generation.md) — Sprint A utility extraction; this doc extends what that doc shipped.
- [enrichment-coalesce-on-overlap-sprint.md](../implemented/enrichment-coalesce-on-overlap-sprint.md) — coalesce-then-add behavior pattern that Sprint B's item-level overlap inherits via `replace(coalesce(...))`.
- [event-data-explicit-columns.md](../implemented/event-data-explicit-columns.md) — precedent for the `helpers.ga4ExportColumns` exported array; `ga4ItemStructFields` mirrors that pattern.
- [items-cte-prep-sprint.md](items-cte-prep-sprint.md) — shipped `event_date` propagation through `items_unnested` that enables composite item-level joinKeys.

---

**Document created**: 2026-05-11
