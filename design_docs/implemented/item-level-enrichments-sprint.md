# Sprint Plan: Item-Level Data Enrichments (Sprint B)

## Summary

Ships Phase 2 of the data-enrichments feature per [data-enrichments.md](data-enrichments.md): the item-level slice. Extends `utils.buildEnrichments` to a nested return shape that routes event-level and item-level entries through separate output channels; removes the `level: 'item'` "not yet supported" throw; adds item-level joinKey validation and (for event-data joinKey columns only) dynamic `items_unnested` extension; routes item-level enrichment columns through the `preItemExpressions` spread mechanic established in Sprint B1. With this sprint, item-level enrichments are first-class — users can declaratively join product master / SKU / category-mapping data into the `items` struct via the same `enrichments` config API used for event-level data.

**Note on items_unnested:** Sprint B1 flattened the item struct in `items_unnested` — every standard items-struct field from `helpers.ga4ItemStructFields` is selected as a top-level column. So item-struct joinKey values (`item_id`, `item_category`, etc.) are already top-level identifiers in `items_unnested`; no dynamic extension is needed for them. Only event_data joinKey columns (like `user_pseudo_id`) need to be added dynamically to `items_unnested.select.columns`.

**Duration:** Single session (~1.5 hours)
**Dependencies:** Sprint B1 ([items-rebuilt-explicit-struct-sprint.md](items-rebuilt-explicit-struct-sprint.md)) ships first — establishes the `preItemExpressions` mechanic and `helpers.ga4ItemStructFields` that this sprint extends.
**Risk Level:** Medium — coupled changes across `utils.buildEnrichments` (return-shape extension), `tables/ga4EventsEnhanced/index.js` (call-site routing for both levels), and `items_unnested` (conditional window + dynamic column extension).
**Design doc:** [enrichment-cte-generation-item-level.md](enrichment-cte-generation-item-level.md) — all four design questions RESOLVED.

## Current Status Analysis

### Recent comparable sprints

| Sprint | LOC | Sessions |
|---|---|---|
| `event-level-enrichments` (Sprint A) | +1249 / −16 | 1 |
| `enrichment-cte-generation` (utility extraction) | +204 / −58 | 1 |
| `enrichment-coalesce-on-overlap` | +91 / −54 | 1 |

This sprint sits between Sprint A and the smaller refactor sprints. Most of the design surface is already specified by [data-enrichments.md](data-enrichments.md); the implementation is two extensions (utility return shape + call-site item routing) on top of the patterns already shipped.

### Velocity

- **Estimated capacity:** ~180 LOC (+50 utility extension / +60 call-site / +70 tests). Single-session.

## Proposed Milestones

Two milestones with M1 → M2 dependency. M1 extends the utility and adapts the existing event-level call site to the new return shape (no new behavior). M2 adds item-level routing at the call site.

### Milestone 1: M1_BUILD_ENRICHMENTS_LEVEL_ROUTING

**Goal:** Extend `utils.buildEnrichments` to return the nested `{ steps, event: {joins, columns, columnNames}, item: {joins, columns, columnNames}, columnOwner }` shape. Drop the `level: 'item'` throw. Adapt the existing event-level call site to consume `.event.*` instead of the flat returns. Add unit tests. After M1, event-level enrichments work identically (under the renamed `.event` channel) and item-level enrichment entries route through `.item.*` without yet being consumed downstream.

**Estimated:** ~50 LOC utility + ~30 LOC call-site adaptation + ~50 LOC unit tests = ~130 LOC
**Duration:** ~45 minutes

**Tasks:**

1. **Refactor `utils.buildEnrichments`** in [utils.js](../../utils.js):
   - Replace the flat-return shape with the nested shape.
   - Route entries by `e.level` (`'event'` default, `'item'` opt-in).
   - Drop the `level: 'item'` throw — item entries go through the `.item.*` channel.
   - `event.columns` and `item.columns` are both `{ col: 'enrich_<name>.<col>' }` maps.
   - `columnOwner` entries gain a `level` field (`'event' | 'item'`).
   - Enrichment-vs-enrichment collision throw fires only within a level (same-level only); cross-level same-name is allowed per Sprint B2 design Q1.

2. **Adapt the existing event-level call site** in [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js):
   - Destructure as `const { steps, event: eventEnrichments, item: itemEnrichments } = utils.buildEnrichments(mergedConfig.enrichments);`
   - Rename `enrichmentColumns` / `enrichmentColumnNames` references → `eventEnrichments.columns` / `eventEnrichments.columnNames`.
   - The coalesce post-processing for event-level overlap behavior stays unchanged in mechanic; it now reads from `eventEnrichments.columns`.
   - `itemEnrichments` is destructured but not yet consumed (no item-level routing at the call site until M2).

3. **Update existing utils tests** in [tests/utils.test.js](../../tests/utils.test.js) — the existing event-only test cases assert against the flat shape; update them to expect `.event.*` outputs.

4. **Add new unit tests** for item-level routing:
   - Single item-level enrichment produces a `steps` entry and an `.item.joins` + `.item.columns` entry.
   - Mixed event + item enrichments route to their respective channels.
   - Same-level item-vs-item column collision throws.
   - Cross-level same-name (one event-level `cohort`, one item-level `cohort`) does NOT throw and produces distinct `.event` / `.item` outputs.

**Acceptance criteria:**
- [ ] `utils.buildEnrichments` returns the nested shape; no `level: 'item'` throw anywhere.
- [ ] All existing tests pass (event-level enrichment behavior identical, just consuming under `.event.*`).
- [ ] ~6 new unit tests for level routing pass.
- [ ] Linting clean.
- [ ] SQL byte-equivalence for representative event-level-only configs (default, with `eventParamsToColumns`, with enrichments) against pre-refactor baselines — no behavior change for users with no item-level enrichments configured.

**Risks:**

- **Existing event-level call site renames touch many lines but are mechanical.** Test failures during the rename surface immediately. Mitigation: keep the rename a single commit; existing test coverage validates.

### Milestone 2: M2_ITEM_LEVEL_ROUTING_AT_CALL_SITE

**Goal:** Wire up item-level routing in `tables/ga4EventsEnhanced/index.js`. Adds item-level joinKey validation, dynamic `items_unnested` extension, conditional `LAST_VALUE` window, and integration of item-level enrichment columns into the `items_rebuilt` struct construction via the `preItemExpressions` spread.

**Estimated:** ~60 LOC implementation + ~70 LOC tests + small README updates = ~150 LOC
**Duration:** ~1 hour
**Dependencies:** M1_BUILD_ENRICHMENTS_LEVEL_ROUTING + Sprint B1 (items-rebuilt-explicit-struct, must already be merged)

**Tasks:**

1. **Item-CTE activation guard** in [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js): change from `if (itemListAttribution)` to `if (itemListAttribution || itemEnrichments.joins.length > 0)`.

2. **Conditional `LAST_VALUE` window in `items_unnested`** — when `itemListAttribution` is configured, emit the window function and the `_item_list_attr` struct; when only item enrichments are active, skip both.

3. **Item-level joinKey validation** (Layer 2; lives at the call site per design doc Q3):
   - For each item-level enrichment's joinKey column `c`:
     - If `c` is in `helpers.ga4ItemStructFields`: valid. Already a top-level column in `items_unnested` after Sprint B1's flatten — nothing to add.
     - Else if `c` is in `eventDataStep.select.columns` (with non-undefined value): valid. Add `<c>: '<c>'` to `items_unnested.select.columns` (skip if already present) so it can be used in `USING(...)`.
     - Else: throw with a clear error message identifying the enrichment, the invalid joinKey, and the two valid sources (items-struct fields + event_data columns).

4. **Item-level joins in `items_rebuilt`** — append `itemEnrichments.joins` to `items_rebuilt.joins`.

5. **Integrate item-level enrichment columns into the `items_rebuilt` struct** via the `preItemExpressions` spread:
   - Wrap overlapping enrichment columns in `coalesce(<enrichExpr>, <originalExpr>)`.
   - Spread the result over `preItemExpressions`. Overlapping entries overwrite; additive entries are appended.
   - Render as `array_agg(struct(<expr> as col, ...))`.

6. **New end-to-end tests** in [tests/enrichments.test.js](../../tests/enrichments.test.js):
   - Item-level enrichment with `joinKey: 'item_id'` and additive column: column appears as an additive struct entry; JOIN uses `using(item_id)` against `items_unnested.item_id` (top-level after B1 flatten).
   - Item-level enrichment overlapping a standard field (`item_category`): emits `coalesce(<expr>, item_category) as item_category` in the struct (the original `item_category` is a top-level column on `items_unnested`).
   - Item-level enrichment with `joinKey: ['event_date', 'item_id']` composite: `items_unnested` exposes both keys; JOIN uses `using(event_date, item_id)`.
   - Item-level enrichment with `joinKey: 'user_pseudo_id'` (event_data column): `items_unnested` exposes `user_pseudo_id`; JOIN uses `using(user_pseudo_id)`.
   - Item-level joinKey that's neither in event_data nor `ga4ItemStructFields`: throws with a clear message.
   - Combined with `itemListAttribution`: both feeders' overrides coexist in the struct.
   - Item enrichment only, no `itemListAttribution`: `items_unnested` body omits the `LAST_VALUE` window.

7. **README updates** in [README.md](../../README.md): add an item-level enrichment worked example to the data-enrichments section. Brief note in the field-row table about `level: 'item'` being supported.

8. **Drop the "not yet supported" language** anywhere it lingers in design docs / validation messages (the throw is gone, but any prose still saying "item-level deferred to Sprint B" should be updated).

**Acceptance criteria:**
- [ ] Item-level enrichments compile and produce the expected `items` struct in `enhanced_events`.
- [ ] All ~8 new end-to-end tests pass.
- [ ] All existing tests pass (event-level enrichment, item-list-attribution, eventDataColumns, etc.).
- [ ] Item-level joinKey validation throws with a clear error for invalid keys.
- [ ] `items_unnested` conditional window: `LAST_VALUE` only emitted when `itemListAttribution` is configured.
- [ ] BigQuery dry-run on at least one representative item-level enrichment config (product master joined on `item_id`) compiles cleanly.
- [ ] README updated with an item-level enrichment worked example.
- [ ] Linting clean.

**Risks:**

- **`items_unnested` body changes both for the conditional window AND the dynamic joinKey column extension.** Two interacting changes in the same CTE — tests must cover the matrix (attribution + enrichment, attribution-only, enrichment-only, neither).
- **Item-struct fields referenced via `item.<col>` in `items_unnested.select.columns`.** Verify BigQuery accepts this projection syntax inside the SELECT scope of `event_data, unnest(items) as item`. The existing `'item': 'item'` entry proves the unnest alias is in scope; per-field references should work the same.
- **`item_params` projection** — passed through unchanged as `item.item_params as item_params` in the rebuilt struct (from Sprint B1's explicit listing). No new code path; just verify it still works after item-level enrichments are layered on.

## Success Metrics

- All existing tests pass.
- ~14 new tests total across M1 (utility unit tests) and M2 (end-to-end item-level cases).
- Item-level enrichments compile and produce expected `items` struct output.
- README documents item-level enrichments with a worked example.
- Documentation: this sprint plan + [enrichment-cte-generation-item-level.md](enrichment-cte-generation-item-level.md) move from `planned/` to `implemented/` after merge. [data-enrichments.md](data-enrichments.md) stays in `planned/` only if outstanding "Future Work" items remain; otherwise it also moves.

## Dependencies

Sprint B1 ([items-rebuilt-explicit-struct-sprint.md](items-rebuilt-explicit-struct-sprint.md)) must ship first. This sprint depends on the `preItemExpressions` mechanic being in place inside `items_rebuilt`'s construction.

## Open Questions

None. Design doc resolved Q1 (cross-level collision: independent), Q2 (item-level auto-descriptions: deferred), Q3 (joinKey validation rule + items_unnested extension), Q4 (struct(...) constructor) before sprint planning began.

## Notes

- After this sprint ships, all design questions in [data-enrichments.md](data-enrichments.md) (Q1–Q19) have implementations in place. The feature is complete.
- Cross-level same-name allowed: explicitly tested in M1. README example should call this out so users naming an item-level column the same as an event-level one is a deliberate choice.
- Item-level auto-descriptions deferred (design doc Q2): if BigQuery later exposes struct-field descriptions through Dataform's column-description mechanism, a follow-up sprint can add them.

---

**Document created**: 2026-05-12
