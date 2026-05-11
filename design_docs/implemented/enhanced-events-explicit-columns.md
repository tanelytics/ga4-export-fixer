# Enhanced Events Explicit Column Listing

**Status**: Planned (Draft)
**Target**: v0.9.x (follow-up to enrichment-cte-generation)
**Priority**: P2 (Low–Medium) — refactor / consistency
**Estimated**: ~half a session
**Dependencies**: [event-data-explicit-columns](../implemented/event-data-explicit-columns.md) shipped through `0.9.0-dev.6` (introduced `utils.buildPassThroughs`); [enrichment-cte-generation](../implemented/enrichment-cte-generation.md) shipped (introduced `utils.buildEnrichments`, returned `enrichmentColumnNames` as a clean utility output).

## Context

After [event-data-explicit-columns](../implemented/event-data-explicit-columns.md), the `event_data` CTE no longer uses a `* except (...)` wildcard — its column set is fully knowable from `Object.keys(eventDataStep.select.columns)`. The same is true of `session_data` (it never had a wildcard).

But the outer `enhanced_events` SELECT still uses two wildcard pass-throughs:

```js
'[sql]event_data': utils.selectOtherColumns(eventDataStep, ..., [...exclusions]),
'[sql]session_data': utils.selectOtherColumns(sessionDataStep, ..., sessionDataEnrichmentExcept),
```

The wildcards are no longer load-bearing — both source CTEs have known column sets — they're just terse. And they're the only remaining reason for the asymmetric overlap filter at [tables/ga4EventsEnhanced/index.js:329-336](../../tables/ga4EventsEnhanced/index.js#L329-L336):

```js
const eventDataExplicit = new Set(Object.keys(eventDataStep.select.columns));
const sessionDataExplicit = new Set(Object.keys(sessionDataStep.select.columns));
const eventDataEnrichmentExcept = enrichmentExcludedColumns.filter(c => eventDataExplicit.has(c));
const sessionDataEnrichmentExcept = enrichmentExcludedColumns.filter(c => sessionDataExplicit.has(c));
```

That filter exists because `selectOtherColumns`'s `EXCEPT` clause rejects non-existent column names — additive enrichment columns must not be passed through. Replacing the wildcards with explicit qualified pass-throughs makes the filter unnecessary: a `buildQualifiedPassThroughs(step, alreadyCovered)` iterates the step's known columns, so unknown column names in `alreadyCovered` are naturally ignored.

This refactor is the final piece of the multi-step push toward "every column emitted by the package is statically knowable" — started by `event-data-explicit-columns`, continued by `buildPassThroughs` and `buildEnrichments`, finished here.

## Problem Statement

Two structural costs remain in the final SELECT:

1. **Asymmetric mechanics.** `event_data.*` and `session_data.*` are wildcards, but every other column in the final SELECT comes from an explicit spread (`finalColumnOrder`, `itemListOverrides`, `enrichmentColumns`, fixed tail). Hybrid wildcard+explicit is harder to reason about than uniform explicit.

2. **The overlap filter exists only because of those wildcards.** The 4-line filter at [tables/ga4EventsEnhanced/index.js:329-336](../../tables/ga4EventsEnhanced/index.js#L329-L336) is paying tax for the wildcard form: it filters `enrichmentColumnNames` against each step's column set to avoid feeding non-existent names to `selectOtherColumns`'s `EXCEPT`. With explicit pass-throughs the filter is moot — `buildQualifiedPassThroughs` iterates the step's columns directly, so names that aren't there are silently skipped.

Generated-SQL transparency is a secondary win: an explicit final SELECT lets a user grep for "where does column X come from" without expanding wildcards mentally.

## Goals

Replace the two `utils.selectOtherColumns` calls in `enhanced_events.select.columns` with a new `utils.buildQualifiedPassThroughs(step, alreadyCovered)` utility that returns `{ <col>: '<step.name>.<col>' }` for each column in the step not already in `alreadyCovered`. Drop the overlap-filter block; pass `enrichmentColumnNames` directly to both call sites (each utility call naturally skips columns not in its step). After this refactor:

- The final `enhanced_events` SELECT is fully explicit — no wildcards anywhere in the package's generated SQL.
- `utils.selectOtherColumns` is deleted from `utils.js` (zero callers; pre-1.0 housekeeping per Q2).
- The 4-line overlap filter disappears entirely.

**Success criteria:**

- `enhanced_events.select.columns` contains no `[sql]event_data` or `[sql]session_data` wildcard keys.
- Final SELECT has one explicit entry per emitted column.
- All existing tests pass without modification.
- Generated SQL produces the same column set as before, in the same final order. Wildcard-expansion ordering inside `event_data.*` / `session_data.*` is replaced by deterministic spread order.
- The Sprint A bug-repro config (purely additive `user_segment_test`) continues to produce valid SQL.

## Proposed shape

### Utility signature

```js
/**
 * Builds a qualified pass-through fragment for spreading into a downstream SELECT's
 * `select.columns`. For each column in `step.select.columns` not already in `alreadyCovered`,
 * emits an entry of the form `{ <col>: '<step.name>.<col>' }`.
 *
 * Columns whose values in `step.select.columns` are `undefined` (the user-exclusion sentinel
 * shape from getExcludedColumns) are skipped. Names in `alreadyCovered` that don't exist in
 * `step.select.columns` are silently ignored — the loop only iterates `step.select.columns`,
 * so unknown names cause no harm. This is the safety property that lets callers pass
 * "everything that might collide" (e.g. enrichmentColumnNames) without pre-filtering.
 *
 * @param {Object} step - A queryBuilder step with a `name` and `select.columns` object.
 * @param {Iterable<string>} alreadyCovered - Column names already mapped elsewhere in the
 *   downstream SELECT (from finalColumnOrder, itemListOverrides, enrichmentColumns, etc.) —
 *   plus any internal-only columns the downstream SELECT shouldn't re-emit (entrances,
 *   session_params_prep, the item-list row id, the data_is_final / export_type tail).
 * @returns {Object} A map of `{ <col>: '<step.name>.<col>' }` entries.
 *
 * @example
 *   buildQualifiedPassThroughs(eventDataStep, ['event_date', 'session_id', 'entrances']);
 *   // → { event_name: 'event_data.event_name', user_pseudo_id: 'event_data.user_pseudo_id', ... }
 */
const buildQualifiedPassThroughs = (step, alreadyCovered) => {
    const covered = new Set(alreadyCovered);
    const passThroughs = {};
    for (const [col, expr] of Object.entries(step.select.columns)) {
        if (expr === undefined) continue;
        if (covered.has(col)) continue;
        passThroughs[col] = `${step.name}.${col}`;
    }
    return passThroughs;
};
```

### Call-site change

The current shape at [tables/ga4EventsEnhanced/index.js:340-374](../../tables/ga4EventsEnhanced/index.js#L340-L374) becomes:

```js
const alreadyMapped = [
    ...Object.keys(finalColumnOrder),
    ...Object.keys(itemListOverrides),
    ...Object.keys(enrichmentColumns),
    'entrances',
    mergedConfig.sessionParams.length > 0 ? 'session_params_prep' : undefined,
    'data_is_final',
    'export_type',
    ...itemListExcludedColumns,
];

const enhancedEventsStep = {
    name: 'enhanced_events',
    select: {
        columns: {
            ...finalColumnOrder,
            ...itemListOverrides,
            ...enrichmentColumns,
            ...utils.buildQualifiedPassThroughs(eventDataStep, alreadyMapped),
            ...utils.buildQualifiedPassThroughs(sessionDataStep, alreadyMapped),
            row_inserted_timestamp: 'current_timestamp()',
            data_is_final: 'data_is_final',
            export_type: 'export_type',
        },
    },
    /* ... unchanged ... */
};
```

The overlap-filter block at [tables/ga4EventsEnhanced/index.js:329-336](../../tables/ga4EventsEnhanced/index.js#L329-L336) is deleted in full. `enrichmentColumns` is already in `alreadyMapped` (via `Object.keys(enrichmentColumns)`), so enrichment-vs-step-column overlap is handled by the same mechanism as `finalColumnOrder`-vs-step-column overlap — no special case needed.

## Resolved Questions

### Q1. New utility vs. extend `buildPassThroughs` (RESOLVED — new utility)

**Resolution:** Add a sibling utility `buildQualifiedPassThroughs` rather than extending `buildPassThroughs` with a "qualify with step name" flag.

**Rationale.** The two utilities have different return shapes — `buildPassThroughs` returns `{ col: col }` (bare identifier for the CTE-internal `select`) and `buildQualifiedPassThroughs` returns `{ col: 'step.col' }` (qualified for the outer SELECT). A flag-controlled single utility would either branch its return shape (fragile) or always qualify (breaks the existing `event_data` call site). Two sibling utilities are clearer at the call site and at the JSDoc level.

### Q2. Retire `selectOtherColumns` (RESOLVED — remove)

**Resolution:** Delete `utils.selectOtherColumns` from `utils.js`, remove it from `module.exports`, and remove its tests. Do this in the same sprint, after the last caller is gone.

**Rationale.** The package is still pre-1.0 (`0.9.0-dev`), so trimming an internal helper is appropriate housekeeping. Once both `buildQualifiedPassThroughs` calls land, `selectOtherColumns` has zero callers; leaving it as dead code with its own tests creates maintenance noise and a misleading suggestion that wildcard-form output is part of the package's vocabulary. The two new utilities (`buildPassThroughs`, `buildQualifiedPassThroughs`) cover the same use cases with explicit output; any future caller that genuinely wants the wildcard form can either reintroduce a similar helper or compose it from `buildQualifiedPassThroughs`.

### Q3. Internal-only column list at the call site (RESOLVED — keep inline)

**Resolution:** The list of internal-only columns (`'entrances'`, conditionally `'session_params_prep'`, `'data_is_final'`, `'export_type'`, item-list row id) stays inline at the call site as part of `alreadyMapped`. Not a utility.

**Rationale.** This list is specific to `ga4EventsEnhanced`'s pipeline — what counts as "internal-only" depends on the table module's design (`entrances` is consumed by `session_data.landing_page`, `data_is_final` is re-listed in the fixed tail, etc.). Pushing it into a utility would either hard-code GA4 specifics or require passing the list in anyway. Inline is clearest.

### Q4. Ordering of pass-through columns in the final SELECT (RESOLVED — match today's behavior)

**Resolution:** event_data pass-throughs follow `enrichmentColumns`; session_data pass-throughs follow event_data. This matches today's wildcard expansion order. The columns within each step's pass-through block render in `Object.keys(step.select.columns)` declaration order — also matching today's behavior since the wildcard expansion preserves that order.

**Rationale.** Same column set, same final order — minimal behavioral diff for users reading generated SQL.

## Open Questions

None. All design points resolved before this doc was drafted.

## Files affected

| File | Change | Size |
|---|---|---|
| [utils.js](../../utils.js) | Add `buildQualifiedPassThroughs` utility + export (alphabetically after `buildPassThroughs`/`buildEnrichments`); delete `selectOtherColumns` and remove from `module.exports` (per Q2) | ~+30 / −40 LOC net −10 |
| [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js) | Delete the 4-line overlap filter; replace two `selectOtherColumns` calls with two `buildQualifiedPassThroughs` calls; introduce `alreadyMapped` helper variable | ~+15 / −15 LOC net 0 |
| [tests/utils.test.js](../../tests/utils.test.js) | New section `3. buildQualifiedPassThroughs` with ~7 unit tests; delete any existing `selectOtherColumns` test cases | ~+80 / −10 LOC |
| [tests/eventDataColumns.test.js](../../tests/eventDataColumns.test.js) | Optional: extend with one assertion that the final SELECT contains no wildcard | ~5 LOC |

`tests/enrichments.test.js` is not modified — its 30 cases provide end-to-end coverage and continue to pass against the new explicit-form SELECT.

**Audit step before deletion:** grep the codebase for `selectOtherColumns` to confirm zero callers remain after the call-site swap. The only expected occurrences after the refactor are in the JSDoc/exports being deleted from `utils.js`.

## Verification

1. **`npm run test:summary`** — all 487 existing tests pass without modification. New unit tests add ~7 cases for the new utility.
2. **Generated SQL diff.** Capture SQL for the 4 representative configs (default, with `excludedColumns`, with `eventParamsToColumns`, with enrichment) before the refactor, then again after. Two expected differences:
   - The `event_data.* except (...)` and `session_data.* except (...)` lines disappear.
   - Each is replaced by N explicit `event_data.<col> as <col>` and `session_data.<col> as <col>` lines.
   - **Column set is identical, in identical order.** Confirm via a column-name extractor.
3. **Sprint A bug-repro.** The purely-additive `user_segment_test` enrichment config still produces valid SQL: the column lands in SELECT exactly once, and no wildcard EXCEPT lists exist (none of them do anymore).
4. **BigQuery dry-run.** A real GA4 export config compiles cleanly and emits the same schema as today.

## Risk

Low. The change is structural but mechanical — replacing two wildcard expansions with explicit listings of the same columns. The column set is unchanged; only the SQL form differs.

Two subtle correctness properties to preserve:

- **`undefined`-valued explicit-columns entries** (the user-excluded sentinels from `getExcludedColumns()`) must be skipped by `buildQualifiedPassThroughs`. The proposed implementation handles this via `if (expr === undefined) continue;`.
- **`itemListExcludedColumns`** (currently `['_item_row_id']` when item-list-attribution is on) must remain in `alreadyMapped` so the row-id column doesn't leak into the final SELECT.

Both properties are covered by existing tests (item-list-attribution scenarios in `tests/enrichments.test.js` and elsewhere).

## Sequencing

This is the final refactor in the multi-step push toward fully-explicit generated SQL. Predecessors:

1. [event-data-explicit-columns](../implemented/event-data-explicit-columns.md) — made `event_data`'s column set knowable.
2. [enrichment-cte-generation](../implemented/enrichment-cte-generation.md) — extracted enrichment generation into a utility, surfacing `enrichmentColumnNames` as a clean output.
3. **This refactor** — consumes the invariants both predecessors established to remove the last wildcards from the final SELECT.

No further refactors planned in this direction.

## References

- [event-data-explicit-columns.md](../implemented/event-data-explicit-columns.md) — the predecessor that established explicit column listing as the package's direction.
- [enrichment-cte-generation.md](../implemented/enrichment-cte-generation.md) — surfaced `enrichmentColumnNames` as a `buildEnrichments` return field; the overlap filter being deleted here was kept inline at that point per Q2 of that doc, with the explicit note that it would disappear once this refactor lands.
- [utils.js → `buildPassThroughs`](../../utils.js) — the precedent: a sibling utility for the same "explicit pass-through" pattern, just for raw-source CTEs instead of qualified inter-CTE references.
- [utils.js → `selectOtherColumns`](../../utils.js) — the wildcard-form helper being deleted in this refactor (Q2).

---

**Document created**: 2026-05-11
