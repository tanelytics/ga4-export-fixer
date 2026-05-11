# Event Data Explicit Column Listing

**Status**: Planned (Draft)
**Target**: v0.9.x (post-Sprint-A)
**Priority**: P2 (Low–Medium) — refactor / tech debt
**Estimated**: ~half a session
**Dependencies**: Sprint A ([event-level-enrichments-sprint](event-level-enrichments-sprint.md)) shipped in `0.9.0-dev.2`

## Context

Sprint A added `enrichments` and surfaced an asymmetry in how `event_data` and `session_data` expose their columns to downstream wildcards in `enhanced_events`:

- `event_data.*` expands to **explicit transformed columns + a wildcard pass-through** of unmodified GA4 export columns. Its column set is not statically knowable from the step config alone — it depends on `helpers/ga4Transforms.isGa4ExportColumn` and `mergedConfig.excludedColumns`.
- `session_data.*` expands to **only its explicit columns**. Its column set is `Object.keys(sessionDataStep.select.columns)` — fully knowable.

The replace-or-add semantics for enrichments must respect this asymmetry. The fix shipped in commit `0642088` filters `enrichmentExcludedColumns` per wildcard with two different predicates, encoding the asymmetry inline:

```js
const eventDataEnrichmentExcept = enrichmentExcludedColumns.filter(c =>
    eventDataExplicit.has(c) || (helpers.isGa4ExportColumn(c) && !userExcluded.has(c))
);
const sessionDataEnrichmentExcept = enrichmentExcludedColumns.filter(c =>
    sessionDataExplicit.has(c)
);
```

The two predicates encode different mental models. Future column-aware features (validation against `enhanced_events` columns, column-description automation, schema introspection) will face the same asymmetry and either re-derive the same logic or work around it.

## Problem Statement

The wildcard pass-through inside `event_data` is structurally invisible: the column set is derived at SQL-render time from `[sql]other_columns` getter at [tables/ga4EventsEnhanced/index.js:241-244](../../tables/ga4EventsEnhanced/index.js#L241-L244), and from there it depends on `helpers.isGa4ExportColumn` plus the runtime contents of `mergedConfig.excludedColumns`. Anything that wants to reason about `event_data`'s column set has to reconstruct it.

Side effects of the implicit pass-through:
- The enrichment EXCEPT filter has to encode an asymmetric predicate (current state).
- New GA4 export columns flow through silently, bypassing the AGENTS.md column-description discipline (`columnDescriptions.json`, `columnLineage.json`, `columnTypicalUse.json` may not have entries for currently passed-through columns).
- Tests cannot assert the full column list of `enhanced_events` without running a real query.
- Custom-schema source tables silently emit non-GA4 columns into the package's output, which the package then has no way to describe or validate.

## Goals

Make every column emitted by `event_data` knowable from `Object.keys(eventDataStep.select.columns)` alone. After the change, the enrichment EXCEPT filter collapses to one symmetric form for both wildcards, and any future column-aware feature can treat the two CTEs identically.

**Success criteria:**

- `eventDataStep.select.columns` enumerates every GA4 export column the package will emit, either as a transform expression or as a literal pass-through (e.g., `device: 'device'`).
- The `[sql]other_columns` getter is removed.
- The enrichment EXCEPT filter shrinks to a single uniform check per wildcard and no longer depends on `helpers.isGa4ExportColumn` or `mergedConfig.excludedColumns`.
- All existing tests pass without modification.
- Generated SQL diff against a representative config: same columns in, same columns out (no behavior change for standard GA4 export schemas).

## The mechanical change

### Current shape ([tables/ga4EventsEnhanced/index.js:201-250](../../tables/ga4EventsEnhanced/index.js#L201-L250))

```js
const eventDataStep = {
    name: 'event_data',
    select: {
        columns: {
            ...getExcludedColumns(),     // { app_info: undefined, publisher: undefined, ... }
            event_date: helpers.eventDate,
            /* ... other transforms ... */
            get '[sql]other_columns'() {
                const definedColumns = Object.keys(this);
                return `* except (${definedColumns.filter(c => helpers.isGa4ExportColumn(c)).join(', ')})`;
            },
        },
    },
    from: mergedConfig.sourceTable,
    where: '...',
};
```

### Target shape

```js
const eventDataExplicitColumns = {
    ...getExcludedColumns(),
    event_date: helpers.eventDate,
    /* ... all currently explicit transforms unchanged ... */
};
// Pass-through every GA4 export column not already explicit (or excluded via undefined).
const eventDataPassThroughs = {};
for (const c of helpers.ga4ExportColumns) {
    if (!(c in eventDataExplicitColumns)) {
        eventDataPassThroughs[c] = c;
    }
}
const eventDataStep = {
    name: 'event_data',
    select: { columns: { ...eventDataExplicitColumns, ...eventDataPassThroughs } },
    from: mergedConfig.sourceTable,
    where: '...',
};
```

`getExcludedColumns()` already maps user-excluded names to `undefined`. The `(c in eventDataExplicitColumns)` check sees the key exists (regardless of value), so the pass-through builder skips it — preserving today's `excludedColumns` behavior.

### Enrichment EXCEPT filter collapses

```js
const eventDataExplicit = new Set(Object.keys(eventDataStep.select.columns));
const sessionDataExplicit = new Set(Object.keys(sessionDataStep.select.columns));
const eventDataEnrichmentExcept = enrichmentExcludedColumns.filter(c => eventDataExplicit.has(c));
const sessionDataEnrichmentExcept = enrichmentExcludedColumns.filter(c => sessionDataExplicit.has(c));
```

`isGa4ExportColumn` and the `userExcluded` set are no longer needed on this code path.

## Resolved Questions

### Q1. How to expose the GA4 export column list (RESOLVED)

**Resolution:** Export the `ga4ExportColumns` array directly from [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js), as a sibling of the existing `isGa4ExportColumn` predicate.

**Rationale.** The array is the underlying data; the predicate is a derived check. Exporting the array lets callers iterate it (needed for the pass-through builder) without losing the convenience of the predicate. `isGa4ExportColumn` continues to read from the same array internally, so there is one source of truth.

**Implications:** One-line change to `module.exports` in `helpers/ga4Transforms.js`. No risk of drift between the array and the predicate.

### Q2. queryBuilder behavior for `undefined`-valued keys in `select.columns` (RESOLVED)

**Resolution:** No change to queryBuilder behavior is needed. `undefined`-valued keys are dropped from the rendered SELECT regardless of whether a `[sql]other_columns` getter is present. The pass-through builder's `(c in eventDataExplicitColumns)` check sees the key exists for user-excluded columns and skips them; the queryBuilder then drops the `undefined` entries from the final SELECT exactly as it does today.

**Verification:** The Q3 of the verification section (generated SQL diff on a config with `excludedColumns: ['app_info', 'publisher']`) confirms this end-to-end.

### Q3. Column description audit (RESOLVED)

**Resolution:** No audit needed. All currently-passed-through GA4 columns already have entries in `tables/ga4EventsEnhanced/columns/columnDescriptions.json`, `columnLineage.json`, and `columnTypicalUse.json` (previously verified). The refactor does not change the emitted column set, so the existing entries remain correct.

## Files affected

| File | Change | Size |
|---|---|---|
| [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js) | Export `ga4ExportColumns` array | 1 line |
| [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js) | Replace `[sql]other_columns` getter with pass-through builder; collapse enrichment EXCEPT filter | ~30 line refactor |
| [tests/](../../tests/) | New: snapshot/structural test asserting `event_data` column list matches `helpers.ga4ExportColumns ∪ explicit transforms` | ~20 line test |

`utils.js` `selectOtherColumns` is not touched. Existing tests pass without modification.

## Risk

Low.

The GA4 BigQuery export schema is standard — users do not introduce custom top-level columns (Tag Manager customizations land inside `event_params`, not as top-level columns). Behavior for the user-facing column set is byte-identical before and after the refactor.

The one ongoing maintenance consideration: Google adds new top-level columns to the GA4 export schema occasionally. The package already tracks this — the `ga4ExportColumns` array in [helpers/ga4Transforms.js:108-139](../../helpers/ga4Transforms.js#L108-L139) carries a `list updated YYYY-MM-DD` comment, and existing discipline keeps it in sync alongside the three `tables/ga4EventsEnhanced/columns/*.json` files (per [AGENTS.md](../../AGENTS.md) "Column descriptions"). Before this refactor, a new GA4 column would flow through silently and silently lack a description; after the refactor it would simply not appear in the output until added to the array. The latter is the safer failure mode — explicit and surfaced by tests, not silent.

## Verification

1. `npm run test:summary` — all existing tests pass without modification (no behavior change for standard configs).
2. New structural test: assert that `Object.keys(eventDataStep.select.columns)` (after filtering `undefined` values) equals the expected union of explicit transforms and pass-throughs.
3. Generated SQL diff on three representative configs:
   - Default config (no `excludedColumns`, no `eventParamsToColumns`)
   - User-excluded GA4 columns (`excludedColumns: ['app_info', 'publisher']`)
   - Promoted event params (`eventParamsToColumns: [{name: 'page_title', type: 'string'}]`)
   In all three cases, the emitted column set should be byte-identical to today's output (modulo column ordering, which the queryBuilder controls).
4. BigQuery dry-run on a real GA4 export with a representative config — confirm no schema mismatch.
5. Re-run the Sprint A bug-repro config (purely additive enrichment column `user_segment_test`) — still produces valid SQL with the simpler symmetric filter.
6. Voice-grep [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js) for any remaining reference to `isGa4ExportColumn` in the enrichment EXCEPT path — expect zero (the predicate is no longer needed there; `helpers.ga4ExportColumns` is used instead in the pass-through builder).

## References

- [helpers/ga4Transforms.js:106-141](../../helpers/ga4Transforms.js#L106-L141) — `isGa4ExportColumn` and the `ga4ExportColumns` array
- [tables/ga4EventsEnhanced/index.js:191-198](../../tables/ga4EventsEnhanced/index.js#L191-L198) — `getExcludedColumns()` (preserved as-is)
- [tables/ga4EventsEnhanced/index.js:241-244](../../tables/ga4EventsEnhanced/index.js#L241-L244) — `[sql]other_columns` getter (to be removed)
- [tables/ga4EventsEnhanced/index.js:328-402](../../tables/ga4EventsEnhanced/index.js#L328-L402) — enrichment block (EXCEPT filter to be collapsed)
- [AGENTS.md](../../AGENTS.md) "Column descriptions" — three-JSON-file discipline that the audit must respect
- Commit `0642088` — the asymmetric-predicate fix this refactor supersedes

---

**Document created**: 2026-05-08
**Last updated**: 2026-05-08
