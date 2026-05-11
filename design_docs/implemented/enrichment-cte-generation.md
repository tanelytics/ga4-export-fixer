# Enrichment CTE Generation Utility

**Status**: Planned (Draft)
**Target**: v0.9.x (post-sprint follow-up)
**Priority**: P2 (Medium) — unlocks future modeled-table modules
**Estimated**: ~half a session
**Dependencies**: Sprint A ([event-level-enrichments-sprint](../implemented/event-level-enrichments-sprint.md)) shipped in `0.9.0-dev.2`; [event-data-explicit-columns](../implemented/event-data-explicit-columns.md) shipped in `0.9.0-dev.3` (introduced `utils.buildPassThroughs`, the template this doc follows for what counts as a reusable utility).

## Context

Sprint A added the `enrichments` config field on `ga4EventsEnhanced` ([design doc](data-enrichments.md)). The generation logic — source-CTE construction, LEFT JOIN compilation, column-name tracking, item-level deferral throw, enrichment-vs-enrichment collision throw — lives inline at [tables/ga4EventsEnhanced/index.js:323-381](../../tables/ga4EventsEnhanced/index.js#L323-L381) (~60 lines).

Two motivations to extract it:

1. **Future modeled-table modules need the same feature.** The package is on a trajectory of adding new table modules (each a sibling of `ga4EventsEnhanced` under `tables/<name>/`). Each will want declarative enrichments. Keeping the generation logic inline in one table module prevents that — extraction makes the feature usable by importing `utils.buildEnrichments(...)`.

2. **The pass-through utility (`utils.buildPassThroughs`) set the precedent.** The same logic is what M2 of [event-data-explicit-columns](../implemented/event-data-explicit-columns.md) demonstrated: pull a self-contained, table-agnostic mechanism into `utils.js`, document and test it independently. Enrichment generation has the same shape — pure config-to-data mapping with no GA4-specific knowledge.

## Problem Statement

The inline block at [tables/ga4EventsEnhanced/index.js:323-381](../../tables/ga4EventsEnhanced/index.js#L323-L381) does five distinct things in one loop:

1. Iterates `mergedConfig.enrichments`.
2. Throws on `level: 'item'` (Layer 2 item-level deferral per [data-enrichments.md](data-enrichments.md) Q15).
3. Builds a source CTE step `{ name: 'enrich_<name>', select: { columns: {...} }, from: e.source, qualify?: '...' }`.
4. Compiles a LEFT JOIN clause `{ type: 'left', table: cteName, on: 'using(<keys>)' }`.
5. Records the enrichment columns into a `columnName → 'enrich_<name>.<col>'` map for the downstream SELECT spread, and into a `Set` of column names for downstream wildcard suppression.
6. Throws on enrichment-vs-enrichment column collisions with a clear error naming both enrichments.

All five concerns are tightly coupled to the input (`enrichments` config) and produce a small set of output data structures. None of them reach into `ga4EventsEnhanced`-specific state (`eventDataStep`, `sessionDataStep`, `finalColumnOrder`, etc.) — the inputs are entirely self-contained.

A second concern, **downstream overlap filtering** ([tables/ga4EventsEnhanced/index.js:383-390](../../tables/ga4EventsEnhanced/index.js#L383-L390)), is also currently in the same area:

```js
const eventDataExplicit = new Set(Object.keys(eventDataStep.select.columns));
const sessionDataExplicit = new Set(Object.keys(sessionDataStep.select.columns));
const eventDataEnrichmentExcept = enrichmentExcludedColumns.filter(c => eventDataExplicit.has(c));
const sessionDataEnrichmentExcept = enrichmentExcludedColumns.filter(c => sessionDataExplicit.has(c));
```

This is **out of scope** for this refactor (see Q2 below).

## Goals

Extract the inline generation block into a reusable utility `utils.buildEnrichments(enrichments)` that returns the data structures downstream code needs. The utility must:

- Take `mergedConfig.enrichments` as input (or equivalent — an array of validated enrichment entries).
- Return all data downstream code needs, in one object: `{ steps, joins, columns, columnNames, columnOwner }`.
- Encapsulate the item-level deferral throw and the enrichment-vs-enrichment column-collision throw — both are generation-time concerns and produce errors the user must see.
- Be source-table-agnostic: no GA4-specific assumptions, no dependency on `eventDataStep`/`sessionDataStep`.
- Be unit-testable in isolation (no Dataform context, no SQL execution).
- Update the existing call site in `ga4EventsEnhanced` to consume the utility's outputs.

**Success criteria:**

- `tables/ga4EventsEnhanced/index.js` loses ~60 lines from the enrichment block; gains a 2-3 line `utils.buildEnrichments` call.
- Future table modules can import the utility and consume enrichments with one call.
- All existing `tests/enrichments.test.js` cases continue to pass against the new utility-backed path (existing tests already cover all the generation outcomes; no behavior change).
- A new `tests/utils.test.js` section (under the existing file added by the `buildPassThroughs` extraction) unit-tests `buildEnrichments` directly with synthetic inputs — no `ga4EventsEnhanced` invocation needed.
- SQL output is byte-identical to today's across the existing test configs (default, with enrichment, with multiple enrichments, with composite key, with dedupe).

## Proposed shape

### Utility signature

```js
/**
 * Builds the per-enrichment CTE definitions, JOIN clauses, and column-name mappings
 * for the declarative `enrichments` feature.
 *
 * Pure config-to-data mapping. No knowledge of downstream CTEs or specific table modules —
 * intended to be called by any table module that exposes an `enrichments` config field.
 *
 * Encapsulates two generation-time throws:
 *   - level: 'item' (not yet supported; deferred per data-enrichments.md Q15).
 *   - Enrichment-vs-enrichment column collisions (two enrichments targeting the same column).
 *
 * @param {Array<Object>} enrichments - Validated enrichment entries. Each entry has fields:
 *   { name, level, source, joinKey, columns, dedupe? } per data-enrichments.md Q8.
 * @returns {Object} A struct with five fields:
 *   - `steps` — array of queryBuilder source-CTE step definitions (one `enrich_<name>` per entry).
 *   - `joins` — array of LEFT JOIN clauses to attach downstream (one per entry).
 *   - `columns` — map of `{ <enrichmentColumn>: 'enrich_<name>.<col>' }` for spreading into a
 *     downstream SELECT's `select.columns`.
 *   - `columnNames` — Set of all enrichment column names (used by callers for overlap detection
 *     against downstream CTEs).
 *   - `columnOwner` — map of `{ <column>: { i, name } }` recording which enrichment owns each
 *     column; preserved for diagnostics (not currently used outside the utility).
 *
 * @throws {Error} If any entry has `level: 'item'` (with a pointer to data-enrichments.md).
 * @throws {Error} If two enrichments target the same column name (with both enrichment names).
 *
 * @example
 *   const { steps, joins, columns, columnNames } = utils.buildEnrichments(config.enrichments);
 *   const allSteps = [...steps, eventDataStep, sessionDataStep, enhancedEventsStep];
 *   enhancedEventsStep.joins.push(...joins);
 *   Object.assign(enhancedEventsStep.select.columns, columns);
 */
const buildEnrichments = (enrichments) => {
    const steps = [];
    const joins = [];
    const columns = {};
    const columnNames = new Set();
    const columnOwner = {};

    for (const [i, e] of (enrichments ?? []).entries()) {
        const level = e.level ?? 'event';
        if (level === 'item') {
            throw new Error(
                `config.enrichments[${i}] uses level: 'item', which is not yet supported in this version. ` +
                `Item-level enrichments will ship in a future release; see design_docs/planned/data-enrichments.md.`
            );
        }
        const joinKeys = Array.isArray(e.joinKey) ? e.joinKey : [e.joinKey];
        const cteName = `enrich_${e.name}`;

        const cteCols = {};
        for (const k of joinKeys) cteCols[k] = k;
        for (const c of e.columns) cteCols[c] = c;
        const sourceStep = { name: cteName, select: { columns: cteCols }, from: e.source };
        if (e.dedupe) {
            sourceStep.qualify = `row_number() over (partition by ${joinKeys.join(', ')}) = 1`;
        }
        steps.push(sourceStep);

        joins.push({ type: 'left', table: cteName, on: `using(${joinKeys.join(', ')})` });

        for (const c of e.columns) {
            if (columnNames.has(c)) {
                const owner = columnOwner[c];
                throw new Error(
                    `config.enrichments[${i}] (name: '${e.name}') and config.enrichments[${owner.i}] ` +
                    `(name: '${owner.name}') both target column '${c}'. ` +
                    `Two enrichments cannot write the same column; rename one in source SQL or pick a different name.`
                );
            }
            columns[c] = `${cteName}.${c}`;
            columnNames.add(c);
            columnOwner[c] = { i, name: e.name };
        }
    }

    return { steps, joins, columns, columnNames, columnOwner };
};
```

### Call-site change

[tables/ga4EventsEnhanced/index.js:323-381](../../tables/ga4EventsEnhanced/index.js#L323-L381) (the ~60-line for-loop and surrounding declarations) collapses to:

```js
const { steps: enrichmentSteps, joins: enrichmentJoins, columns: enrichmentColumns,
        columnNames: enrichmentColumnNames } = utils.buildEnrichments(mergedConfig.enrichments);
const enrichmentExcludedColumns = [...enrichmentColumnNames];
```

The downstream code that consumes `enrichmentSteps`, `enrichmentJoins`, `enrichmentColumns`, `enrichmentExcludedColumns` is unchanged. `columnOwner` is no longer used outside the utility; remove the local variable.

## Resolved Questions

### Q1. Utility output shape (RESOLVED — return-object with named fields)

**Resolution:** Return a single object `{ steps, joins, columns, columnNames, columnOwner }`. Caller destructures the fields it needs.

**Rationale.** All five outputs are produced by the same iteration over enrichments — splitting into multiple functions would require iterating twice or threading state. A single return object is the natural shape for "compute everything in one pass." `columnOwner` is kept in the return for diagnostic completeness even though current callers don't use it outside the throw — exposing it costs nothing and keeps the utility's outputs self-describing.

### Q2. Downstream overlap filtering — in or out (RESOLVED — out)

**Resolution:** The overlap filter at [tables/ga4EventsEnhanced/index.js:383-390](../../tables/ga4EventsEnhanced/index.js#L383-L390) (`eventDataEnrichmentExcept` / `sessionDataEnrichmentExcept`) stays at the call site as a 1-line filter per downstream CTE. It does NOT move into `buildEnrichments`.

**Rationale.** The utility is about *what enrichments look like*; the overlap filter is about *how enrichments interact with a specific table's downstream CTEs*. Coupling the two would force `buildEnrichments` to know about `eventDataStep` and `sessionDataStep`, which defeats the table-agnostic goal. A future modeled table that doesn't have wildcards (or has different downstream CTEs) shouldn't have to thread fake step objects through the utility.

The filter is also slated to disappear entirely once enhanced_events moves to explicit pass-throughs (the follow-up refactor — `buildQualifiedPassThroughs` will subsume the filter via its natural set-difference behavior). Giving it a dedicated utility now would create a doomed abstraction.

### Q3. Input shape — full mergedConfig or just the enrichments array (RESOLVED — just the array)

**Resolution:** The utility takes `enrichments` (an array) directly, not `mergedConfig`.

**Rationale.** No part of the generation logic needs any other config field. Taking the minimal input keeps the utility unit-testable with `[]`/synthetic arrays and makes the call site self-documenting (`utils.buildEnrichments(mergedConfig.enrichments)` is clearer than `utils.buildEnrichments(mergedConfig)`).

### Q4. Where to live in utils.js (RESOLVED — near `buildPassThroughs`)

**Resolution:** Place `buildEnrichments` after `buildPassThroughs` in [utils.js](../../utils.js), and add it to the `module.exports` block in alphabetical order with the existing exports.

**Rationale.** Same family of utilities — both extract reusable mechanisms out of table-module code. Co-locating helps a reader find them.

## Open Questions

None. All design points resolved before this doc was drafted.

## Files affected

| File | Change | Size |
|---|---|---|
| [utils.js](../../utils.js) | Add `buildEnrichments` utility + export | ~50 LOC (incl. JSDoc) |
| [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js) | Replace inline ~60-line enrichment block with a 2-3 line utility call | −60 LOC net |
| [tests/utils.test.js](../../tests/utils.test.js) | New `buildEnrichments` section with unit tests for: empty input, single enrichment, multiple enrichments, composite joinKey, dedupe, item-level deferral throw, collision throw | ~80 LOC |

`tests/enrichments.test.js` is not modified — it continues to test enrichment behavior end-to-end via `ga4EventsEnhanced.generateSql(...)`, which after the refactor exercises the same logic through `utils.buildEnrichments`.

## Critical files

- [utils.js](../../utils.js) — target file for the new utility. Existing conventions: camelCase verb-noun, JSDoc with `@param`/`@returns`/`@throws`/`@example`, `module.exports` near the bottom.
- [tables/ga4EventsEnhanced/index.js:323-381](../../tables/ga4EventsEnhanced/index.js#L323-L381) — the inline block being extracted.
- [tables/ga4EventsEnhanced/index.js:383-390](../../tables/ga4EventsEnhanced/index.js#L383-L390) — the overlap-filter block. Stays inline (Q2).
- [tests/utils.test.js](../../tests/utils.test.js) — existing utility-test file from the `buildPassThroughs` extraction; new section appends here.
- [tests/enrichments.test.js](../../tests/enrichments.test.js) — existing end-to-end coverage; unchanged.

## Verification

1. `npm run test:summary` — all existing tests pass without modification. The 30 cases in [tests/enrichments.test.js](../../tests/enrichments.test.js) exercise every generation outcome (source CTE rendering, dedupe wrap, composite joinKey, item-level deferral, collision throw, source-format handling, description generation) and serve as the integration baseline. New unit tests for `buildEnrichments` add ~8 cases.
2. **SQL byte-equivalence**: generate SQL for a representative config that exercises multiple enrichments (one Dataform-ref source, one backtick-FQN source, one with composite joinKey, one with dedupe). Diff against a pre-refactor baseline — expect zero changes.
3. **Throw-path coverage**: confirm both error messages are byte-identical to today's (the user-facing strings haven't changed; only the call stack location). Tests at [tests/enrichments.test.js:181-220](../../tests/enrichments.test.js#L181-L220) (item-level deferral) and [tests/enrichments.test.js:316-330](../../tests/enrichments.test.js#L316-L330) (collision) verify these.
4. **Independence test**: at least one unit test in `tests/utils.test.js` calls `buildEnrichments` with a synthetic `[{ name: 'foo', level: 'event', source: '\`p.d.t\`', joinKey: 'id', columns: ['x'] }]` config and asserts the return shape, without invoking `ga4EventsEnhanced`. Confirms the utility is genuinely standalone and reusable for future table modules.

## Risk

Trivially low. Pure refactor: same logic, same outputs, same error messages — relocated. Existing test coverage on [tests/enrichments.test.js](../../tests/enrichments.test.js) (30 cases) is comprehensive and exercises every code path through the extracted utility. Public API additive (one new export, no changes to existing ones). No version bump needed (internal cleanup).

## Sequencing

This refactor lands BEFORE the planned `enhanced_events` explicit-pass-through refactor. Order matters for two reasons:

1. **Higher value first.** This refactor unblocks future modeled-table modules; the pass-through refactor is internal polish for the existing module.
2. **Simpler diff for the follow-up.** After this lands, `ga4EventsEnhanced/index.js`'s enrichment area is a 2-3 line utility call. The pass-through refactor then operates on a leaner file, making its diff more focused.

The two are otherwise orthogonal — the data interface between them (`enrichmentColumns`, `enrichmentColumnNames`) is stable.

## References

- [event-level-enrichments-sprint.md](../implemented/event-level-enrichments-sprint.md) — Sprint A; introduced the inline enrichment block this doc extracts.
- [data-enrichments.md](data-enrichments.md) — original feature design for `enrichments`; defines the validated input shape (`{ name, level, source, joinKey, columns, dedupe? }`) consumed by `buildEnrichments`.
- [event-data-explicit-columns.md](../implemented/event-data-explicit-columns.md) — the just-shipped refactor that introduced `utils.buildPassThroughs`. Sets the precedent (and the test-file structure) for the utility being added here.
- [AGENTS.md](../../AGENTS.md) — house-voice conventions; column-description discipline (`buildEnrichments` does not change which columns are described — [documentation.js:172-220](../../documentation.js#L172-L220) still handles enrichment column auto-descriptions inline and is out of scope here).

---

**Document created**: 2026-05-11
