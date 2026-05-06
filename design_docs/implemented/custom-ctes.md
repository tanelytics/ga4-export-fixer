# Custom CTEs via Configuration

**Status**: Planned
**Target**: v0.8.0 (alongside [query-builder-v2](query-builder-v2.md))
**Priority**: P1 (Medium) — depends on v2 step shape; ships in same release
**Estimated**: 0.5 days
**Dependencies**: [query-builder-v2.md](query-builder-v2.md) — landed in `0.8.0-dev.0`

## Context

This is the second piece of the three-part initiative outlined in the [query-builder-v2 design doc](query-builder-v2.md):

1. **Query Builder v2** ✅ landed `0.8.0-dev.0` — generalized step configuration so each top-level key maps to a SQL clause, plus a raw `{name, query}` shape for whole-CTE bodies.
2. **Custom CTEs via configuration** (this doc) — let users append their own steps to the pipeline through the package config JSON.
3. **Data enrichments** (future) — opinionated wrappers around (2) for the typical cases (item-level, session-level, page-level).

The v2 raw shape (`{name, query}`) was specifically designed to be the surface for this feature — users hand the package SQL bodies, the package wraps them as CTEs and appends them to the steps array.

## Problem Statement

The package today produces a fixed pipeline: `event_data → [item_list_attribution → item_list_data] → session_data → final`. Users who need pipeline-level transformations beyond what the package config exposes have only two awkward escape hatches:

1. **Wrap the package output in another Dataform model.** Adds a model file, an extra materialization, and breaks the "one config builds one table" mental model.
2. **Fork the package.** Heavy maintenance burden; loses upstream improvements.

There's no way to interleave a custom CTE between (e.g.) `session_data` and the final output, or to define a derived table that joins package CTEs with custom logic, without leaving the package's surface.

**Concrete use cases users have hit:**

- UTM-source attribution computed from `session_data` and joined back into the final output.
- Cohort labels computed from a pre-existing dimension table joined onto session data.
- Custom funnel/event-sequence flags derived from `event_data` before the session aggregation runs.

The first case is a common-enough pattern that it's the prototypical example for this feature — and it's the foundation for the **data enrichments** feature (item 3 of the initiative).

## Goals

**Primary goal:** Let users declaratively append CTEs to the package's pipeline via the package config, so the package still produces "one config = one table" but with user-defined intermediate steps included.

**Success criteria:**

- A user can add one or more custom CTEs (raw SQL or v2-structured) via a single config field.
- Custom CTEs can reference any of the package's named CTEs (`event_data`, `session_data`, `item_list_attribution`, `item_list_data`, plus the renamed-final CTE — see Q1).
- The last custom CTE becomes the final SELECT (the actual output of the table). No custom CTEs ⇒ same behavior as before, just under a new internal name (Q1).
- Validation rejects name collisions with reserved CTE names at config time, with a clear error message.
- All existing tests pass; new test cases cover empty / one structured / one raw / multiple / collision configurations.

## Proposed Configuration

```js
// packageConfig.js
{
    sourceTable: ctx.ref('analytics_123', 'events_*'),
    // ...existing config...
    customSteps: [
        // Raw shape — user writes the whole CTE body
        {
            name: 'utm_attribution',
            query: `select
  session_id,
  array_agg(struct(utm_source, utm_medium) order by event_timestamp limit 1)[safe_offset(0)] as first_utm
from event_data, unnest(event_params) p
where p.key in ('utm_source', 'utm_medium')
group by session_id`,
        },
        // Structured shape — same as v2 step shape
        {
            name: 'final',
            select: {
                columns: { '[sql]passthrough': 'enhanced_events.*' },
                sql: 'utm_attribution.first_utm as first_utm',
            },
            from: 'enhanced_events',
            joins: [{ type: 'left', table: 'utm_attribution', on: 'using(session_id)' }],
        },
    ],
}
```

**Rendering rules:**

- `customSteps` is appended to the package's existing `steps` array, after the renamed-final step.
- v2 `queryBuilder` already handles the rest: each entry except the last becomes a CTE, the last is the outer SELECT.
- Each custom step is a v2 step object (raw or structured), validated by v2 `queryBuilder`'s existing validation.
- Names available to reference from custom CTEs: `event_data`, `session_data`, `item_list_attribution`, `item_list_data` (when item-list-attribution is enabled), and the renamed-final CTE (Q1).

## Design Decisions

All design questions are resolved. Each entry below records the resolution and the reasoning trail.

### Q1. Renamed final step (RESOLVED)

**Resolution:** rename `final` → `enhanced_events`.

The current `finalStep` is named `final` ([tables/ga4EventsEnhanced/index.js:319](tables/ga4EventsEnhanced/index.js#L319) on the v0.8.0-dev.0 branch). With custom steps appended after it, "final" is no longer accurate. `enhanced_events` matches the table name (`ga4_events_enhanced`), reads naturally in joins (`from enhanced_events`), and is descriptive of the contents that custom CTEs will join against.

**Backward-compatibility note:** in v1 the CTE was named `final`. Anyone reading the generated SQL externally (rare but possible) would see the rename. v0.8.0 is already a breaking-change release, so bundling this rename costs nothing extra.

### Q2. Config field name (RESOLVED)

**Resolution:** `customSteps`.

Consistent with v2 `queryBuilder`'s "step" terminology — matches the internal model exactly. The slight tradeoff (most steps become CTEs, but the last is the final SELECT) is documented in the field's description rather than encoded in the name. The internal/external terminology stays aligned, which makes errors and docs easier to read.

### Q3. Name collision handling (RESOLVED)

**Resolution:** throw on collision. The reserved-names list is **derived at runtime from the package's actual steps array** — not hard-coded.

User-supplied step names could collide with package-internal CTE names. Any collision is rejected at SQL generation time with a clear error naming the offender and listing the active reserved names.

**Why derive instead of hard-code:** the reserved set isn't static. `item_list_attribution` and `item_list_data` only exist when `itemListAttribution` is configured. Hard-coding the list creates two problems:

1. **Drift risk.** If a future change adds, removes, or renames an internal CTE, the hard-coded list silently lags behind reality. Users could be blocked from a name that's no longer reserved, or allowed to collide with a name that became reserved.
2. **Conditional reservations are brittle.** Even today, the active reserved set depends on config (`itemListAttribution`). A hard-coded list that always includes `item_list_data` would falsely reserve it when item-list-attribution is off.

**Implementation pattern.** Compute the reserved set from the actual `packageSteps` array immediately before custom steps are appended (inside `_generateEnhancedEventsSQL`). This makes the package steps the single source of truth.

```js
const packageSteps = [
    eventDataStep,
    ...(itemListSteps ?? []),
    sessionDataStep,
    enhancedEventsStep,
];

if (mergedConfig.customSteps?.length) {
    const reservedNames = new Set(packageSteps.map(s => s.name));
    for (const [i, step] of mergedConfig.customSteps.entries()) {
        if (reservedNames.has(step.name)) {
            throw new Error(
                `config.customSteps[${i}].name '${step.name}' collides with a reserved package CTE name. ` +
                `Reserved names (active for this config): ${[...reservedNames].join(', ')}. Choose a different name.`
            );
        }
    }
}

const steps = [...packageSteps, ...(mergedConfig.customSteps ?? [])];
```

**The reserved set for the default config** (no `itemListAttribution`):
- `event_data`
- `session_data`
- `enhanced_events`

**With `itemListAttribution`** the set additionally includes:
- `item_list_attribution`
- `item_list_data`

### Q4. Reserved CTE names as a stable contract (RESOLVED)

**Resolution:** yes. The package-internal CTE names that custom steps can reference are a documented stable contract under the v0.8.x line. Renames or removals bundle into a minor version bump (same way Q1's `final → enhanced_events` is bundled into v0.8.0).

**Stable names that custom steps can reference:**

| Name | Always present? | Contents |
|---|---|---|
| `event_data` | yes | The first CTE — extracted and shaped events from `sourceTable`, with date filtering and column promotions applied. |
| `session_data` | yes | Session-level aggregations (session_id grouping). |
| `item_list_attribution` | only when `itemListAttribution` is configured | Per-event item attribution rows. |
| `item_list_data` | only when `itemListAttribution` is configured | Re-aggregated items with attributed list fields. |
| `enhanced_events` | yes | The package's standard output shape (joined event_data + session_data + item_list_data, columns ordered, incremental date filter applied). The natural starting point for most custom CTEs. |

**What "stable contract" means here:**

- **Names won't be renamed** without a minor version bump and a migration note.
- **Column shape of each named CTE** is stable within v0.8.x. Adding columns is fine; removing or renaming columns is a minor-version-bump change.
- **Active subset** depends on config (Q3) — `item_list_*` only exist when item-list-attribution is on. Custom steps that reference them must be paired with the matching config.
- **Internal pre-`enhanced_events` CTEs** (`event_data`, `session_data`, `item_list_*`) are unfiltered for the buffer-days range — referencing them gives access to the buffer window. Useful for window functions; document this as part of the contract.

**Where this gets documented:** README + AGENTS.md sections on `customSteps`, plus inline JSDoc on the `customSteps` config field. The same locations users will look for the field's usage will surface the contract.

### Q5. Custom column documentation / assertions (RESOLVED)

**Resolution:** out of scope for v0.8.0.

Custom CTEs can change the output schema (add columns, rename, filter rows). The package's auto-generated column descriptions and assertions assume the standard schema. For v0.8.0:

- **User-added columns appear in the output table without auto-generated descriptions.** Users who want descriptions for their custom columns supply them via Dataform's standard `dataformTableConfig.columns` field — the existing override path that already works for any column.
- **Assertions assume the standard schema.** Users with custom CTEs that change the schema (rename columns, drop columns, filter rows in ways that break assertion expectations) should disable affected assertions explicitly via the existing assertions config.
- **Documentation responsibility shifts to the user** for any rows / columns / behaviors their custom CTEs introduce. The package guarantees coverage only for the columns it generates.

**What's deferred to future work:**

- A `customColumnDescriptions: { ... }` config field that takes user-defined column metadata and surfaces it in the table schema alongside the package-generated columns. Separable from the core feature; can ship in a later version once usage patterns from custom CTEs make the right shape clear.
- Custom assertions targeting user-added columns — same pattern, separable, not blocking v0.8.0.

### Q6. Pre-validation at the package boundary (RESOLVED)

**Resolution:** yes — pre-validate `customSteps` at the package boundary, with the split established in Q3:

- **Layer 1 (config shape) in [tables/ga4EventsEnhanced/validation.js](tables/ga4EventsEnhanced/validation.js):** `customSteps` must be an array (or undefined); each entry must be a non-null object with a non-empty `name`; names must be unique within `customSteps`. Step-shape validation (clause keys, required fields) is deferred to `queryBuilder`.
- **Layer 2 (collision with package step names) in [tables/ga4EventsEnhanced/index.js](tables/ga4EventsEnhanced/index.js):** the runtime-derived reserved-set check (Q3) — runs after package steps are built but before custom steps are appended.

**Reasoning:** catches the most common user errors at the package boundary with package-aware error messages (referencing `config.customSteps[i]`, listing reserved names) rather than at the lower-level `queryBuilder` boundary which only sees an opaque array of steps and doesn't know which names are reserved. Layer 1 fires at config-validation time, Layer 2 fires at SQL-generation time — both before the user sees any output, so error UX is the same in practice.

## Solution Design

### Architecture

The change is small and localized:

1. **Rename `finalStep` → `enhancedEventsStep`** in [tables/ga4EventsEnhanced/index.js](tables/ga4EventsEnhanced/index.js); set its `name` to `'enhanced_events'`.
2. **Append `mergedConfig.customSteps`** to the steps array.
3. **Add `customSteps` validation** in [tables/ga4EventsEnhanced/validation.js](tables/ga4EventsEnhanced/validation.js).
4. **Update `getFinalColumnOrder`** if it references the old name (it doesn't — it accesses `eventDataStep.select.columns` and `sessionDataStep.select.columns` only; the renamed step doesn't affect it).
5. **Default `customSteps: []`** in [tables/ga4EventsEnhanced/config.js](tables/ga4EventsEnhanced/config.js).

### Change in `_generateEnhancedEventsSQL`

**Before** (current `0.8.0-dev.0`):

```js
const finalStep = {
    name: 'final',
    select: { columns: { ... } },
    from: 'event_data',
    joins: [...],
    where: helpers.incrementalDateFilter(mergedConfig),
};

const steps = [
    eventDataStep,
    ...(itemListSteps ?? []),
    sessionDataStep,
    finalStep,
];
return utils.queryBuilder(steps);
```

**After:**

```js
const enhancedEventsStep = {
    name: 'enhanced_events',
    select: { columns: { ... } },
    from: 'event_data',
    joins: [...],
    where: helpers.incrementalDateFilter(mergedConfig),
};

const steps = [
    eventDataStep,
    ...(itemListSteps ?? []),
    sessionDataStep,
    enhancedEventsStep,
    ...(mergedConfig.customSteps ?? []),
];
return utils.queryBuilder(steps);
```

The pipeline is identical when `customSteps` is empty — `enhanced_events` becomes the final SELECT (no CTE wrapping, since v2 `queryBuilder` only wraps steps[0..N-2]). When customSteps is non-empty, `enhanced_events` becomes a CTE and the user's last step is the final SELECT.

### Validation — split between two layers

Validation is split across the two natural boundaries — config-shape concerns at config-validation time, package-aware concerns at SQL-generation time.

**Layer 1 — config shape, in [tables/ga4EventsEnhanced/validation.js](tables/ga4EventsEnhanced/validation.js):**

```js
// inside validateEnhancedEventsConfig:
if (config.customSteps !== undefined) {
    if (!Array.isArray(config.customSteps)) {
        throw new Error(`config.customSteps must be an array. Received: ${JSON.stringify(config.customSteps)}`);
    }
    const seenNames = new Set();
    config.customSteps.forEach((step, i) => {
        if (!step || typeof step !== 'object' || Array.isArray(step)) {
            throw new Error(`config.customSteps[${i}] must be a non-null object. Received: ${JSON.stringify(step)}`);
        }
        if (typeof step.name !== 'string' || !step.name.trim()) {
            throw new Error(`config.customSteps[${i}] must have a non-empty 'name'. Received: ${JSON.stringify(step.name)}`);
        }
        if (seenNames.has(step.name)) {
            throw new Error(`config.customSteps contains duplicate name '${step.name}'.`);
        }
        seenNames.add(step.name);
        // step-shape validation (clause keys, etc.) is deferred to queryBuilder
        // collision-with-package-names check is deferred to _generateEnhancedEventsSQL (Q3)
    });
}
```

**Layer 2 — collision with package step names, in [tables/ga4EventsEnhanced/index.js](tables/ga4EventsEnhanced/index.js)'s `_generateEnhancedEventsSQL`:**

See the implementation pattern in Q3 above — the reserved-names set is built from `packageSteps.map(s => s.name)` after the package's internal steps are constructed but before `customSteps` are appended.

**Why split:** the config-shape rules (array, objects, has-name, unique-within-customSteps) don't depend on what the package generates, so they belong in the config-validation pass. The collision rule needs to know the actual generated step names, which depend on config (e.g. `itemListAttribution`). Computing it from `packageSteps` makes the steps array the single source of truth (Q3) — no duplicate list to maintain.

### Default config entry

In [tables/ga4EventsEnhanced/config.js](tables/ga4EventsEnhanced/config.js), add `customSteps: []` to the default config so the merge rules don't drop the field.

## Files to Modify

| File | Change | Est. LOC |
|---|---|---|
| [tables/ga4EventsEnhanced/index.js](tables/ga4EventsEnhanced/index.js) | Rename `finalStep` → `enhancedEventsStep`, set `name: 'enhanced_events'`, append `customSteps` to the steps array, runtime-derive reserved names + collision check (Q3) | ~+15 / -3 |
| [tables/ga4EventsEnhanced/validation.js](tables/ga4EventsEnhanced/validation.js) | Add `customSteps` config-shape validation (array, objects, name presence, no-duplicates-within-customSteps) | ~+20 |
| [tables/ga4EventsEnhanced/config.js](tables/ga4EventsEnhanced/config.js) | Default `customSteps: []` | ~+1 |
| [tests/ga4EventsEnhanced.test.js](tests/ga4EventsEnhanced.test.js) | New cases: empty / one structured / one raw / multiple / collision / non-array | ~+80 |
| [tests/inputValidation.test.js](tests/inputValidation.test.js) | Validation error cases for `customSteps` | ~+40 |
| README, AGENTS.md, helpers/index.js docs | Document the `customSteps` config option and the reserved CTE names | ~+30 |

**Estimated total:** ~190 LOC (mostly tests and docs).

## Examples

### Use case: UTM-source attribution joined back into final

```js
ga4EventsEnhanced.createTable(publish, {
    sourceTable: ctx.ref('analytics_123', 'events_*'),
    customSteps: [
        {
            name: 'utm_first_touch',
            query: `select
  session_id,
  array_agg((select value.string_value from unnest(event_params) where key = 'utm_source')
    ignore nulls order by event_timestamp limit 1)[safe_offset(0)] as utm_source
from event_data
group by session_id`,
        },
        {
            name: 'final',
            select: {
                columns: {
                    '[sql]passthrough': 'enhanced_events.*',
                    utm_source: 'utm_first_touch.utm_source',
                },
            },
            from: 'enhanced_events',
            joins: [{ type: 'left', table: 'utm_first_touch', on: 'using(session_id)' }],
        },
    ],
});
```

### Use case: Cohort label from external dimension

```js
customSteps: [
    {
        name: 'final',
        select: {
            columns: {
                '[sql]passthrough': 'enhanced_events.*',
                cohort: 'user_cohorts.cohort',
            },
        },
        from: 'enhanced_events',
        joins: [
            { type: 'left', table: '`proj.ds.user_cohorts`', on: 'using(user_pseudo_id)' },
        ],
    },
],
```

A single custom step is sufficient for adding a column; no intermediate CTE is required.

### No custom steps (default)

```js
ga4EventsEnhanced.createTable(publish, {
    sourceTable: ctx.ref('analytics_123', 'events_*'),
    // customSteps not provided
});
```

Pipeline emits exactly as before, except the previously-named `final` CTE is now the un-wrapped outer SELECT under the name `enhanced_events`.

## Success Criteria

- [ ] `customSteps` config field accepted and validated
- [ ] Reserved CTE names (`event_data`, `session_data`, `item_list_attribution`, `item_list_data`, `enhanced_events`) reject collisions at config time
- [ ] `finalStep` renamed to `enhanced_events` throughout
- [ ] Pipeline emits unchanged SQL semantics when `customSteps` is empty (modulo the rename)
- [ ] Custom CTEs can reference all reserved CTE names
- [ ] BigQuery dry-run validates configurations with custom CTEs
- [ ] All existing tests pass; new test cases cover the matrix above
- [ ] README and AGENTS document `customSteps` and the reserved-names contract

## Testing Strategy

- **Unit tests in `tests/ga4EventsEnhanced.test.js`** — pipeline shape with various `customSteps` configurations:
  - Empty / undefined `customSteps` → pipeline identical to current behavior (with rename)
  - One raw step → renders as expected
  - One structured step → renders as expected
  - Multiple steps in mixed shapes → array order preserved, last step is final SELECT
  - Custom step references `event_data` → SQL valid
  - Custom step references `enhanced_events` → SQL valid
- **Config-shape validation tests in `tests/inputValidation.test.js`** (Layer 1):
  - `customSteps: 'not an array'` → throws
  - `customSteps: [null]` → throws
  - `customSteps: [{}]` → throws (no name)
  - `customSteps: [{name: 'a', query: '...'}, {name: 'a', query: '...'}]` → throws (duplicate within customSteps)
- **Collision tests in `tests/ga4EventsEnhanced.test.js`** (Layer 2 — runtime-derived reserved set):
  - `customSteps: [{name: 'event_data', query: '...'}]` → throws (always reserved)
  - `customSteps: [{name: 'session_data', query: '...'}]` → throws (always reserved)
  - `customSteps: [{name: 'enhanced_events', query: '...'}]` → throws (always reserved)
  - `customSteps: [{name: 'item_list_data', query: '...'}]` with no `itemListAttribution` → does NOT throw (not reserved when feature off)
  - `customSteps: [{name: 'item_list_data', query: '...'}]` with `itemListAttribution` enabled → throws (reserved when feature on)
- **Integration test** runs Dataform compile + BigQuery dry-run for the same configurations (after publish).

## Non-Goals

- **Custom column descriptions** for user-added columns. Out of scope for v0.8.0; users use Dataform's standard `dataformTableConfig.columns` override. Add as future config field if demand emerges.
- **Custom assertions** that target user-added columns. Out of scope; users add their own Dataform assertions.
- **Modifying or replacing internal CTEs.** Custom steps can only *append*. Replacing `event_data`'s logic is out of scope (and most use cases don't need it).
- **Cross-step parameterization or templating.** Custom steps are plain v2 step objects; no string interpolation, no parameterization beyond what the user computes outside the config.
- **Data enrichments helpers** (item 3 of the initiative). Separate design doc; this feature is the foundation it builds on.

## Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Users reference internal CTE names that the package later renames | Med | Q4 — document reserved CTE names as stable contract; renames bundle into minor version bumps. |
| Name collision produces confusing SQL errors instead of a clean validation error | Low | Q3, Q6 — pre-validate reserved names with a clear error message at the package boundary. |
| User CTEs bypass the package's date filter (e.g. by querying `event_data` directly) and produce too-large outputs | Low-Med | Document that `event_data` is unfiltered for the buffer-days range. Don't enforce — buffer access is sometimes intentional. |
| Users add CTEs that change the output schema, breaking auto-generated column descriptions / assertions | Low | Documented in Non-Goals — users override descriptions via `dataformTableConfig.columns` and disable affected assertions. |
| Renaming `final` → `enhanced_events` is a backward-compat change for anyone reading the generated SQL | Low | v0.8.0 is already a breaking release; bundle the rename. |

## References

- [design_docs/planned/query-builder-v2.md](design_docs/planned/query-builder-v2.md) — the v2 step shape this feature builds on
- [design_docs/implemented/query-builder-formatting.md](design_docs/implemented/query-builder-formatting.md) — formatting/indentation rules preserved through the pipeline
- [tables/ga4EventsEnhanced/index.js](tables/ga4EventsEnhanced/index.js) — primary file modified
- [tables/ga4EventsEnhanced/validation.js](tables/ga4EventsEnhanced/validation.js) — where new validation lives

## Future Work

- **Data enrichments** (item 3 of the initiative) — opinionated wrappers (item-level, session-level, page-level) that compile down to `customSteps`. Separate design doc; depends on this feature.
- **Custom column descriptions** — optional config field to attach descriptions to user-added columns, surfacing in the table schema. Out of scope for v0.8.0.
- **Per-step pre/post hooks** — e.g. modify `event_data` before `session_data` runs. Significantly more complex than append-only; rarely needed since users can express the same with custom CTEs.

---

**Document created**: 2026-05-06
**Last updated**: 2026-05-06
