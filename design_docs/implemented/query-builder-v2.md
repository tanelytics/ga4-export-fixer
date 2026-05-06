# Query Builder v2 — SQL-shaped Step Configuration

**Status**: Planned
**Target**: v0.8.0 (proposed — minor bump to signal the breaking change to `queryBuilder`'s exported shape)
**Priority**: P1 (Medium) — foundational for upcoming features
**Estimated**: 1.5 days (refactor + tests + migration)
**Dependencies**: None (this is the dependency for two follow-on features)

## Context

This design doc is the **first of three** related pieces of work:

1. **Query Builder v2** (this doc) — generalize step configuration so each top-level key maps to a SQL clause, instead of the current method-specific keys.
2. **Custom CTEs via configuration** — let users append their own step objects to the pipeline through the package config JSON. Depends on (1) so users have a SQL-shaped way to express full CTE bodies without requiring a method per clause.
3. **Data enrichments** — opinionated, controlled wrappers around (2) for the typical cases: item-level enrichments keyed by `item_id`, session-level by `session_id`, page-level by `page_location`. The enrichment surface compiles down to v2 step objects.

(1) is foundational because (2) and (3) both expose step objects to end-user configuration. The current ad-hoc shape (`leftJoin`, `groupBy`, `where`-only) is not suitable for external exposure — it is missing common SQL features and the keys are method names rather than SQL keywords. Fixing the shape now keeps the public surface for (2) and (3) stable.

## Problem Statement

`queryBuilder` in [utils.js:35-117](utils.js#L35-L117) accepts steps with this shape today:

```js
{
    name: 'session_data',
    columns: { session_id: 'session_id', max_ts: 'max(event_timestamp)' },
    from: 'event_data',
    leftJoin: [{ table: '...', condition: 'using(...)' }],
    where: 'session_id is not null',
    groupBy: ['session_id'],
}
```

**Limitations:**

- **Method-shaped, not SQL-shaped.** Keys like `leftJoin` and `groupBy` mirror the *builder method* rather than the SQL keyword. A SQL-literate user has to learn the mapping.
- **Limited clause coverage.** No `having`, no `qualify`, no `order by`, no `limit`, no `inner join` / `right join` / `full join`, no `window`, no `distinct`, no `union`. Each new clause currently requires changing `selectSQL` in `utils.js`.
- **Awkward column expression escape hatches.** Raw SQL fragments require either a `[sql]other_columns` key prefix (see [tables/ga4EventsEnhanced/index.js:240-243](tables/ga4EventsEnhanced/index.js#L240-L243)) or `key === value` to skip the alias. These will still be needed but they're hard to discover.
- **Internal-only today, but soon public.** The current shape is tolerable while the only caller is `_generateEnhancedEventsSQL` (one call, [tables/ga4EventsEnhanced/index.js:369](tables/ga4EventsEnhanced/index.js#L369)). The custom-CTE feature will make step objects part of the package's public configuration surface.

**Impact:** Adding any new SQL feature to a step today means editing `selectSQL`. Once step configs are user-supplied, the limited coverage will turn into recurring "please add support for X" requests.

## Goals

**Primary goal:** A step configuration where each top-level key (other than `name`) maps directly to a SQL clause, so a user who knows SQL can read and write step objects without learning a parallel vocabulary.

**Success criteria:**

- Step config keys read like SQL: `select`, `from`, `where`, `group by`, `having`, etc.
- The set of supported clauses covers the realistic needs of (a) the existing `ga4EventsEnhanced` pipeline and (b) reasonable user-defined enrichment CTEs.
- Existing helper output (multi-line SQL fragments returned by `helpers/*`) remains correctly indented — i.e. the formatting work from [query-builder-formatting.md](design_docs/implemented/query-builder-formatting.md) is preserved.
- The `ga4EventsEnhanced` table compiles to identical SQL (modulo whitespace) before and after the migration, validated by existing dry-run tests.
- One internal caller is migrated; no other call sites need changes (only `tables/ga4EventsEnhanced/index.js` uses `queryBuilder` today).

## Proposed Step Shape

A step is one of two shapes — **structured** (the bulk of this doc) or **raw** (`name` + raw SQL body). The two are mutually exclusive within a single step; both can coexist within a `steps` array.

### Structured shape

The default shape — each top-level key maps to a SQL clause. Sketch:

```js
{
    name: 'session_data',
    select: {
        columns: {
            session_id: 'session_id',
        },
        sql: 'max(event_timestamp) as max_ts',
        // optional: distinct: true
    },
    from: 'event_data',
    joins: [
        { type: 'left',  table: 'user_data',    on: 'using(user_id)' },
        { type: 'inner', table: 'session_meta', on: 'using(session_id)' },
    ],
    where: 'session_id is not null',
    'group by': 'session_id',
    having: 'max_ts > 1',
    'order by': 'session_id',
    qualify: 'row_number() over (...) = 1',
    limit: 100,
}
```

**General rules:**

- Each top-level key (except `name` and `joins`) corresponds to a SQL clause and takes a string value. Keys with spaces (e.g. `group by`, `order by`) match SQL keywords directly.
- Values are strings that are inserted under the clause keyword with appropriate indentation (the current `reindent` / `indentBlock` machinery from [utils.js:42-63](utils.js#L42-L63) is reused).
- `select` is one structured exception — it accepts either a raw string (sugar for `{sql: <string>}`) or an object `{ columns, sql }` where `columns` is the alias map (same semantics as v1) and `sql` is an optional raw column-list tail. See Q4 (resolved) for full rules.
- `joins` is the other structured exception — see Q3 (resolved) for the array shape and rendering rules.
- Column-expression escape hatches kept from v1: `key === value` skips the `as` alias, and the `[sql]<id>` key prefix marks raw expressions that should not be aliased ([utils.js:66-84](utils.js#L66-L84)). These become part of `select.columns`.
- Output ordering of clauses is **canonical** (the order SQL requires), not input order — keeps generated SQL stable regardless of how a user wrote the JSON. Within `joins`, array order is preserved (join order is semantic in SQL).
- All clauses except `select` and `from` are optional. Empty/absent clauses produce no output (no spurious blank lines, per the rules from [query-builder-formatting.md](design_docs/implemented/query-builder-formatting.md)).

### Raw shape

For users who already have a CTE written as a single block of SQL — typical for the upcoming **custom CTE configuration** feature (Future Work) — a step can be just `name` + raw `query` body:

```js
{
    name: 'session_data',
    query: `select
  session_id,
  max(event_timestamp) as max_ts
from event_data
where session_id is not null
group by session_id
having max_ts > 1`,
}
```

**Rules:**

- A step is detected as raw iff it has a top-level `query` key.
- In raw shape, the only allowed keys are `name` and `query`. Any other key (including clause keys like `where`, `joins`, `select`) throws — the two shapes are mutually exclusive within a step. (Mixing them silently would create ambiguity about whether the body or the clauses win.)
- The string is emitted verbatim into the CTE body, with `reindent` applied so the relative indentation the user wrote is preserved at the CTE's indent depth.
- `name` is still required and used for CTE wrapping the same way as for structured steps.

**Naming note: `query` (top-level) vs. `select.sql` (nested).** Two distinct fields with no name collision:

| Where | Meaning |
|---|---|
| `step.query` (top-level) | The entire CTE body. Mutually exclusive with structured clause keys. |
| `step.select.sql` (nested) | A raw column-list tail appended after `step.select.columns`. Lives inside the structured shape. |

`query` reads naturally as "the query this CTE wraps" (mirroring SQL's `WITH name AS (<query>)`). `sql` is reserved for the column-list-tail use case inside `select`, where the value really is just a fragment of SQL rather than a full query.

**Why an object and not just a bare string entry in the `steps` array.** A bare string `'select ... from ...'` would have no place to attach the CTE name — and `name` is required for any step that isn't the final one. Keeping the shape as an object (`{name, query}`) preserves the invariant that every step is uniformly addressable.

**Why this matters now.** This is the shape the **custom CTEs via configuration** feature (Future Work item 1) will hand to `queryBuilder` for user-supplied CTEs. Designing it in v2 means feature (2) is just plumbing — user-supplied `{name, query}` objects get appended to the `steps` array, and `queryBuilder` already knows how to render them. Without this, feature (2) would either force users to decompose their SQL or require a special-case path that bypasses `queryBuilder`.

### Concrete migration of an existing step

**Before** (today's `sessionDataStep`, [tables/ga4EventsEnhanced/index.js:251-265](tables/ga4EventsEnhanced/index.js#L251-L265)):

```js
{
    name: 'session_data',
    columns: { session_id: 'session_id', /* ... */ },
    from: 'event_data',
    where: `session_id is not null`,
    groupBy: ['session_id'],
}
```

**After:**

```js
{
    name: 'session_data',
    select: { columns: { session_id: 'session_id', /* ... */ } },
    from: 'event_data',
    where: 'session_id is not null',
    'group by': 'session_id',
}
```

**Before** (today's `finalStep` left joins, [tables/ga4EventsEnhanced/index.js:319-360](tables/ga4EventsEnhanced/index.js#L319-L360)):

```js
{
    /* ... */
    leftJoin: [
        { table: 'item_list_data', condition: 'using(_item_list_attribution_row_id)' },
        { table: 'session_data', condition: 'using(session_id)' },
    ],
}
```

**After** (single `joins` array preserves order; mixed types fit naturally — see Q3 resolution below):

```js
{
    /* ... */
    joins: [
        { type: 'left', table: 'item_list_data', on: 'using(_item_list_attribution_row_id)' },
        { type: 'left', table: 'session_data',   on: 'using(session_id)' },
    ],
}
```

## Design Decisions

All design questions are resolved. Each entry below records the resolution and the reasoning trail; future readers can use these as the rationale for the chosen shape.

### Q1. Whitelist of clause keys (RESOLVED)

**Resolution:** Option A — strict whitelist. `queryBuilder` knows the closed set of valid step keys, branched by shape:

- **Structured step:** `name` + any subset of the clause-registry keys (`select`, `from`, `joins`, `where`, `group by`, `having`, `qualify`, `order by`, `limit` — see Q7 for the v2 set).
- **Raw step:** `name` + `query` only.

A step is detected as raw iff it has a top-level `query` key. The two shapes are mutually exclusive — mixing them within one step throws.

**Validation behavior.** When `queryBuilder` receives a step, it determines the shape (raw vs. structured), scans the keys against the corresponding allow-list, and throws a clear error on any unknown or shape-incompatible key, naming the bad key and listing the valid ones. Typos like `gruop by`, `wehre`, `joinz` fail at config time with a precise message rather than at BigQuery dry-run with a confusing one.

**Adding a new clause.** Requires adding an entry to the renderer registry in `utils.js`. This is the right friction — every new clause should get a deliberate review of how it indents, where it sits in canonical order (Q2), and whether it has a structured value shape.

**Niche / vendor-specific clauses.** Users who need a clause v2 doesn't support can fall back to writing it inline inside `select.sql` or as raw SQL in another clause's value. If a niche clause becomes a recurring need, add it to the registry then.

### Q2. Clause ordering (RESOLVED)

**Resolution:** Option A — canonical order. Clauses are always emitted in valid SQL order regardless of how they appear in the input object.

**Canonical order for v2:**

```
select → from → joins → where → group by → having → qualify → order by → limit
```

(`joins` is the array's *position* in the rendered query — entries within the array still render in array order, since join order is semantic. See Q3.)

**Implementation.** The renderer registry (`CLAUSE_RENDERERS` in the dispatcher sketch) is itself an ordered array. Iterating over it in declaration order produces canonical output. Registry order = canonical clause order; nothing else needs to know about ordering.

**Why this over input order.** Object key insertion order is preserved in modern JS, but it's a brittle thing to depend on (JSON round-trips, config merges, defensive object copies can all break it). Canonical order is also one less thing for step authors to think about — they can write keys in whatever order is most readable.

### Q3. Joins — `joins` array, with string fallback (RESOLVED)

**Why an array.** SQL join order is semantic (an `INNER JOIN` after a `LEFT JOIN` can drop rows that the `LEFT JOIN` preserved; an `ON` clause can only reference tables introduced by earlier joins). A single ordered array preserves the user's intent without relying on object key order or canonical reordering.

**Why `joins` and not `join`.** `JOIN` alone in SQL means `INNER JOIN`, so the singular key would be misleading. The plural breaks that association cleanly.

**Shape.**

```js
joins: [
    { type: 'left',  table: 'session_data',   on: 'using(session_id)' },
    { type: 'inner', table: 'item_list_data', on: 'using(_item_list_attribution_row_id)' },
    { type: 'cross', table: 'unnest(items)' },
],
```

- `type` — one of `'left'`, `'inner'`, `'cross'`, `'right'`, `'full'`. Lower-case, short forms only (no `outer`, no `join` suffix).
- `table` — the joined table, CTE, subquery, or `unnest(...)` expression.
- `on` — the join condition, written as either `'using(...)'` or a boolean expression. Emitted **as-is** after the table: the renderer produces `<type> join <table> <on>`. So `using(session_id)` and `a.x = b.x` both work, with no special-casing.
- `cross` joins omit `on` (BigQuery doesn't accept `ON` / `USING` on a `CROSS JOIN`).

**String fallback.** When the structured form is awkward (e.g. complex `unnest` chains, vendor-specific join clauses), `joins` accepts a string instead — emitted verbatim. Useful as an escape hatch:

```js
joins: 'left join unnest(items) as item with offset as item_offset on true',
```

**Renderer behavior.** `renderJoins(value)`:

- If `value` is a string: emit it inline, reindented to the insertion column. Same as `renderInline`.
- If `value` is an array: emit each entry as `<type> join <table>[ <on>]`, joined with newlines, reindented to the insertion column.

**Order in the final query.** `joins` sits between `from` and `where` in the canonical clause order (Q2). Within the array, entries render in the order given.

### Q4. `select` shape (RESOLVED)

**Resolution:** `select` accepts either a string or an object — string is sugar for the `sql`-only object form.

**String form** (raw passthrough or ad-hoc select list):

```js
select: '*',
select: 'distinct user_id, count(*) as n',
```

**Object form** (structured alias map, optional raw tail):

```js
select: {
    columns: {
        event_date: helpers.eventDate,
        event_timestamp: 'event_timestamp',          // key === value: no alias
        event_custom_timestamp: cond ? expr : undefined,  // undefined: filtered out
    },
    sql: '* except (entrances, session_params_prep)',  // optional raw column-list tail
}
```

**Equivalence rule.** `select: '<string>'` is treated as `select: { sql: '<string>' }`. One renderer, two surface forms — no duplicate logic.

**Field semantics inside the object form:**
- `columns` — object map of `alias → expression`. Same semantics as v1's top-level `columns`: `key === value` skips the `as` alias, `[sql]<id>`-prefixed keys emit the raw value with no alias and no key-as-text, `undefined` values are filtered out.
- `sql` — raw column-list SQL appended after the `columns` entries (separator: `,\n<indent>`). Order between `columns` and `sql` is fixed (columns first); users who need a raw entry mid-list use a `[sql]<id>` key inside `columns`.
- Either field is optional, but at least one must be present (otherwise the step has no select list).
- Future fields (`distinct: true`, BigQuery `* EXCEPT (...)` / `* REPLACE (...)`) can be added as sibling object keys without breaking the shape.

**Why not the other alternatives.** Keeping `columns` at the top level (Alternative C from the prior analysis) breaks the "every top-level key is a SQL clause" framing. An ordered-array `select` (Alternative D) maps cleanest to SQL but forces a rewrite of the existing object-spread column-composition patterns in [tables/ga4EventsEnhanced/index.js:201-247](tables/ga4EventsEnhanced/index.js#L201-L247) — too costly for v2.

### Q5. Field naming for joins (RESOLVED)

Field names in the join object: `type`, `table`, `on`. The `on` value is emitted verbatim after the table — both `using(...)` and bare boolean expressions work without special-casing.

### Q6. Backwards compatibility (RESOLVED)

**Resolution:** Hard switch. v1 shape support is deleted; the one internal caller is migrated in the same change.

**Rationale.** Only one caller exists ([tables/ga4EventsEnhanced/index.js:369](tables/ga4EventsEnhanced/index.js#L369)). The package's documented public API is `createTable` / `generateSql` / `setPreOperations` / etc. — `queryBuilder` is exported from `utils.js` ([utils.js:472-482](utils.js#L472-L482)) but not part of the documented surface. A dual-support window would mean carrying two code paths and emitting deprecation warnings for users who almost certainly don't exist.

**Implications:**

- **Breaking change for any direct importer of `utils.queryBuilder`.** The `0.7.x → 0.8.0` minor-version bump is the canonical signal under 0.x semver; the git tag annotation can carry a brief before/after example for the step shape and a pointer to this doc. (No CHANGELOG file in this repo by convention — version history lives in git tags.)
- **Version bump.** Package is at `0.7.1` ([package.json](package.json)). Under 0.x semver the minor bump signals a breaking change — `0.8.0` is the natural target.
- **One-shot migration.** [tables/ga4EventsEnhanced/index.js](tables/ga4EventsEnhanced/index.js) is rewritten to v2 shape in the same PR that lands the v2 `queryBuilder`. No transitional state where some steps are v1 and others are v2.
- **Tests update with the migration.** Any tests that hand-construct step objects (currently none — `tests/ga4EventsEnhanced.test.js` exercises `queryBuilder` indirectly through the table-module's exported SQL generator) port over with the implementation.

### Q7. Scope of clauses to support in v2 (RESOLVED)

Confirmed scope. v2 covers:

- `select` (with `columns` and `sql`)
- `from`
- `joins` (supports `left`, `inner`, `cross`, `right`, `full`)
- `where`
- `group by`
- `having`
- `order by`, `limit` (cheap, useful for enrichments)
- `qualify` (BigQuery — useful for window-based enrichments)

Out of v2 (defer until needed): `union` / `union all`, recursive CTEs, `window` definitions, `tablesample`, set operators.

## Solution Design (Sketch)

### Architecture

The bones of `queryBuilder` stay the same:

- `reindent` and `indentBlock` ([utils.js:42-63](utils.js#L42-L63)) are reused unchanged.
- `columnsToSQL` ([utils.js:66-84](utils.js#L66-L84)) is reused for `select.columns`.
- `selectSQL` is replaced by a top-level `renderStep` dispatcher that branches on shape (raw `{name, query}` vs. structured) and, for the structured branch, iterates a registry of `{ keyword, render(value, ctx) }` entries indexed by clause key.
- CTE wrapping ([utils.js:111-117](utils.js#L111-L117)) is unchanged.

### Sketch of the dispatcher

```js
const CLAUSE_RENDERERS = [
    { key: 'select',   render: renderSelect           },
    { key: 'from',     render: renderInline('from')   },
    { key: 'joins',    render: renderJoins            },
    { key: 'where',    render: renderInline('where')  },
    { key: 'group by', render: renderInline('group by') },
    { key: 'having',   render: renderInline('having') },
    { key: 'qualify',  render: renderInline('qualify') },
    { key: 'order by', render: renderInline('order by') },
    { key: 'limit',    render: renderInline('limit')  },
];

// Top-level dispatcher: handle raw shape, otherwise fall through to clause registry.
const renderStep = (step) => {
    if ('query' in step) {
        // Raw shape: emit the body verbatim, indentation normalized to col 0
        return reindent(step.query, 0);
    }
    return CLAUSE_RENDERERS
        .filter(c => step[c.key] !== undefined)
        .map(c => c.render(step[c.key]))
        .join('\n');
};

const renderJoins = (value) => {
    if (typeof value === 'string') {
        // string fallback: emit verbatim, reindented
        return reindent(value, 0);
    }
    return value
        .map(j => j.type === 'cross'
            ? `cross join ${j.table}`
            : `${j.type} join ${j.table} ${j.on}`)
        .join('\n');
};
```

`renderInline(keyword)` returns `${keyword}\n${pad}${reindent(value, INDENT)}`. `renderJoins` handles either an array of `{type, table, on}` objects or a raw string fallback. `renderSelect` normalizes the string form to the object form first, then renders:

```js
const renderSelect = (value) => {
    const { columns, sql } = typeof value === 'string' ? { sql: value } : value;
    const parts = [];
    if (columns) parts.push(columnsToSQL(columns));
    if (sql)     parts.push(reindent(sql, INDENT));
    return `select\n${pad}${parts.join(',\n' + pad)}`;
};
```

Validation (per Q1) happens up front, branching on shape:

- **Raw step** (has top-level `query`): only `name` and `query` are allowed. Any other key throws with a message naming the offender and pointing out that raw and structured shapes don't mix within a step.
- **Structured step** (no top-level `query`): only `name` plus the clause-registry keys are allowed. Unknown keys throw with the bad key named and the valid set listed.

### Implementation plan

**Phase 1 — `queryBuilder` v2 (~4h)**
- [ ] Add clause renderer registry to `utils.js`
- [ ] Replace `selectSQL` with `renderStep` (raw vs. structured shape dispatch)
- [ ] Implement `renderSelect` (string form + `{columns, sql}` object form)
- [ ] Implement `renderJoins` (array of `{type, table, on}` + string fallback)
- [ ] Validation pass (Q1) — branched on shape

**Phase 2 — Migrate `ga4EventsEnhanced` (~2h)**
- [ ] Rewrite the four step objects in [tables/ga4EventsEnhanced/index.js](tables/ga4EventsEnhanced/index.js) to v2 shape
- [ ] Confirm dry-run tests pass — generated SQL should be functionally identical

**Phase 3 — Tests & docs (~3h)**
- [ ] Unit tests for `queryBuilder`: each clause renders correctly; ordering is canonical; missing optional clauses produce no blank lines; multi-line values are reindented
- [ ] Update JSDoc on `queryBuilder` ([utils.js:21-34](utils.js#L21-L34)) to describe the new shape
- [ ] Update [AGENTS.md](AGENTS.md) / [README.md](README.md) only if they reference the v1 shape; the v1 shape was internal so this is unlikely

### Files to modify

| File | Change | Est. LOC |
|------|--------|----------|
| [utils.js](utils.js) | Refactor `queryBuilder` and `selectSQL`, add clause registry, update JSDoc | ~+80 / -40 |
| [tables/ga4EventsEnhanced/index.js](tables/ga4EventsEnhanced/index.js) | Migrate four step objects to v2 shape | ~+15 / -15 |
| `tests/queryBuilder.test.js` (new) | Unit tests for v2 behavior | ~+150 |

## Testing Strategy

- **Unit tests for `queryBuilder` directly** (new file). The current tests only exercise it indirectly via `ga4EventsEnhanced.test.js`. Direct tests make it safe to extend later for custom-CTE / enrichment features.
  - **Structured shape:** each clause keyword is rendered correctly when present; each clause is omitted (no keyword, no blank line) when absent; canonical ordering holds when keys are passed in random order; multi-line clause values get re-indented to the insertion column.
  - **Raw shape:** `{name, query}` body is emitted verbatim; `reindent` normalizes the body to the CTE's indent depth; the same step composes correctly with neighboring structured steps in a `steps` array.
  - **CTE wrapping** unchanged for >1-step pipelines, regardless of shape mix.
  - **Validation:** unknown structured-step keys throw; raw + structured key mix in one step throws; both error paths produce messages that name the bad key and list valid ones.
- **Existing `ga4EventsEnhanced` dry-run test** is the regression check that the migration didn't break SQL semantics. Whitespace differences are acceptable; SQL semantics must be identical.
- **Integration test** (`npm run test:integration`) exercises full Dataform compilation end-to-end.

## Non-Goals

- Custom-CTE configuration surface — separate doc, depends on this one.
- Data enrichment helpers (`itemEnrichment`, `sessionEnrichment`, `pageEnrichment`) — separate doc, depends on this one.
- Replacing the `helpers/` flat library structure — orthogonal.
- Adding SQL formatting beyond what [query-builder-formatting.md](design_docs/implemented/query-builder-formatting.md) already established.
- Supporting non-BigQuery dialects.

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| `queryBuilder` is exported from `utils.js`; external callers (if any) break on the shape change | Low | Q6 resolves to hard switch. Single internal caller; `queryBuilder` isn't part of the documented public API (`createTable` / `generateSql` are). The `0.7.1 → 0.8.0` minor-version bump signals the break under 0.x semver; git tag annotation carries the migration note. |
| Whitelist (Q1) blocks a user who needs a niche clause v2 doesn't support | Low | Users can fall back to inlining the clause inside `select.sql` or another clause's value; a recurring need triggers a registry addition in a follow-up. |
| Mixed-type join chains lose user-intended order if the renderer reorders by type | Resolved | Q3 uses an ordered `joins` array — entries render in array order, mirroring the user's intent. |
| Migration of `ga4EventsEnhanced` step objects introduces a semantic SQL diff | Low | Existing dry-run + integration tests catch this. Generate full SQL pre/post and diff before merging. |

## References

- [utils.js:35-117](utils.js#L35-L117) — current `queryBuilder` implementation
- [tables/ga4EventsEnhanced/index.js:201-369](tables/ga4EventsEnhanced/index.js#L201-L369) — only caller of `queryBuilder` today
- [design_docs/implemented/query-builder-formatting.md](design_docs/implemented/query-builder-formatting.md) — formatting/indentation rules to preserve
- [design_docs/implemented/separate-merge-from-sql-generation.md](design_docs/implemented/separate-merge-from-sql-generation.md) — adjacent refactor; same architectural style of "thin exported wrapper + internal base function"

## Future Work

- **Custom CTE configuration** — expose v2 step objects through the package config so users can append their own CTEs to the pipeline. The raw step shape (`{name, query}`) is the primary surface for this feature: users supply name + body and the package appends them to the `steps` array. The structured shape is also available for users who want it. Depends on this doc.
- **Data enrichments** — opinionated wrappers (item-level, session-level, page-level) that compile down to v2 step objects. Depends on this doc and on custom CTEs.

---

**Document created**: 2026-05-05
**Last updated**: 2026-05-06
