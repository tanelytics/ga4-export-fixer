# Data Enrichments

**Status**: Planned (Draft)
**Target**: v0.9.0 (proposed)
**Priority**: P1 (Medium) — third piece of the v0.8.0 initiative
**Estimated**: 1.5–2 days
**Dependencies**: [custom-ctes](../implemented/custom-ctes.md), [query-builder-v2](../implemented/query-builder-v2.md) — both shipped in `0.8.0-dev`

## Context

This is the third piece of the three-part initiative outlined in [query-builder-v2.md](../implemented/query-builder-v2.md):

1. **Query Builder v2** ✅ landed `0.8.0-dev.0` — generalized step configuration and the raw `{name, query}` shape.
2. **Custom CTEs via configuration** ✅ landed `0.8.0-dev.2` — `customSteps` config field for appending arbitrary v2-shaped steps.
3. **Data enrichments** (this doc) — opinionated wrappers around `customSteps` for the typical case: joining external dimension data into the GA4 events output.

Anything that data enrichments can do, `customSteps` can already do — write the source CTE manually, write the join manually, manage the column conflicts manually. The point of this feature is to make the typical cases declarative and safe: the user describes *what* dimension data to attach and *where* to attach it, and the package handles the SQL.

The expected usage pattern is **most users reach for enrichments first, drop down to customSteps only when the structured shape doesn't fit**.

## Problem Statement

A user who wants to attach external dimension data to GA4 events today has two paths:

1. **Wrap the package output in a downstream Dataform model.** Adds an extra model file and materialization for what should be a one-line addition.
2. **Use `customSteps`.** Works, but the user writes the source CTE, the join, and the column-list passthrough manually — and has to think about deduplication, name collisions, and rebuilding the items array if the join is item-level.

Four patterns recur often enough that they justify a tighter API:

- **Session-level enrichment** — joining session-grained data (session-quality scores, marketing attribution computed at session grain, A/B test assignments) onto each event by `session_id`.
- **User-level enrichment** — joining user-grained data (cohort labels, lifecycle-stage flags, customer segments, LTV buckets) onto each event by `user_pseudo_id`, `user_id`, or `merged_user_id`. The choice of key matters: `user_pseudo_id` covers anonymous and identified users alike but resets per device; `user_id` only matches identified users but persists across devices; `merged_user_id` is the package's coalesce of the two.
- **Page-level enrichment** — joining a page-metadata table (content group, page template, internal owner) by `page_location` or `page.path`.
- **Item-level enrichment** — joining product master data (margin, brand, internal SKU) onto each item inside the `items` array. This is the most complex case because the join key (`item_id`) is nested in an array, so the items array has to be unnested, joined, and re-aggregated — exactly what [item-list-attribution](../implemented/item-list-attribution.md) already does for its own purposes.

Each of these has a clean canonical form. The `customSteps` API forces every user to re-derive it, which is friction without flexibility benefit.

Mechanically, only two cases need distinguishing. The first three patterns (session/user/page) are the same operation — a flat `LEFT JOIN` at the event grain on different keys. Item-level differs because the join key is nested inside the `items` array, requiring an unnest+rebuild. The config exposes these as `level: 'event' | 'item'`; the user's choice of `joinKey` carries the cardinality intent (`session_id`, `user_pseudo_id`, `page_location`, `item_id`, etc.). The same `event` mechanism extends to arbitrary keys outside the four common patterns — for example, a `company_id` or `market_id` collected as an event parameter and promoted to a column via `eventParamsToColumns`.

## Goals

**Primary goal:** A declarative `enrichments` config field that compiles to enrichment-source CTEs at the top of the pipeline plus join modifications to `enhanced_events`. Users describe the dim data and the join key; the package handles the rest.

**Success criteria:**

- Users can attach external dimension data at the event grain (any column on `enhanced_events` — `session_id`, `user_pseudo_id`, `page_location`, or any column promoted via `eventParamsToColumns`) and at the item grain (joining inside the `items` array) via a single config field.
- The config exposes two levels: `event` (flat join at `enhanced_events` for any column) and `item` (nested array unnest+rebuild). The user's `joinKey` choice carries the cardinality intent.
- For event-level enrichment, the join is added to the existing `enhanced_events` step's `joins` array — no new structural pipeline complexity.
- For item-level enrichment, the package reuses the proven item-list-attribution scaffold (`unnest(items)` → transform → `array_agg(select as struct item.* replace(...))`) so item-level joins don't introduce a new mechanism.
- Reserved-CTE-name protections from custom-ctes still apply; enrichment CTEs use a distinct prefix so they don't collide with user `customSteps`.
- Anything users can do today with `customSteps` they can still do; `enrichments` is purely additive.

## Proposed Configuration

```js
ga4EventsEnhanced.createTable(publish, {
    sourceTable: { schema: 'analytics_123', name: 'events_*' },
    enrichments: [
        // User-grained: join cohort labels onto each event by user_pseudo_id
        {
            name: 'user_cohorts',
            level: 'event',
            source: { schema: 'analytics', name: 'user_cohorts' },
            joinKey: 'user_pseudo_id',
            columns: ['cohort', 'lifecycle_stage'],
        },
        // Page-grained: attach page metadata by page_location
        {
            name: 'page_metadata',
            level: 'event',
            source: '`proj.ds.page_metadata`',
            joinKey: 'page_location',
            columns: ['content_group', 'page_owner'],
        },
        // Item-grained: attach product master data (joined inside the items array)
        {
            name: 'product_master',
            level: 'item',
            source: { schema: 'analytics', name: 'product_master' },
            joinKey: 'item_id',
            columns: ['margin_bucket', 'brand_internal'],
        },
    ],
});
```

Each entry compiles to:

1. An **enrichment-source CTE** at the top of the pipeline, named `enrich_<name>` (e.g. `enrich_user_cohorts`). Selects `joinKey` plus the requested `columns` from `source`.
2. **Join integration** into `enhanced_events`:
   - For `level: 'event'` — adds a `LEFT JOIN enrich_<name> USING (joinKey)` to the existing `joins` array. Adds the requested columns to `select.columns` qualified as `enrich_<name>.<column>`. The cardinality of the dim data (one row per session vs. user vs. page vs. arbitrary key) is determined by the user's `joinKey` choice.
   - For `level: 'item'` — reuses the item-list-attribution scaffold: unnest items, join `enrich_<name>` inside the unnest, re-aggregate with `array_agg(select as struct item.* replace(...))`, override the `items` column in `enhanced_events` via the same coalesce pattern already used by item-list-attribution.

## Pipeline Integration

The pipeline is extended at two points only:

```
[enrich_X CTEs at top]                        ← enrichment sources (one CTE per entry)
event_data
[items_unnested, items_rebuilt]               ← when itemListAttribution OR any item-level enrichments are configured (Q16)
session_data
enhanced_events                                ← extended joins + extended select.columns
[customSteps]                                  ← user-supplied steps, unchanged
```

Notable properties:

- Enrichment-source CTEs sit at the top, before `event_data`. They're independent of the GA4 export and can be referenced freely by later steps.
- Event-level enrichments don't add new CTEs after `event_data` — they just extend `enhanced_events`'s join list.
- Item-level enrichments share the two-CTE item scaffold (`items_unnested`, `items_rebuilt`) with `itemListAttribution`. The scaffold is emitted whenever either feature is active; multiple item-level enrichments add additional joins inside `items_rebuilt` rather than additional CTE pairs (Q16).
- The reserved-name set from `custom-ctes` automatically expands to include the new `enrich_*` names because it's runtime-derived from `packageSteps`. The `items_unnested` / `items_rebuilt` names replaced the previously-reserved `item_list_attribution` / `item_list_data` in `0.9.0-dev.0` (see [items-cte-prep-sprint.md](items-cte-prep-sprint.md)).

## Design Decisions

### Q1. Pipeline placement (RESOLVED)

**Resolution:** enrichment-source CTEs sit at the **top** of the pipeline, before `event_data`.

External dimension tables don't depend on `event_data`, so they can be defined first. Defining them at the top means later CTEs (`event_data`, `session_data`, `enhanced_events`) can reference them without ordering constraints.

BigQuery's optimizer skips CTEs that no later step references, so leaving them at the top has no execution-cost penalty.

### Q2. Join location — depends on level (RESOLVED)

**Resolution:** the join location is determined by the enrichment's `level`:

- **`event` (any flat column on `enhanced_events`):** join at `enhanced_events`. The existing `joins` array gets a new entry; the existing `select.columns` gets new entries qualified by the enrichment CTE name. No new structural CTEs are needed beyond the enrichment source.
- **`item` (nested key inside `items` array):** use the shared two-CTE item scaffold (`items_unnested`, `items_rebuilt` — see Q16). The items array is unnested, joined with the enrichment source on the chosen `joinKey`, and re-aggregated. The rebuilt `items` column overrides the original via `coalesce(<rebuilt>, event_data.items)` in `enhanced_events` — the same coalesce pattern previously used by item-list-attribution.

Joining at `enhanced_events` rather than earlier (e.g. on `event_data`) is essential: `event_data` has many rows per session, so a session-grained or user-grained join would multiply rows. By the time the pipeline reaches `enhanced_events`, session aggregations have already produced one canonical row per event at the right grain for the join.

### Q3. Duplication safeguards (RESOLVED)

**Resolution:** trust the user by default; offer opt-in dedupe via a `dedupe: true` flag on the per-enrichment config. No package-generated assertions for enrichment sources.

The package can't validate that an arbitrary user-supplied dimension table has unique values for the chosen `joinKey`, and a `LEFT JOIN` against a duplicated key silently multiplies rows. Uniqueness is the user's contract; the package's role is to keep the most common safety net (`dedupe`) one config field away.

**Default behavior** (`dedupe` omitted or `false`): the enrichment source is used as-is. The user is responsible for ensuring `joinKey` uniqueness in their source — typically by selecting an already-unique key, or by pre-aggregating in source SQL via `group by joinKey` + `any_value(...)` for non-key columns.

**Opt-in dedupe** (`dedupe: true`): the package wraps the enrichment-source CTE in `qualify row_number() over (partition by joinKey) = 1`. This picks one row per `joinKey` non-deterministically. Users who need a specific row to win (latest, highest-priority, etc.) should pre-aggregate in their source SQL rather than rely on `dedupe`.

**What's not in scope:**

- **Always-on dedupe.** Silently masks data quality issues in every case and adds an unnecessary window function for genuinely unique sources.
- **Auto-aggregation at the join** (subquery-wrapped `group by joinKey` + `any_value(...)`). Same silent-masking concern; users who need this write it in their source SQL.
- **Package-generated Dataform assertions for enrichment uniqueness.** Adds machinery the package doesn't otherwise have for external sources. Uniqueness assertions are a project-level concern that users add to their own dim tables in Dataform if they want them.

### Q4. Join key management (RESOLVED)

**Resolution:** two `level` values, each with its own join mechanism:

| `level` | Join key | Join location | Mechanism |
|---|---|---|---|
| `event` | any column on `enhanced_events` (single string or array for composite keys — Q18) | `enhanced_events.joins` | flat `LEFT JOIN ... USING (<keys>)` |
| `item` | any field on the `item` struct (typically `item_id`); `event_date` is also available for composite keys (Q18) | shared `items_unnested` / `items_rebuilt` scaffold (Q16) | unnest → join → `array_agg(select as struct item.* replace(...))` |

`joinKey` is required in both cases — no default — so the entry's intent reads at a glance. Single-column joins use a string (`joinKey: 'session_id'`); composite joins use an array (`joinKey: ['event_date', 'user_pseudo_id']`). See Q18 for composite-key semantics.

For `level: 'event'`, the chosen column must exist on `enhanced_events` (validated at SQL generation time). The cardinality of the dim data is whatever the user's `joinKey` implies — common choices include:

- `session_id` for session-grained dim data
- `user_pseudo_id`, `user_id`, or `merged_user_id` for user-grained dim data (each has different identity semantics — `user_pseudo_id` covers anonymous and identified users alike but resets per device; `user_id` only matches identified users but persists across devices; `merged_user_id` is the package's coalesce of the two)
- `page_location` for page-grained dim data
- any column promoted via `eventParamsToColumns` (e.g. `company_id`, `market_id`) for arbitrary keys

For `level: 'item'`, the chosen `joinKey` must be a field on the `item` struct in the `items` array. `item_id` is the typical choice (canonical primary key for items), but other fields work if they're the appropriate key for the dim data.

For `page.path` joins specifically: the `page` column on `enhanced_events` is a struct, so `page.path` requires either a struct-field reference (BigQuery supports `STRUCT.field` in `USING` only when both sides have the same struct shape, which they won't) or pre-flattening. The simplest path is to require the user to pre-flatten in their source SQL: `select page_location, content_group from ...`. To surface as a documentation note, not as automatic struct-aware joining.

### Q5. USING vs ON (RESOLVED)

**Resolution:** enforce `USING`. The `joinKey` field carries one column name; the user's source SQL must produce a column with the same name.

This matches the opinionated framing of the feature — fewer ways to write the join wrong, fewer config fields to document. Users whose dim table uses a different column name pre-alias it in their source SQL (`select user_id_legacy as user_pseudo_id, cohort from ...`).

The escape hatch is direct `customSteps`, which already supports both `using(...)` and `on a.x = b.y` forms. This stays true to the rule "anything users can do with `customSteps` they can still do — `enrichments` is for the typical case".

### Q6. CTE name prefix (RESOLVED)

**Resolution:** auto-prefix every enrichment-source CTE with `enrich_`. So `enrichments[0].name === 'user_cohorts'` produces a CTE named `enrich_user_cohorts`.

Benefits:

- Visual separation in generated SQL — readers can see at a glance which CTEs are user-supplied dim sources vs. package-internal pipeline steps vs. user `customSteps`.
- Reduces collision risk with the existing reserved set (`event_data`, `session_data`, `enhanced_events`, etc.) and with user-supplied `customSteps` names. The user's enrichment name only has to be unique among other enrichments, not against the whole reserved set.
- Surfaces intent: anyone reading `enrich_*` knows the CTE provides external dim data.

Cost: slight verbosity — `enrich_user_cohorts` is two extra characters and one underscore over `user_cohorts`.

### Q7. Config field name (RESOLVED)

**Resolution:** `enrichments`. Array of objects, plural. Matches the verb-form usage ("attach enrichments to events") and the array shape.

Considered alternatives: `dataEnrichments` (verbose), `enrich` (verb, awkward as a noun config field).

### Q8. Per-entry shape (RESOLVED)

**Resolution:** each entry in `enrichments` has the following shape:

```js
{
    name: 'user_cohorts',          // string, required — used in the enrich_<name> CTE name
    level: 'event',                // 'event' | 'item' — optional, defaults to 'event'
    source: { schema: 'dim', name: 'cohorts' },  // ref object, ref() inside SQLX js block, or backtick-FQN — required
    joinKey: 'user_pseudo_id',     // string or string[] — required, no default; arrays compile to USING(col1, col2) (Q18)
    columns: ['cohort'],           // string[] — required, non-empty list of source columns to add to the output (excluding joinKey)
    dedupe: false,                 // boolean, optional — see Q3 (defaults to false)
}
```

`name`, `source`, `joinKey`, and `columns` are required. `level` defaults to `'event'` if omitted (the structurally specialized `'item'` is opt-in). `dedupe` is optional and defaults to `false` (per Q3).

Validation follows the existing `eventParamsToColumns` pattern from [tables/ga4EventsEnhanced/validation.js](../../tables/ga4EventsEnhanced/validation.js) (array shape, non-null objects, name uniqueness within `enrichments`, allowed `level` values).

Layer 1 / Layer 2 validation split (per [custom-ctes Q6](../implemented/custom-ctes.md)) applies:

- **Layer 1 (config shape):** `enrichments` is array, each entry is a non-null object, `name` is non-empty unique string, `level` (when present) is one of the allowed values, `source` is a valid Dataform ref or backtick string, `joinKey` is a non-empty string OR a non-empty array of non-empty strings (Q18), `columns` is a non-empty array of strings.
- **Layer 2 (collision & schema):** `enrich_<name>` doesn't collide with the runtime-derived reserved set or with user `customSteps` names. For `level: 'event'`, every `joinKey` column exists on `enhanced_events`; for `level: 'item'`, every `joinKey` column is a field on the `item` struct or `event_date`. Column-overlap behavior (coalesce-then-add or add) is determined at SQL generation time per Q13 (event-level) and Q17 (item-level); enrichment-vs-enrichment column collisions throw.

### Q9. Source format (RESOLVED — with optional expansion)

**Resolution:** `source` accepts the same two formats as the existing `sourceTable` config field:

- A Dataform table reference object — either a manually constructed `{ schema, name }` object (the form usable in `.js` definition files) or the return value of `ref(...)` from inside an SQLX `js { }` block. Resolved at SQL-generation time via `ctx.ref(obj)` inside the package's `setDataformContext`.
- A backtick-quoted string in `` `project.dataset.table` `` format. Passed through as literal SQL.

Note that `ctx.ref(...)` itself is **not** available at config-construction time — it is bound only inside Dataform callbacks (`.preOps(ctx => ...)`, `.query(ctx => ...)`, `.assert(ctx => ...)`). Likewise `ref(...)` is bound only inside SQLX `js { }` blocks; in `.js` definition files neither is available, and users must use the manually-constructed `{ schema, name }` form or a backtick-FQN.

Reuses `isDataformTableReferenceObject` from [utils.js](../../utils.js) for type detection.

**Possible expansion (open):** also accept a raw SQL query string (e.g. for users who need to pre-filter or aggregate the dim before the join). If accepted, the package wraps the user's SQL in `(<query>)` as the join target. Adds flexibility but introduces parsing concerns (must distinguish between a backtick-quoted table reference and a SQL query). To be resolved interactively if there's a recurring need.

### Q11. Multiple enrichments per level (RESOLVED)

**Resolution:** supported. Each entry in `enrichments` becomes its own `enrich_<name>` CTE and its own join in `enhanced_events.joins`. There is no upper bound on the number of enrichments.

For item-level specifically, multiple item-level enrichments share a single unnest+rebuild scaffold (one item-array iteration with all enrichment joins inside) rather than one scaffold per enrichment, to avoid materializing multiple unnested copies of the items array.

### Q12. Default columns from enrichment source (RESOLVED)

**Resolution:** `columns` is required. The user always specifies an explicit list of source columns to add to the output (`columns: ['col1', 'col2']`); the `joinKey` itself isn't included by default since it already exists on the events side.

This aligns with the package's existing column-discipline philosophy — the column order on `enhanced_events` is fixed and curated, and surfacing arbitrary additional columns from external sources without explicit opt-in is risky for downstream stability. Schema drift in the source dim (a new column added upstream) can't silently expand the output table. The cost is one more required field in the config; users who want everything from a small dim explicitly list it.

### Q13. Column-name overlap behavior (RESOLVED)

**Resolution:** when an event-level enrichment column name overlaps an existing column on `enhanced_events`, the package emits `coalesce(enrich_<name>.<col>, <original_expr>) as <col>` so a missed JOIN falls back to the existing value rather than emitting NULL. When there is no overlap, the column is added as the plain `enrich_<name>.<col>`. Coalesce-then-add applies uniformly whether the original column comes from the package's explicit column map (promoted via `eventParamsToColumns`, package-generated like `landing_page`, etc.) or from `event_data` / `session_data` pass-throughs.

**The use case.** Enrichment data is most often intended to fix or supersede an existing column — for example, replacing a promoted `page_title` event parameter with a clean version sourced from a page-metadata table joined on `page_location`. Coalesce is the natural semantics for "fix the data when the mapping exists; otherwise keep the original" and aligns with how users typically think about enrichment joins. Always emitting NULL on a missed JOIN — the previous design — silently degrades the column's coverage and is rarely what the user actually wants.

**Implementation.** At SQL generation time the package:

1. Computes a `preEnrichmentExpressions` map: every column already mapped to its source-qualified expression before enrichment is layered on (entries from `finalColumnOrder`, `itemListOverrides`, and pass-throughs from `event_data` / `session_data`).
2. For each enrichment column `c`:
   - If `c` is in `preEnrichmentExpressions`, emit `coalesce(enrich_<name>.${c}, ${preEnrichmentExpressions[c]}) as ${c}` into the outer `enhanced_events` SELECT — the coalesce-then-add case.
   - Otherwise emit `enrich_<name>.${c} as ${c}` — the additive case.
3. Adds `c` to `alreadyMapped` so the downstream pass-through builder skips the original column from `event_data.*` / `session_data.*` (preventing double-emission).

**Enrichment-vs-enrichment overlap.** When two enrichments target the same column name, the package throws — overlap with package output is intentional (user is fixing a column), but overlap between two enrichments is almost certainly accidental. The error names both enrichments and the conflicting column.

**No opt-out.** There is no `replace: true` (or similar) flag to suppress the coalesce. If a user genuinely wants hard-replace semantics (NULL on missed JOIN), they pre-aggregate or sentinel-fill in their source SQL — and that's a corner case rare enough to not warrant API surface area. The data-enrichments feature is still pre-1.0 (`0.9.0-dev.*`); coalesce-by-default is the simplest API and the right default for the fix-the-data use case the feature was designed for.

**Why this over hard-replace.** A previous version of this resolution had the enrichment value unconditionally REPLACE the existing column, emitting NULL when the JOIN missed. That silently degraded existing column coverage for any row not matched by the dim — a subtle correctness issue that surfaces only in production data. Coalesce-then-add preserves coverage by design.

### Q14. Item-level events filter (RESOLVED)

**Resolution:** reuse `ga4EcommerceEvents` from [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js). Not configurable per enrichment.

GA4 only carries `items` array data on the default ecommerce event names — non-default events have their items dropped at data collection time. The `ga4EcommerceEvents` constant already encodes this list; using a different filter would either miss valid events or include events that can't have items by design. The filter is determined by GA4's behavior, not by per-enrichment intent, so there's no meaningful customization to expose.

### Q15. Supported levels (RESOLVED)

**Resolution:** ship two levels — `event` and `item`.

Mechanically, only these two cases need distinguishing. Session-grained, user-grained, page-grained, and arbitrary-key joins (e.g. on a `company_id` promoted from an event parameter) are all the same operation: a flat `LEFT JOIN` at the event grain on a column of `enhanced_events`. The cardinality intent is carried by the user's choice of `joinKey`. Item-grained joins are the only structurally distinct case, because the join key is nested inside the `items` array.

This design subsumes the "event-level" framing originally listed in the README's Planned Features — that case is just `level: 'event'` with whatever column the user wants to join on.

### Q16. Item-CTE sharing, naming, and generation (RESOLVED)

When item-level enrichments and `itemListAttribution` are configured together, the package needs to choose how the item-array CTEs interact. The package has two item-array CTEs for attribution (named `items_unnested` and `items_rebuilt` since `0.9.0-dev.0`). Item-level enrichments participate in the same unnest+rebuild scaffold.

**Resolution:**

- **CTEs are shared.** Item-level enrichments and item-list-attribution use the same two-CTE scaffold; the package doesn't generate a separate scaffold per item-level enrichment.
- **CTE names** (renamed in `0.9.0-dev.0` to reflect their multi-feature role; previously `item_list_attribution` / `item_list_data`):
  - `items_unnested` (one row per item per event; window functions for attribution applied here when configured; carries `event_date` so date-grained item joins work — see Q18)
  - `items_rebuilt` (re-aggregation with `array_agg(select as struct item.* replace(...))`; enrichment joins applied here)
  - The internal `_item_row_id` join column (renamed from `_item_list_attribution_row_id` for the same reason). Underscored, package-private.
- **Item-level enrichment joins always happen in the last item CTE** (`items_rebuilt`). The LEFT JOIN with `enrich_<name>` is added alongside the `array_agg` re-aggregation; the unnest CTE just produces one row per item without enrichment-specific work.
- **The two-CTE structure is preserved across all configurations** — even when only item-level enrichments are configured (without `itemListAttribution`), the package still emits both CTEs. Consistent structure beats a slightly leaner enrichment-only path; `items_unnested` is just a plain unnest in that case.

**SQL pattern by configuration:**

| Configuration | `items_unnested` does | `items_rebuilt` does |
|---|---|---|
| Neither enabled | not emitted | not emitted |
| Only `itemListAttribution` | unnest + `LAST_VALUE` attribution window | re-aggregate; `replace(...)` for the three attribution fields |
| Only item enrichment | unnest | LEFT JOIN `enrich_<name>`; re-aggregate with `replace(...)` for enrichment columns whose names match existing item fields and additive `, ... as col` for new fields (Q17) |
| Both | unnest + `LAST_VALUE` attribution window | LEFT JOIN `enrich_<name>`; re-aggregate combining attribution `replace(...)` with enrichment `replace(...)` (for overlap) and additive clauses (for new fields) |

Multiple item-level enrichments add multiple `LEFT JOIN enrich_<name> USING (joinKey)` clauses to `items_rebuilt`, with each enrichment's columns folded into the same `array_agg(select as struct item.* replace(...), ...)` block. Each enrichment column is classified as REPLACE or ADD per Q17 based on whether its name is in `GA4_STANDARD_ITEM_FIELDS`.

**Backwards-compat note.** The CTE renames (`item_list_attribution` / `item_list_data` → `items_unnested` / `items_rebuilt`, plus `_item_list_attribution_row_id` → `_item_row_id`) shipped in `0.9.0-dev.0` as a prep sprint ahead of this feature (see [items-cte-prep-sprint.md](items-cte-prep-sprint.md)). They are breaking changes for users referencing these names from `customSteps`; the minor version bump under 0.x semver signals the break per the existing convention from [query-builder-v2](../implemented/query-builder-v2.md). The reserved-names contract in `customSteps` documentation and AGENTS.md was updated at the same time.

### Q17. Item-level column overlap behavior and SQL syntax (RESOLVED)

**Resolution:** for each item-level enrichment column, the package emits BigQuery's `replace(coalesce(<expr>, item.<col>) as <col>)` syntax when the column name matches a field in `GA4_STANDARD_ITEM_FIELDS`, and additive `, <expr> as <col>` syntax otherwise. The coalesce wrap inside `replace(...)` makes a missed item-level JOIN fall back to the existing item field value — symmetric with the event-level coalesce-then-add behavior in [Q13](#q13-column-name-overlap-behavior-resolved).

**The use case.** Item-level enrichment is most often intended to fix or supersede an existing item field — for example, correcting `item_category` values from a category-mapping table joined on `item_id`. Coalesce-then-add lets the user express this naturally by naming the enrichment column the same as the field being fixed; a row whose `item_id` doesn't match in the dim keeps its original `item.item_category` value instead of becoming NULL.

**The underlying SQL constraint.** BigQuery's `select as struct item.* ...` syntax forces a per-column choice the package has to make at SQL-gen time:

- `replace(<expr> as <existing_field>)` — overwrites a field that already exists in `item.*`. Errors at runtime if the field doesn't exist.
- `, <expr> as <new_field>` — adds a new field. Errors at runtime if the field already exists.

The two forms combine in a single struct construction, but the classification has to be correct upfront. Item-list-attribution uses `replace(...)` for the three fields it modifies (`item_list_name`, `item_list_id`, `item_list_index`); item-level enrichment uses `replace(coalesce(<expr>, item.<col>) as <col>)` whenever the enrichment column name matches a standard item field, and additive syntax (no coalesce — no original to fall back to) otherwise.

**Implementation:**

- Add `GA4_STANDARD_ITEM_FIELDS` to [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js), enumerating the standard items struct (`item_id`, `item_name`, `item_brand`, `item_category`, `item_variant`, `item_list_name`, `item_list_id`, `item_list_index`, `price`, `quantity`, `affiliation`, `coupon`, `discount`, `item_revenue`, `promotion_id`, `promotion_name`, `creative_name`, `creative_slot`).
- For each item-level enrichment column at SQL-gen time, classify as REPLACE-WITH-COALESCE (in the standard list — `replace(coalesce(<expr>, item.<col>) as <col>)`) or ADD (not in the list — appends a new field; no coalesce since there's no original to fall back to).
- The package emits a single `select as struct item.* replace(<all replace clauses>), <all add clauses>` per item-level rebuild, combining attribution `replace()` clauses (when active) with enrichment coalesce-wrapped `replace()` clauses for fields in the standard list and additive clauses for everything else.
- Users with customized item structs (extra fields not in the standard list) whose ADD clauses collide with their custom field will see a BigQuery "duplicate column" error at dry-run; they alias in source SQL to resolve.

**Enrichment-vs-enrichment overlap.** When two item-level enrichments target the same column name, the package throws — same logic as event-level Q13. The error names both enrichments and the conflicting column.

**No opt-out** (mirror of Q13): coalesce-then-add applies uniformly with no per-enrichment flag to suppress it. Users wanting hard-replace semantics for item-level can sentinel-fill in source SQL.

**Maintenance.** `GA4_STANDARD_ITEM_FIELDS` is small (~17 fields) and stable. GA4 occasionally adds standard item fields; the list updates with a minor package release. If a future GA4 addition matches a column name a user enrichment is currently adding additively, the next package release would change the SQL from ADD to REPLACE-WITH-COALESCE for that column — which is the intended behavior.

### Q18. Composite join keys (RESOLVED)

**Resolution:** `joinKey` accepts a string (single column) or a non-empty array of strings (composite key). The compiled SQL is `USING(<comma-separated keys>)`. Item-level CTEs propagate `event_date` from `event_data` so item-level enrichments can use date-grained composite keys like `['event_date', 'item_id']`.

**Why composite keys.** A common pattern is joining on date + identifier — for example, `joinKey: ['event_date', 'user_pseudo_id']` for user-grained dim data that varies by day, or `['event_date', 'item_id']` for product-master data that tracks historical pricing or attributes. Single-column joins remain the typical case (and are written as a plain string for ergonomics), but composite keys are common enough to support directly.

**Implementation:**

- Layer 1 validation accepts either `joinKey: 'col'` or `joinKey: ['col1', 'col2', ...]`. The package normalizes to an array internally.
- The compiled `USING` clause is `using(col1, col2, ...)`. Q5's USING enforcement still applies — every key must have the same name on both sides; users alias in source SQL when their dim uses different column names.
- For `level: 'event'`, every key must exist as a column on `enhanced_events`. Layer 2 validation checks each key independently.
- For `level: 'item'`, every key must be a field on the `item` struct OR be `event_date` (the one event-level field propagated through `items_unnested`).
- Source-CTE generation includes all join keys in the source CTE's `select.columns` so the dim CTE has them available for the join.
- `items_unnested` is extended to select `event_date` from `event_data` alongside the existing item-row columns, so date-grained item joins work without further plumbing.

**SQL example — event-level composite key:**

```sql
left join
  enrich_user_segments using(event_date, user_pseudo_id)
```

**SQL example — item-level composite key (date + item_id):**

```sql
items_rebuilt as (
  select
    _item_row_id,
    array_agg(
      (select as struct item.*, enrich_pricing.list_price as list_price)
    ) as items
  from items_unnested
  left join enrich_pricing using(event_date, item_id)
  group by _item_row_id
)
```

The user's source SQL must produce all join-key columns with the same names (per Q5).

### Q19. Auto-generated column descriptions for enrichment columns (RESOLVED)

**Resolution:** the package auto-generates column descriptions for every column added or replaced by an enrichment, noting the enrichment source. The descriptions deep-merge with `dataformTableConfig.columns` per the existing convention, so users can override individual descriptions without losing the others.

**The use case.** When a column on `enhanced_events` came from an external enrichment table, the table's BigQuery schema should make that visible — anyone querying the table or browsing the schema in BigQuery can see "this column came from enrichment X joined on Y" without having to look at the Dataform config.

**Format:**

- **Added column** (no overlap with an existing column): `` Added by enrichment `<name>` (joined on <joinKey> from <source>). ``
- **Replaced column** (overlap with a package column): `` Replaced by enrichment `<name>` (joined on <joinKey> from <source>). Original: <package's original description> ``

`<source>` is rendered as the backtick-quoted table reference (`` `proj.ds.user_cohorts` ``). `<joinKey>` is the column name (or comma-separated list for composite keys per Q18).

**Item-level enrichments** document the modification at the appropriate field of the items struct, using the same wording. The mechanism follows the existing convention the package uses for item-struct fields in [tables/ga4EventsEnhanced/columns/columnDescriptions.json](../../tables/ga4EventsEnhanced/columns/columnDescriptions.json).

**User override.** Users wanting a custom description for a specific enrichment column still use `dataformTableConfig.columns`:

```js
ga4EventsEnhanced.createTable(publish, {
    enrichments: [
        { name: 'cohorts', level: 'event', source: { schema: 'dim', name: 'cohort_dim' }, joinKey: 'user_pseudo_id', columns: ['cohort_label'] },
    ],
    dataformTableConfig: {
        columns: {
            cohort_label: 'User behavioral cohort assigned by the ML pipeline. Updated weekly.',
        },
    },
});
```

The user's description wins via the existing deep-merge in [utils.js](../../utils.js)'s `mergeDataformTableConfigurations`. Auto-generated descriptions only appear for columns the user hasn't documented themselves.

## Solution Design

### Architecture

The change is contained — most of the work is in [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js):

1. **Validation** in [tables/ga4EventsEnhanced/validation.js](../../tables/ga4EventsEnhanced/validation.js): Layer 1 shape validation for the new `enrichments` field, mirroring the `eventParamsToColumns` block.
2. **Default config** in [tables/ga4EventsEnhanced/config.js](../../tables/ga4EventsEnhanced/config.js): `enrichments: []`.
3. **Source-CTE generation** in `_generateEnhancedEventsSQL`: build one `enrich_<name>` step per enrichment entry, prepend to `packageSteps`.
4. **Flat-key join integration** in `_generateEnhancedEventsSQL`: extend `enhancedEventsStep.joins` with one entry per session/user/page enrichment; extend `enhancedEventsStep.select.columns` with the corresponding qualified column references.
5. **Shared item scaffold** in `_generateEnhancedEventsSQL`: emit `items_unnested` and `items_rebuilt` whenever `itemListAttribution` is configured OR at least one item-level enrichment exists. `items_unnested` does the unnest plus attribution windows when present; `items_rebuilt` adds enrichment LEFT JOINs and the `array_agg(...)` re-aggregation combining `replace(...)` for attribution fields with additive `, ... as col` clauses for enrichment columns (Q16, Q17). The CTE renames (`item_list_attribution` / `item_list_data` / `_item_list_attribution_row_id` → `items_unnested` / `items_rebuilt` / `_item_row_id`) shipped separately in `0.9.0-dev.0`. Override `enhancedEventsStep.items` via the existing coalesce pattern.
6. **Standard items field constant** in [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js): add `GA4_STANDARD_ITEM_FIELDS` enumerating the GA4 items struct fields (Q17). Used by item-level enrichment validation.
7. **Layer 2 validation + column-overlap classification** in `_generateEnhancedEventsSQL`: the runtime-derived reserved-name set already picks up the new `enrich_*` CTE names automatically (no code change needed). Event-level enrichment columns are routed to the `select.columns` map and added to the `excludedColumns` set passed to `selectOtherColumns` (Q13). Item-level enrichment columns are classified as REPLACE-or-ADD via `GA4_STANDARD_ITEM_FIELDS` and folded into the items struct construction (Q17). Enrichment-vs-enrichment column collisions throw at this point (Q13 / Q17).

### Source CTE generation (sketch)

For each entry in `enrichments`, generate a step that selects the join key plus requested columns from the source:

```js
const enrichSteps = (mergedConfig.enrichments ?? []).map(e => {
    const cteName = `enrich_${e.name}`;
    const columns = { [e.joinKey]: e.joinKey, ...Object.fromEntries(e.columns.map(c => [c, c])) };
    return {
        name: cteName,
        select: { columns },
        from: typeof e.source === 'string' ? e.source : '${ref(e.source)}',  // ref-object form is resolved later via ctx.ref() inside .query(ctx => ...)
    };
});
```

(Sketch only — actual code resolves `ref` via `setDataformContext`; when `dedupe: true`, the source CTE's `select` is wrapped in a `qualify row_number() over (partition by joinKey) = 1` clause.)

### enhanced_events join extension (sketch)

For each event-level enrichment, push to `joins`:

```js
const eventEnrichments = enrichments.filter(e => e.level === 'event');

enhancedEventsStep.joins.push(...eventEnrichments.map(e => ({
    type: 'left',
    table: `enrich_${e.name}`,
    on: `using(${e.joinKey})`,
})));

eventEnrichments.forEach(e => {
    e.columns.forEach(col => {
        enhancedEventsStep.select.columns[col] = `enrich_${e.name}.${col}`;
    });
});
```

### Shared item scaffold (sketch)

The existing `itemListSteps` block is generalized into the shared `items_unnested` / `items_rebuilt` scaffold per Q16:

- `items_unnested`: emitted whenever `itemListAttribution` OR any `level: 'item'` enrichment is configured. Unnests items from the `ga4EcommerceEvents` filter; adds the attribution `LAST_VALUE` window function when `itemListAttribution` is on.
- `items_rebuilt`: emitted under the same condition. Adds one `LEFT JOIN enrich_<name> USING (joinKey)` per item-level enrichment, then `array_agg(select as struct item.* replace(...))` with one `replace` clause per attribution field (when on) and one per enrichment column.

The most involved part of the implementation. The item-list-attribution code already produces the hard parts (the `LAST_VALUE` over a struct, the deterministic row ID, the coalesce override on `enhanced_events.items`); item-level enrichments are additive transformations inside the same shared scaffold. The CTE renames that prepared this scaffold for multi-feature use shipped in `0.9.0-dev.0`.

## Implementation Phases

The feature ships in two sprints (Sprint A: event-level, Sprint B: item-level) following one prep sprint already shipped. Each phase has a self-contained verification surface, so PRs can be reviewed and verified for one mechanism at a time.

### Phase 0: items-cte prep (shipped in `0.9.0-dev.0`)

Renamed the existing item-array CTEs and helper to neutral multi-purpose names; carried `event_date` through `items_unnested` to enable composite-key item joins. See [items-cte-prep-sprint.md](items-cte-prep-sprint.md). Covered the structural prerequisites from Q16 and Q18 without introducing any new feature behavior.

### Phase 1: Event-level enrichments — Sprint A (proposed)

Ships the event-level slice: any `level: 'event'` enrichment with a flat `LEFT JOIN ... USING(<keys>)` on `enhanced_events`. Most users only need event-level (session-, user-, page-, and custom-key dim joins all use `level: 'event'`), so this phase delivers most of the feature's value while deferring the structurally distinct item-level case.

**In scope:**

- Source-CTE generation at the top of the pipeline (`enrich_<name>` per entry)
- `enrichments` config field with Layer 1 + Layer 2 validation
- Event-level join integration into `enhanced_events`
- Replace-or-add column-overlap behavior (Q13)
- USING enforcement (Q5)
- Composite-key support — works automatically for event-level since `enhanced_events` carries the keys natively (Q18)
- Opt-in `dedupe: true` flag (Q3)
- `enrich_<name>` CTE name prefix (Q6)
- Auto-generated column descriptions for event-level enrichments (Q19, event-level slice)

**Q&As covered:** Q1, Q2 (event-level slice), Q3, Q4 (event-level slice), Q5, Q6, Q7, Q8, Q9, Q11, Q12, Q13, Q15 (event level), Q18 (event-level slice), Q19 (event-level slice).

**Defers to Phase 2:** all `level: 'item'` handling. Layer 1 validation accepts `'item'` as a valid value, but at SQL generation time a clear "item-level enrichments not yet supported in this version" error fires, pointing to Phase 2.

### Phase 2: Item-level enrichments — Sprint B (proposed)

Ships the structurally distinct item-level case: `level: 'item'` enrichments that join inside the items array via the existing `items_unnested` / `items_rebuilt` scaffold from Phase 0.

**In scope:**

- `GA4_STANDARD_ITEM_FIELDS` constant in [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js) (Q17)
- Item-level join integration in `items_rebuilt` — adds LEFT JOINs inside the existing scaffold (Q16 application)
- Replace-or-add column-overlap classification using the standard-fields constant (Q17)
- Item-level auto-descriptions (Q19, item-level extension)
- Removal of Phase 1's "not yet supported" guard

**Q&As covered:** Q2 (item-level slice), Q4 (item-level slice), Q14, Q15 (item level), Q17, Q19 (item-level extension).

**Builds on Phase 1:** all source-CTE generation, validation infrastructure, configuration plumbing, and event-level testing patterns are already in place. Phase 2's diff is contained to the items-array path.

## Files to Modify

| File | Change | Est. LOC |
|---|---|---|
| [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js) | Generate enrichment-source CTEs; extend `enhancedEventsStep.joins` and `select.columns`; refactor item-list scaffold into shared `items_unnested` / `items_rebuilt` (Q16) supporting item-level enrichments; replace-or-add classification for event-level (Q13) and item-level (Q17); auto-generate column descriptions for enrichment columns (Q19) | ~+130 / -10 |
| [tables/ga4EventsEnhanced/validation.js](../../tables/ga4EventsEnhanced/validation.js) | Layer 1 `enrichments` shape validation | ~+50 |
| [tables/ga4EventsEnhanced/config.js](../../tables/ga4EventsEnhanced/config.js) | Default `enrichments: []` | ~+1 |
| [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js) | Add `GA4_STANDARD_ITEM_FIELDS` constant (Q17); rename `itemListAttribution*` helpers in line with the CTE renames if applicable | ~+30 |
| [tests/ga4EventsEnhanced.test.js](../../tests/ga4EventsEnhanced.test.js) | New test cases: each level, multiple enrichments, item-level scaffold | ~+60 (BigQuery dry-run) |
| New `tests/enrichments.test.js` | Pure-Node tests: pipeline shape, source CTE generation, join integration, validation paths, conditional scaffold, item-level collision detection | ~+220 |
| [tests/inputValidation.test.js](../../tests/inputValidation.test.js) | Layer 1 validation cases | ~+50 |
| README, AGENTS | Document `enrichments` config; update reserved-names contract to mention `enrich_*` namespace and the `items_unnested` / `items_rebuilt` rename | ~+50 |

**Estimated total:** ~500 LOC.

## Examples

### Example 1: User-grained cohort labels

```js
ga4EventsEnhanced.createTable(publish, {
    sourceTable: { schema: 'analytics_123', name: 'events_*' },
    enrichments: [
        {
            name: 'cohorts',
            level: 'event',
            source: { schema: 'analytics', name: 'user_cohorts' },
            joinKey: 'user_pseudo_id',
            columns: ['cohort_label', 'lifecycle_stage'],
        },
    ],
});
```

Generates a CTE `enrich_cohorts` selecting `user_pseudo_id`, `cohort_label`, `lifecycle_stage` from the cohorts table. Adds a `LEFT JOIN enrich_cohorts USING (user_pseudo_id)` to `enhanced_events`. The output table gets two new columns: `cohort_label`, `lifecycle_stage`.

### Example 2: Page-grained metadata

```js
enrichments: [
    {
        name: 'page_meta',
        level: 'event',
        source: '`proj.ds.page_metadata`',
        joinKey: 'page_location',
        columns: ['content_group', 'internal_page_owner'],
    },
],
```

The page metadata table must have a `page_location` column (matching the `joinKey`). Pre-aliasing happens in the user's source if their dim uses a different column name.

### Example 3: Item-grained product master

```js
enrichments: [
    {
        name: 'products',
        level: 'item',
        source: { schema: 'dim', name: 'product_master' },
        joinKey: 'item_id',
        columns: ['margin_bucket', 'brand_internal'],
    },
],
```

Reuses the item-list-attribution scaffold. The items array is unnested over the same `ga4EcommerceEvents` filter, joined with `enrich_products` on `item_id`, and re-aggregated. Each item in the output's `items` array now carries `margin_bucket` and `brand_internal` fields. No top-level columns are added to `enhanced_events` — the enrichment lives inside the items struct.

### Example 4: Enrichment on a promoted event parameter

```js
ga4EventsEnhanced.createTable(publish, {
    sourceTable: { schema: 'analytics_123', name: 'events_*' },
    eventParamsToColumns: [
        { name: 'company_id', type: 'int64' },
    ],
    enrichments: [
        {
            name: 'company_dim',
            level: 'event',
            source: { schema: 'dim', name: 'companies' },
            joinKey: 'company_id',
            columns: ['company_name', 'company_segment', 'market_id'],
        },
    ],
});
```

`company_id` is promoted from an event parameter to a column on `enhanced_events` via `eventParamsToColumns`. The enrichment joins on it the same way it would on `session_id` or `page_location` — the only thing that differs is the `joinKey`. The package validates that `company_id` exists as a column on `enhanced_events` at SQL generation time; if it doesn't, a clear error is thrown.

### Example 5: Fix `item_category` values via item-level enrichment

```js
enrichments: [
    {
        name: 'category_fixes',
        level: 'item',
        source: { schema: 'dim', name: 'item_category_overrides' },
        joinKey: 'item_id',
        columns: ['item_category'],   // overlaps existing item-struct field
    },
],
```

`item_category` is in `GA4_STANDARD_ITEM_FIELDS`, so the package emits `replace(enrich_category_fixes.item_category as item_category)` inside the `array_agg(select as struct item.* ...)` re-aggregation. The corrected values from `item_category_overrides` overwrite the original `item_category` on each item; everything else in the items struct is preserved (per Q17).

### Example 6: Fix a promoted `page_title` event parameter

```js
ga4EventsEnhanced.createTable(publish, {
    sourceTable: { schema: 'analytics_123', name: 'events_*' },
    eventParamsToColumns: [
        { name: 'page_title', type: 'string' },   // promoted to a column on enhanced_events
    ],
    enrichments: [
        {
            name: 'clean_titles',
            level: 'event',
            source: { schema: 'dim', name: 'page_title_cleanup' },
            joinKey: 'page_location',
            columns: ['page_title'],   // overlaps the promoted page_title column
        },
    ],
});
```

`page_title` is promoted from an event parameter and exists as an explicit column on `enhanced_events`. The enrichment's `page_title` column has the same name, so the package overrides the column-map entry — the value from `enrich_clean_titles` wins, and the original promoted `page_title` is no longer selected (per Q13). The output table's `page_title` column carries the cleaned values.

### Example 7: Mixed enrichments + customSteps

`enrichments` and `customSteps` coexist. Enrichments run first (their CTEs at the top of the pipeline, joins integrated into `enhanced_events`); `customSteps` run after, building on the enriched `enhanced_events`.

```js
{
    enrichments: [{ name: 'cohorts', level: 'event', joinKey: 'user_pseudo_id', /* ... */ }],
    customSteps: [
        {
            name: 'final',
            query: 'select *, case when cohort_label = \'high_value\' then 1 else 0 end as is_hv from enhanced_events',
        },
    ],
}
```

The `customSteps` query references `cohort_label` (added by the enrichment) as if it were a native column.

## Success Criteria

- [ ] `enrichments` config field accepted and validated
- [ ] Event-level enrichments produce `LEFT JOIN ... USING (joinKey)` on `enhanced_events` for any valid `joinKey` column (`session_id`, `user_pseudo_id`, `page_location`, `eventParamsToColumns`-promoted columns, etc.)
- [ ] Item-level enrichments reuse the item-list-attribution scaffold; items array is correctly enriched
- [ ] Multiple enrichments at each level supported
- [ ] Reserved-name collision check picks up `enrich_*` names automatically
- [ ] Column-name conflicts surface as a clear error at config time
- [ ] No change to generated SQL when `enrichments` is empty/undefined
- [ ] Existing tests pass without modification
- [ ] BigQuery dry-run validates configurations with each level
- [ ] README + AGENTS document `enrichments` and the `enrich_*` namespace

## Testing Strategy

- **Pure-Node unit tests in new `tests/enrichments.test.js`:** pipeline shape (one source CTE per entry, joins integrated, columns added); event-level rendering with various `joinKey` choices (session_id, user_pseudo_id, page_location, a `eventParamsToColumns`-promoted column); item-level scaffold rendering; multiple enrichments at each level; conditional item scaffold (only when at least one item-level enrichment exists); column-conflict throws; missing-column-on-enhanced-events throws for `level: 'event'`.
- **Layer 1 validation tests in `tests/inputValidation.test.js`:** non-array, null entry, missing required fields (`name`, `level`, `source`), invalid `level` value, name uniqueness within `enrichments`.
- **BigQuery dry-run in `tests/ga4EventsEnhanced.test.js`:** generated SQL is syntactically valid for each level type. Item-level scaffold tested for whitespace-equivalence to a known-good output.
- **Integration test:** Dataform compile + dry-run with a representative multi-enrichment config.

## Non-Goals

- **Cross-package enrichment composition.** Enrichments target the `ga4EventsEnhanced` table only. Future table modules can adopt a parallel `enrichments` field.
- **Custom traffic-source attribution as a special enrichment.** Listed in Planned Features as a separate feature; will get its own design doc when scoped.
- **Reverse-direction joins** (events → dim, e.g. updating a dim table from event data). Out of scope; that's a separate downstream Dataform model concern.
- **Schema-aware struct joining.** `joinKey: 'page.path'` syntax — out of scope. Users pre-flatten in their source SQL.
- **Rich enrichment metadata in column descriptions** (beyond the auto-generated "Added/Replaced by enrichment" prefix in Q19). Things like enrichment freshness, lineage diagrams, or data-quality notes are out of scope; users with richer documentation needs use `dataformTableConfig.columns` to override.
- **Auto-aggregation of user source SQL.** If users need `group by` + `any_value()` aggregation in their source, they write it themselves. The package only offers opt-in `dedupe` (per Q3 if resolved that way).

## Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Non-unique join keys in user dim tables silently multiply rows | High | Q3 — opt-in dedupe (`dedupe: true`) wraps the source CTE in a `qualify row_number() = 1`. Document the uniqueness requirement clearly in the `enrichments` config field description. Users with strict needs pre-aggregate in their source SQL. |
| Shared item-CTE scaffold breaks existing item-list-attribution behavior | Med | The two-CTE shape is preserved; existing item-list-attribution dry-run tests cover the attribution-only case unchanged (modulo CTE renames). New tests cover enrichment-only and combined attribution + enrichment cases. CTE renames (`item_list_*` → `items_*`) ship with v0.9.0 as a documented breaking change. |
| Enrichment silently replaces an existing column the user didn't realize had a matching name | Med | Q13 / Q17 — replace-or-add is the intended behavior, but the user can still be surprised. Document clearly in the `enrichments` config field description: any enrichment column whose name matches an existing column REPLACES it. Users wanting strictly additive behavior pick column names that don't collide. The naming convention (`enrich_<name>` CTE prefix) makes it easy to scan generated SQL and see what each enrichment contributes. |
| Two enrichments target the same column name | Low | Q13 / Q17 — throw at SQL-gen time with a clear error naming both enrichments and the column. |
| `GA4_STANDARD_ITEM_FIELDS` constant goes stale when GA4 adds new standard item fields | Low | Small static list (~17 fields) maintained in `helpers/ga4Transforms.js`; updates with a minor package release. If a future GA4 addition matches a column name a user enrichment is currently adding additively, the next package release would change the SQL from ADD to REPLACE — which is the intended behavior. |
| `page.path` joining surprises users (struct field, can't `USING` directly) | Low | Document explicitly. Users pre-flatten in source SQL. |
| `enrich_*` namespace collides with a future internal CTE name | Low | The runtime-derived reserved set picks up `enrich_*` names from `enrichments` config; future internal names just need to avoid the `enrich_` prefix. AGENTS already documents the stable-contract convention. |
| Config validation grows large | Low | Mirror the existing `eventParamsToColumns` validation block. ~50 LOC is consistent with similar fields. |

## References

- [design_docs/implemented/query-builder-v2.md](../implemented/query-builder-v2.md) — v2 step shape this feature builds on
- [design_docs/implemented/custom-ctes.md](../implemented/custom-ctes.md) — Layer 1 / Layer 2 validation pattern, reserved-names contract, the `customSteps` integration point this feature compiles into
- [design_docs/implemented/item-list-attribution.md](../implemented/item-list-attribution.md) — the item-array unnest+rebuild scaffold reused for item-level enrichments
- [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js) — primary file modified; `_generateEnhancedEventsSQL` is the integration point
- [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js) — `itemListAttributionRowId`, `ga4EcommerceEvents` reused for item-level scaffold
- [utils.js](../../utils.js) — `isDataformTableReferenceObject` reused for source-format detection

## Future Work

- **Custom column descriptions for enrichment columns** — optional config field to attach descriptions to user-added enrichment columns, surfacing in the table schema. Same shape as the parallel future-work item in `custom-ctes.md`.
- **Per-enrichment events filter** — `eventsFilter` field for item-level enrichments that need a different event subset than `ga4EcommerceEvents`. Defer until a real use case appears.
- **Source as raw SQL** — accept a raw SQL query as `source` (in addition to refs and strings) for users who need pre-filtering or aggregation. Defer until validated need.
- **Auto-aggregation helper** — opt-in `aggregateBy: 'any_value' | 'last' | 'first'` field that wraps the source CTE in a `group by joinKey` aggregation. More structured than `dedupe: true`; defer if `dedupe` covers the common case.
- **Schema-aware struct join keys** (`joinKey: 'page.path'`) — automatic flattening of nested join keys. Defer until a recurring need.
- **Cross-table enrichment reuse** — when other table modules ship, lift `enrichments` to a shared utility. Premature today; revisit when there's a second consumer.

---

**Document created**: 2026-05-06
**Last updated**: 2026-05-06
