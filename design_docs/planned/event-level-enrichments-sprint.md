# Sprint Plan: Event-Level Data Enrichments (Sprint A)

## Summary

Ships Phase 1 of the data-enrichments feature per [data-enrichments.md](data-enrichments.md): the event-level slice. Adds the `enrichments` config field with Layer 1 + Layer 2 validation, source-CTE generation at the top of the pipeline, event-level join integration on `enhanced_events`, replace-or-add column-overlap behavior (Q13), opt-in `dedupe: true` flag (Q3), composite-key support (Q18), `enrich_<name>` CTE prefix (Q6), and auto-generated column descriptions (Q19). Item-level enrichments (`level: 'item'`) are validated as a config shape but throw "not yet supported" at SQL generation time, deferring to Sprint B.

**Duration:** 2 days
**Dependencies:** items-cte-prep (`0.9.0-dev.0`, shipped)
**Risk Level:** Medium — replace-or-add wildcard mechanics are non-trivial; existing dry-run tests + new direct unit tests catch regressions

## Why ship this first (vs. bundling with item-level)

Per the [Implementation Phases section in data-enrichments.md](data-enrichments.md#implementation-phases), event-level vs. item-level have very different mechanical complexity (flat LEFT JOIN at `enhanced_events` vs. unnest+rebuild scaffold inside `items_rebuilt`). Splitting them gives:

- A reviewable PR diff scoped to one mechanism per sprint
- Most users only need event-level — session/user/page/custom-key dim joins all use `level: 'event'`, so this phase delivers ~80% of the feature's value
- Item-level (Sprint B) builds on shared infrastructure (validation, source-CTE generation, replace-or-add patterns) shipped here

## Current Status Analysis

### Completed Recently

- **items-cte-prep** (this week): ~50 LOC, single session — CTE rename + `event_date` propagation
- **Custom CTEs sprint**: ~190 LOC, single session — closest comparable in feature scope
- **Query Builder v2 sprint**: ~250 LOC, single session — closest comparable in implementation depth

### Velocity

- Recent feature sprints: 200–250 LOC/day
- Pure-refactor sprints: 30–50 LOC/day
- This sprint sits at v2/custom-ctes complexity. ~350 LOC across implementation + tests + docs in 2 days = realistic with a buffer.

### Q&As covered (from data-enrichments.md)

| Q | Topic |
|---|---|
| Q1 | Pipeline placement — enrichment-source CTEs at top |
| Q2 (event slice) | Join location at `enhanced_events` |
| Q3 | Opt-in `dedupe: true` |
| Q4 (event slice) | `joinKey` for event-level columns |
| Q5 | USING enforcement |
| Q6 | `enrich_<name>` CTE prefix |
| Q7 | `enrichments` config field name |
| Q8 | Per-entry shape (with item-level acceptance for deferral) |
| Q9 | Source format (Dataform ref or backtick string) |
| Q11 | Multiple enrichments per level |
| Q12 | Required `columns` list |
| Q13 | Replace-or-add column-overlap behavior |
| Q15 (event) | `level: 'event'` mechanism |
| Q18 (event slice) | Composite-key joins (works automatically since `enhanced_events` carries the keys) |
| Q19 (event slice) | Auto-generated column descriptions |

Item-level Q&As (Q14, Q17, item slices of Q2/Q4/Q15/Q19) are deferred to Sprint B.

## Proposed Milestones

Three milestones, single PR. Each has a self-contained verification surface.

### M1: Config + validation + source-CTE generation + deferral path

**Goal:** Add the `enrichments` config field with default `[]`, Layer 1 shape validation, and source-CTE generation at the top of the pipeline. The Layer 2 collision-with-package-names check picks up the new `enrich_*` names automatically (the reserved set is already runtime-derived from `packageSteps`). Item-level enrichments are validated as a config shape but throw "not yet supported" at SQL generation time, deferring to Sprint B.

**Estimated:** ~180 LOC (~120 impl + ~60 tests)
**Duration:** 0.7 days

**Tasks:**

- [tables/ga4EventsEnhanced/config.js](../../tables/ga4EventsEnhanced/config.js): add `enrichments: []` to the default config
- [tables/ga4EventsEnhanced/validation.js](../../tables/ga4EventsEnhanced/validation.js): Layer 1 shape validation
  - `enrichments` is array (when present)
  - Each entry is a non-null object
  - `name` is non-empty unique string within `enrichments`
  - `level` (when present) is `'event'` or `'item'`
  - `source` is a Dataform table reference object or backtick-quoted string (reuse `isDataformTableReferenceObject` and the `sourceTable` regex)
  - `joinKey` is required, non-empty string OR non-empty array of non-empty strings (Q18)
  - `columns` is required, non-empty array of strings (Q12)
  - `dedupe` (when present) is boolean
- [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js):
  - Generate one `enrich_<name>` step per enrichment entry, prepended to `packageSteps` (Q1, Q6)
  - Source CTE selects `joinKey` columns + requested `columns` from `source` (Q9)
  - When `dedupe: true`, wrap the source CTE's `select` in `qualify row_number() over (partition by <joinKey>) = 1` (Q3)
  - Layer 2 deferral: when any enrichment has `level: 'item'`, throw with `config.enrichments[<i>] uses level: 'item', which is not yet supported in this version. Item-level enrichments will ship in a future release; see design_docs/planned/data-enrichments.md (Sprint B).`
- New `tests/enrichments.test.js` (assert-based, matches `queryBuilder.test.js` / `customSteps.test.js` style):
  - Source-CTE rendering for each `source` format (Dataform ref + backtick string)
  - `dedupe: true` wraps in `qualify row_number()`
  - `dedupe: false` / omitted produces no `qualify`
  - Composite `joinKey` selects multiple columns in the source CTE
  - Item-level deferral throws with the exact error string (so users can grep for it)
  - `enrich_<name>` collision with reserved `event_data` / `session_data` / `enhanced_events` throws (the existing reserved-set check picks this up automatically; verify it does)
- [tests/inputValidation.test.js](../../tests/inputValidation.test.js): Layer 1 cases — non-array, null entry, missing required fields, invalid level, duplicate names within enrichments, non-string joinKey, empty columns array
- Wire `tests/enrichments.test.js` into `npm test` and `tests/testRunner.js`

**Acceptance Criteria:**

- [ ] All 9 existing test suites pass without modification
- [ ] `enrichments` config field accepted; default `[]` preserves existing behavior
- [ ] Source CTE renders correctly for both Dataform ref and backtick-string sources
- [ ] Validation throws with clear messages for: non-array, null entry, missing required fields, invalid level, duplicate names, non-empty array constraints
- [ ] `level: 'item'` config validates at Layer 1 but throws "not yet supported" at SQL gen with a pointer to Sprint B
- [ ] `tests/enrichments.test.js` wired into `npm test` and `tests/testRunner.js`

**Risks:**

- The runtime-derived reserved-name set should automatically include `enrich_*` CTE names since they're added to `packageSteps`. Mitigation: explicit test case verifying this.
- `dedupe: true` with composite `joinKey` needs `partition by col1, col2`. Mitigation: tested explicitly.

### M2: Event-level join integration + replace-or-add + composite keys

**Goal:** For each event-level enrichment, add the `LEFT JOIN` to `enhanced_events.joins` with `USING(<keys>)`. Apply replace-or-add semantics per Q13: column added to `select.columns` map (overrides explicit columns automatically via JS object semantics) AND added to `excludedColumns` for the wildcards (suppresses overlap with default GA4 columns from `event_data.*` / `session_data.*`). Composite keys compile to multi-column USING. Throws on enrichment-vs-enrichment column collisions.

**Estimated:** ~150 LOC (~80 impl + ~70 tests)
**Duration:** 0.7 days
**Dependencies:** M1

**Tasks:**

- [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js):
  - For each event-level enrichment: append `{ type: 'left', table: 'enrich_<name>', on: 'using(<keys>)' }` to `enhancedEventsStep.joins` (Q2)
  - For each enrichment column: add to `enhancedEventsStep.select.columns` mapping (`<col>: 'enrich_<name>.<col>'`) — JS object semantics override explicit-column entries naturally (Q13)
  - For each enrichment column: add to the `excludedColumns` set passed to `selectOtherColumns` for both `event_data.*` and `session_data.*` (Q13 — handles wildcard-column overlap)
  - Composite-key support: when `joinKey` is an array, normalize to `using(col1, col2, ...)` (Q18 event-level slice)
  - Enrichment-vs-enrichment column collision: track all enrichment column names; throw with `config.enrichments[<i>].columns conflicts with config.enrichments[<j>].columns on column '<name>'` if two enrichments target the same column name
- `tests/enrichments.test.js` cases:
  - Single enrichment with single-column joinKey
  - Multiple enrichments
  - Composite key (array `joinKey` compiles to `USING(col1, col2)`)
  - Replace explicit column (e.g. an enrichment column matching a promoted `eventParamsToColumns` column)
  - Replace wildcard column (e.g. an enrichment column matching a default GA4 column from `event_data.*`)
  - `dedupe: true` combined with replacement
  - Enrichment-vs-enrichment column collision throws with offender named
  - Pure additive case (enrichment column doesn't match anything) works

**Acceptance Criteria:**

- [ ] Event-level enrichments compile to `LEFT JOIN enrich_<name> USING(<keys>)` on `enhanced_events`
- [ ] Composite-key enrichments compile to `USING(col1, col2)` correctly
- [ ] Replace-or-add: explicit columns overridden by enrichment values; wildcard columns excluded; new columns added
- [ ] Enrichment-vs-enrichment column collisions throw with clear error naming both enrichments and the column
- [ ] Generated SQL passes BigQuery dry-run for representative configs

**Risks:**

- Wildcard-column replacement requires correct interaction with `selectOtherColumns`. Mitigation: explicit test for the wildcard case using a known default GA4 column.
- Composite keys must produce valid `USING(col1, col2)` syntax. Mitigation: BigQuery dry-run validates.

### M3: Auto-generated column descriptions + docs + final test pass

**Goal:** Auto-generate column descriptions per Q19 format. Document `enrichments` in README and AGENTS. Run final test suite.

**Estimated:** ~55 LOC (~30 impl + ~25 docs)
**Duration:** 0.5 days
**Dependencies:** M1, M2

**Tasks:**

- [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js): for each enrichment column, generate a description and inject into the columns object passed to Dataform's table config:
  - Added column: `Added by enrichment '<name>' (joined on <joinKey> from <source>).`
  - Replaced column: `Replaced by enrichment '<name>' (joined on <joinKey> from <source>). Original: <original description>`
  - `<source>` rendered as the backtick-quoted final form (e.g. `` `proj.ds.user_cohorts` ``)
  - For composite `joinKey`: comma-separated column list
  - The existing deep-merge in `mergeDataformTableConfigurations` makes `dataformTableConfig.columns` overrides win automatically
- [README.md](../../README.md): new `enrichments` section under Configuration Object with a worked example (similar in scale to the customSteps section); update reserved-names contract table to mention the `enrich_*` namespace
- [AGENTS.md](../../AGENTS.md): brief note about the `enrich_<name>` namespace convention
- `tests/enrichments.test.js`: description-generation cases (added column, replaced explicit column, replaced wildcard column); user-override-via-`dataformTableConfig.columns` wins case
- Run `npm run test:summary` — all 10 suites green

**Acceptance Criteria:**

- [ ] Auto-generated descriptions appear for added and replaced enrichment columns
- [ ] User-supplied `dataformTableConfig.columns` overrides win over auto-generated descriptions
- [ ] README documents `enrichments` with at least one worked example
- [ ] README's reserved-names contract mentions `enrich_<name>` namespace
- [ ] AGENTS notes the `enrich_` prefix convention
- [ ] Final test suite green (~420+ tests across 10 suites)

**Risks:**

- Description generation might surprise users (silent metadata appearing in BigQuery schema). Mitigation: documented clearly; user override via `dataformTableConfig.columns` is the existing escape hatch.

## Success Metrics

- All existing tests pass without modification (regression check)
- `tests/enrichments.test.js` covers source CTE generation, joins, replace-or-add, composite keys, dedupe, descriptions, and the item-level deferral path (≥20 test cases)
- Generated SQL for a representative event-level enrichment config validates via BigQuery dry-run
- `level: 'item'` config throws at SQL gen with a clear "not yet supported" error pointing to Sprint B
- README + AGENTS document the new config field
- Single PR landed with three commits (or a squash); version stays on `0.9.0-dev.x` (no version bump in this sprint — the breaking change already shipped in `dev.0`)

## Dependencies

- None external. M2 depends on M1; M3 depends on M1 + M2.

## Open Questions

None — all four resolved before sprint planning:

| # | Resolution |
|---|---|
| 1 | Sprint branch: `sprint/event-level-enrichments`, branched from `main`. |
| 2 | Description format renders `<source>` in backtick-quoted final form (e.g. `` `proj.ds.user_cohorts` ``); composite `joinKey` renders as comma-separated. |
| 3 | `tests/enrichments.test.js` uses the existing assert-based pattern matching `queryBuilder.test.js` / `customSteps.test.js`. |
| 4 | Item-level deferral error: `config.enrichments[<i>] uses level: 'item', which is not yet supported in this version. Item-level enrichments will ship in a future release; see design_docs/planned/data-enrichments.md (Sprint B).` |

## Notes

- The Implementation Phases section in [data-enrichments.md](data-enrichments.md#implementation-phases) is the authoritative scope reference for this sprint.
- `customSteps.test.js`'s reserved-name collision tests already cover the `enrich_*` namespace expansion automatically (the reserved set is runtime-derived from `packageSteps`, so adding source CTEs to that array makes their names auto-reserved). Verifying this works via a new explicit test in `enrichments.test.js`.
- `getColumnDescriptions` in [documentation.js](../../documentation.js) is the existing hook for column descriptions — Q19 extends it; the deep-merge with `dataformTableConfig.columns` for user overrides is already in place via `mergeDataformTableConfigurations`.
- Integration tests (`npm run test:integration`) install from the npm registry and only validate after publication — not a development-time gate.
- This sprint enables Sprint B's work without coupling event-level and item-level mechanics in one diff.

---

**Document created**: 2026-05-08
**Last updated**: 2026-05-08
