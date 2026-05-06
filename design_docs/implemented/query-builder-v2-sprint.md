# Sprint Plan: Query Builder v2

## Summary
Refactor `queryBuilder` in `utils.js` from v1 method-shaped step config to v2 SQL-shaped clause registry with dual step shapes (structured `{name, select, from, ...}` and raw `{name, query}`). Migrate the one internal caller, add direct unit tests, ship as a single PR with version bump 0.7.1 → 0.8.0.

**Duration:** 2.5 days
**Dependencies:** None — design doc fully specified ([query-builder-v2.md](design_docs/planned/query-builder-v2.md))
**Risk Level:** Medium-Low — whitespace/semantic regression of `ga4EventsEnhanced` SQL is the main concern; existing dry-run suite catches it.

## Current Status Analysis

### Completed Recently
- Item list attribution row id fix (most recent): ~10 LOC, 1 day
- Query Builder Formatting (closest precedent — same file): ~30 LOC, 0.5 days
- Separate merge from SQL generation: ~30 LOC, ~0.5 days
- Item list attribution feature: ~280 LOC across implementation + tests, ~3 days

### Velocity
- 30-day window: 6,329 lines added / 1,473 deleted
- 14-day window: 43 added / 13 deleted (recent slowdown)
- Comparable utility-level sprints land at 30–50 LOC/day implementation, 100+ LOC/day for test files
- Estimated capacity for this sprint: ~250 LOC across 2.5 days, comfortable

### Remaining from Design Doc
- M1: `utils.js` refactor — `~+80 / -40` LOC
- M2: `tables/ga4EventsEnhanced/index.js` migration — `~+15 / -15` LOC
- M3: New `tests/queryBuilder.test.js` (~150 LOC) + version bump

## Proposed Milestones

All three milestones ship in a **single PR** to avoid a transitional state where v2 dispatcher exists but the caller is still v1.

### Milestone 1: queryBuilder v2 dispatcher + clause registry
**Goal:** Replace `selectSQL` with a top-level `renderStep` that dispatches on raw `{name, query}` vs. structured shape. Build the ordered `CLAUSE_RENDERERS` registry. Reuses existing `reindent` / `indentBlock` / `columnsToSQL` unchanged.
**Estimated:** ~80 LOC implementation
**Duration:** 1 day

**Tasks:**
- Add `CLAUSE_RENDERERS` array (declaration order = canonical SQL order, per Q2): `select` → `from` → `joins` → `where` → `group by` → `having` → `qualify` → `order by` → `limit`
- Implement `renderInline(keyword)` → `${keyword}\n${pad}${reindent(value, INDENT)}`
- Implement `renderSelect(value)` — string form (sugar for `{sql}`) and `{columns, sql}` object form; column-list join uses `,\n<indent>` separator
- Implement `renderJoins(value)` — array form `[{type, table, on}]` (renders as `<type> join <table> <on>`, `cross` omits `on`) and string fallback
- Implement `renderStep(step)` — branches on `'query' in step`; raw branch returns `reindent(step.query, 0)`; structured branch iterates registry
- Strict-whitelist validation, branched on shape (Q1):
  - Raw step: only `name` + `query` allowed; throw on any other key
  - Structured step: only `name` + clause-registry keys allowed; throw on unknowns
  - Both error messages name the offender and list the valid set
- Update JSDoc on `queryBuilder` ([utils.js:21-34](utils.js#L21-L34)) for the v2 step shape
- Keep `reindent`, `indentBlock`, `columnsToSQL`, and CTE wrapping unchanged

**Acceptance Criteria:**
- [ ] Structured step with all clauses present renders in canonical order (registry-driven)
- [ ] Raw step `{name, query}` emits body verbatim, reindented to insertion column
- [ ] Unknown structured-step key throws with named offender + valid set listed
- [ ] Mixing raw + structured keys in one step throws with a "shapes are mutually exclusive" message
- [ ] Empty/absent clauses produce no output and no spurious blank lines
- [ ] Helper function references (`reindent`, `indentBlock`, `columnsToSQL`) untouched

**Risks:**
- Dual-shape branching adds new logic absent in v1 — Mitigation: covered by M3 unit tests, plus structural review during implementation.
- The QB-FMT formatting rules (CTE indent, no spurious blank lines) need preserving — Mitigation: visual inspection on full `ga4EventsEnhanced` SQL during M2; existing dry-run tests assert SQL semantics.

### Milestone 2: Migrate `ga4EventsEnhanced` to v2 step shape
**Goal:** Rewrite the four step objects in `tables/ga4EventsEnhanced/index.js` to v2 structured shape. Confirm dry-run + integration tests pass with no semantic SQL change.
**Estimated:** ~15 LOC churn (`+15 / -15`)
**Duration:** 0.5 days
**Dependencies:** M1

**Tasks:**
- `eventDataStep` ([index.js:201-248](tables/ga4EventsEnhanced/index.js#L201-L248)): wrap `columns` in `select: { columns: ... }`; `where` keyword stays as-is
- `sessionDataStep` ([index.js:251-265](tables/ga4EventsEnhanced/index.js#L251-L265)): wrap `columns`; rename `groupBy: ['session_id']` → `'group by': 'session_id'`
- Item list attribution steps ([index.js:270-307](tables/ga4EventsEnhanced/index.js#L270-L307)): wrap `columns`; rename `groupBy`
- `finalStep` ([index.js:319-360](tables/ga4EventsEnhanced/index.js#L319-L360)): wrap `columns`; rewrite `leftJoin: [{table, condition}]` → `joins: [{type: 'left', table, on}, ...]`
- Run `npm test` — all 7 suites pass with no test modifications
- Run `npm run test:integration` — full suite passes
- Visual inspection: generate full enhanced events SQL with representative config (item list attribution enabled, session params, event params to columns), confirm whitespace-equivalent to v1 output

**Acceptance Criteria:**
- [ ] All 7 existing test suites pass without modification
- [ ] Integration test passes
- [ ] Generated SQL is semantically identical to v1 (BigQuery dry-run validates)
- [ ] Visual inspection confirms whitespace and indentation match the QB-FMT rules

**Risks:**
- A clause key typo during migration silently emits wrong SQL — Mitigation: M1's strict whitelist throws on typos at config time.
- Helper output indentation could shift if `renderSelect` differs subtly from v1 `columnsToSQL` invocation — Mitigation: `renderSelect` calls `columnsToSQL` with the same args; existing dry-run tests catch any semantic drift.

### Milestone 3: Direct unit tests + version bump
**Goal:** New `tests/queryBuilder.test.js` exercising v2 directly (currently only indirect coverage via `ga4EventsEnhanced.test.js`). Wire into npm scripts. Bump to 0.8.0.
**Estimated:** ~150 LOC tests + ~5 LOC config
**Duration:** 1 day
**Dependencies:** M1, M2

**Tasks:**
- Create `tests/queryBuilder.test.js`:
  - **Structured shape:** each clause renders alone; combinations render in canonical order regardless of input key order; multi-line clause values reindent; absent clauses emit no blank lines; CTE wrapping correct for >1 step
  - **Raw shape:** `{name, query}` emits body verbatim with `reindent`; mixes correctly with structured steps in a `steps` array
  - **`joins` clause:** array form with mixed types renders in array order; `cross` omits `on`; string fallback works; multiple joins of same type render correctly
  - **`select` clause:** string form (e.g. `select: '*'`); object form with `columns`; object form with `sql` only; combined `columns + sql`; `[sql]<id>` prefix preserves no-alias semantics; `key === value` skips alias; `undefined` values filtered
  - **Validation:** unknown structured-step key throws (assert message format includes bad key + valid set); raw + structured mix in one step throws; both messages match expected pattern
- Add `node tests/queryBuilder.test.js` to the `test` script in [package.json](package.json)
- Bump `package.json` version `0.7.1` → `0.8.0`
- Spot-check [README.md](README.md) and [AGENTS.md](AGENTS.md) for any references to v1 step shape; update only if found (the v1 shape was internal)

**Acceptance Criteria:**
- [ ] New test file has ≥20 distinct test cases covering both shapes + validation
- [ ] All test suites green via `npm test`
- [ ] `package.json` version is `0.8.0`
- [ ] README/AGENTS spot-check completed; any v1-shape references updated

**Risks:**
- Test scope creep — Mitigation: stick to the cases enumerated above; defer exhaustive helper-output indentation tests since QB-FMT already covered those indirectly.

## Success Metrics
- All existing tests pass without modification (regression check)
- New `tests/queryBuilder.test.js` covers both shapes + validation paths (≥20 test cases)
- Generated SQL whitespace-equivalent before/after migration for `ga4EventsEnhanced`
- `package.json` at `0.8.0`
- Single PR landed with three commits (M1, M2, M3) or a squash; up to the author

## Dependencies
- None external. M2 depends on M1; M3 depends on M1 + M2.

## Open Questions
None — all design questions resolved in the design doc. Implementation can start.

## Notes
- Design doc estimate was 1.5 days; this sprint plan budgets 2.5 days to absorb dual-shape validation work, full unit-test file authoring, and migration verification — matches recent comparable refactor velocity.
- Helpers (`helpers/*.js`) are not modified; only `utils.js` and `tables/ga4EventsEnhanced/index.js` change in production code.
- The QB-FMT sprint already established that whitespace-equivalent `queryBuilder` changes can ship safely behind the dry-run suite — same regression-proof harness applies here.
- This repo doesn't use a CHANGELOG.md; version history lives in git tags. The `0.8.0` tag annotation should mention the breaking shape change to the exported `queryBuilder` and link to the design doc.
- Custom CTEs and data enrichments (Future Work in the design doc) are explicitly out of scope for this sprint.

---

**Document created**: 2026-05-06
**Last updated**: 2026-05-06
