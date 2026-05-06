# Sprint Plan: Query Builder Formatting

## Summary
Refactor `queryBuilder()` in `utils.js` to produce properly indented SQL output with 2-space indentation, CTE body indentation, and no spurious blank lines. Whitespace-only change — no semantic SQL changes.

**Duration:** 0.5 days
**Dependencies:** None
**Risk Level:** Low

## Current Status Analysis

### Completed Recently
- Daily quality assertion fix (CASE to UNNEST approach): ~30 LOC in 1 day
- Item list attribution split into two CTEs: ~40 LOC in 1 day
- Assertion sourceTable input validation: ~20 LOC in 1 day

### Velocity
- Recent average: ~30-50 LOC/day for utility-level changes
- This sprint: ~30 net LOC — well within a half-day

### Remaining from Design Doc
- All work is in a single file (`utils.js`), single function (`queryBuilder`)
- Prototype was already built and validated — all tests passed

## Proposed Milestones

### Milestone 1: Add re-indentation utilities and refactor columnsToSQL
**Goal:** Add `reindent` and `indentBlock` helper functions inside `queryBuilder`. Refactor `columnsToSQL` to apply `reindent` to multi-line column values and use 2-space indent for the column join separator.
**Estimated:** ~15 LOC
**Duration:** 0.25 days

**Tasks:**
- Add `reindent(sql, targetIndent)` function that normalizes continuation line indentation
- Add `indentBlock(sql, spaces)` function that shifts an entire SQL block right
- Refactor `columnsToSQL` to apply `reindent(entry, INDENT)` to each column entry
- Change column join from `,\n    ` (4 spaces) to `,\n` + pad (2 spaces)

**Acceptance Criteria:**
- [ ] `reindent` preserves first line, normalizes continuation lines to target indent
- [ ] `reindent` is a no-op for single-line values
- [ ] `indentBlock` shifts all non-empty lines right by the given number of spaces
- [ ] Multi-line column values (e.g., `fixEcommerceStruct`, `aggregateValue`) have continuation lines aligned to column indent depth
- [ ] All existing tests pass

**Risks:**
- `reindent` edge cases with unusual helper indentation — Mitigation: helpers use consistent patterns; dry-run tests catch syntax errors

### Milestone 2: Refactor selectSQL and CTE assembly
**Goal:** Refactor `selectSQL` to a parts-based approach (eliminating spurious blank lines) and wrap CTE bodies with `indentBlock` for proper indentation. Change all clause indentation to 2 spaces.
**Estimated:** ~15 LOC
**Duration:** 0.25 days
**Dependencies:** M1 (uses `reindent` and `indentBlock`)

**Tasks:**
- Refactor `selectSQL` to build a `parts` array instead of a template literal with empty interpolations
- Apply `reindent` to `where` clause values
- Change `from`, `left join`, `where`, `group by` clause indentation to 2 spaces
- Refactor CTE wrapping: use `indentBlock(selectSQL(step), INDENT)` for CTE bodies
- Change CTE format from `name as (select...)` to `name as (\n  select...\n)`
- Change CTE join from `,\n    ` to `,\n`
- Visual inspection of generated SQL output with representative config
- Run full test suite

**Acceptance Criteria:**
- [ ] CTE bodies are indented inside their parentheses (keywords at indent 2, columns at indent 4)
- [ ] No spurious blank lines from absent `where`/`groupBy`/`leftJoin` clauses
- [ ] Indent size is 2 spaces throughout
- [ ] No line breaks are added or removed — only indentation changes
- [ ] All existing tests pass (BigQuery dry-run validation)

**Risks:**
- None significant — the prototype already validated this approach

## Success Metrics
- All existing tests passing (BigQuery dry-run validation across all configurations)
- Visual inspection confirms proper indentation for a full enhanced events query with item list attribution, session params, and event params to columns

## Dependencies
- None

## Open Questions
- None — approach was prototyped and validated

## Notes
- The prototype was already built and tested during the design phase, so implementation is a matter of re-applying the known-good changes
- Helper functions are not modified — only `queryBuilder` in `utils.js` changes
- Assertion SQL generators (e.g., `dailyQuality`, `itemRevenue`) use string templates, not `queryBuilder`, so they are unaffected
