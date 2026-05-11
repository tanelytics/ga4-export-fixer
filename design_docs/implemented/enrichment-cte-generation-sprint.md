# Sprint Plan: Enrichment CTE Generation Utility

## Summary

Extract the inline ~60-line enrichment generation block at [tables/ga4EventsEnhanced/index.js:323-381](../../tables/ga4EventsEnhanced/index.js#L323-L381) into a reusable `utils.buildEnrichments(enrichments)` utility, so future modeled-table modules can consume enrichments with a single call. Pure refactor: same logic, same outputs, same error messages — relocated.

**Duration:** Single session (~1.5 hours)
**Dependencies:** Sprint A (event-level-enrichments) shipped in `0.9.0-dev.2`; `event-data-explicit-columns` shipped through `0.9.0-dev.6`. No active blockers.
**Risk Level:** Trivially low
**Design doc:** [enrichment-cte-generation.md](enrichment-cte-generation.md) — all four design questions RESOLVED, no open questions remain.

## Current Status Analysis

### Completed Recently (most recent comparable refactors)

| Sprint / commit | LOC | Sessions |
|---|---|---|
| `buildPassThroughs` extraction (M3 of event-data-explicit-columns sprint) | +161 / −18 | 1 (part of larger sprint) |
| `event-data-explicit-columns` M2: collapse enrichment EXCEPT filter | +6 / −12 | 1 |
| Sprint A: event-level data enrichments | +1249 / −16 | 1 |

### Velocity

- **Comparable refactor:** the `buildPassThroughs` extraction shipped in `0.9.0-dev.3` (commit `5e91242`) is the closest precedent — same shape (extract utility + add unit tests + swap call site), same scope (~50 LOC utility + ~80 LOC tests + small call-site reduction). It landed in a single session.
- **Estimated capacity:** ~60 LOC net change (+50 utility, +80 tests, −60 inline block). Single-session.

### Remaining from Design Doc

This sprint covers the entire scope of [enrichment-cte-generation.md](enrichment-cte-generation.md). Concern B (downstream-overlap filtering) is explicitly out of scope per Q2 — disappears in the planned `enhanced_events` explicit-pass-through follow-up refactor.

## Proposed Milestones

Two milestones split along a natural dependency boundary: M1 introduces the tested utility independently, M2 migrates the existing call site to consume it. After M1 alone, `utils.buildEnrichments` is exported and unit-tested but unused — a clean intermediate state. M2 then swaps the inline block for the utility, verified by existing end-to-end tests in `tests/enrichments.test.js` (30 cases).

### Milestone 1: M1_BUILD_ENRICHMENTS_UTILITY

**Goal:** Add `utils.buildEnrichments` plus unit tests, verified in isolation. Existing tests continue to pass unchanged (no behavior change).

**Estimated:** ~50 LOC utility + ~80 LOC unit tests = ~130 LOC
**Duration:** ~45 minutes

**Tasks:**

1. **Add `buildEnrichments` to [utils.js](../../utils.js)** — placement: immediately after `buildPassThroughs` (the precedent established in the previous refactor). Function signature, behavior, and JSDoc shape exactly as specified in [enrichment-cte-generation.md](enrichment-cte-generation.md). Includes both throws (item-level deferral, enrichment-vs-enrichment collision) — error message strings byte-identical to the current inline implementation so [tests/enrichments.test.js](../../tests/enrichments.test.js) cases continue to assert against the same text.

2. **Export `buildEnrichments`** from `utils.js`'s `module.exports` block, alphabetically next to `buildPassThroughs`.

3. **Add unit tests** in [tests/utils.test.js](../../tests/utils.test.js) under a new `2. buildEnrichments` section. Test cases:
   - Empty input (`[]`) → all-empty output (`{ steps: [], joins: [], columns: {}, columnNames: empty Set, columnOwner: {} }`).
   - Single event-level enrichment with backtick-FQN source → one source CTE, one join, one column, one name in set, one owner entry.
   - Single enrichment with Dataform-ref-object source → same structure; `from` field carries the ref object through unmodified.
   - Composite `joinKey` (array) → CTE selects multiple keys; join `on` clause uses `using(col1, col2)`.
   - `dedupe: true` → source step carries `qualify row_number() over (partition by <keys>) = 1`.
   - Multiple enrichments → all five fields aggregate correctly across entries; preserves entry order.
   - `level: 'item'` → throws with the exact error string referencing `config.enrichments[i]` and `data-enrichments.md`.
   - Enrichment-vs-enrichment column collision → throws with both enrichment names AND the conflicting column name.

4. **Wire into test runner** — no change needed; `tests/utils.test.js` is already wired into [tests/testRunner.js](../../tests/testRunner.js) and [package.json](../../package.json) from the previous sprint.

**Acceptance criteria:**
- [ ] `utils.buildEnrichments` exported and importable as `require('../../utils.js').buildEnrichments`.
- [ ] All 8 new unit tests pass.
- [ ] All existing 477 tests pass unchanged (no call site touched yet).
- [ ] Linting clean.

**Risks:**
- **Error message drift.** Both throw strings must be byte-identical to the current inline strings, since [tests/enrichments.test.js:181-220](../../tests/enrichments.test.js#L181-L220) (item-level deferral) and [tests/enrichments.test.js:316-330](../../tests/enrichments.test.js#L316-L330) (collision) assert on substrings. Mitigation: copy the strings verbatim from the current implementation; the M2 SQL-diff verification catches any drift.

### Milestone 2: M2_MIGRATE_ENRICHMENT_CALL_SITE

**Goal:** Replace the inline ~60-line enrichment block at [tables/ga4EventsEnhanced/index.js:323-381](../../tables/ga4EventsEnhanced/index.js#L323-L381) with a 2-3 line `utils.buildEnrichments(...)` call. Net reduction of ~60 LOC from `index.js`. Verified by existing 30 enrichment tests and SQL byte-equivalence.

**Estimated:** −60 LOC net (delete inline block, add destructure-call)
**Duration:** ~30 minutes
**Dependencies:** M1_BUILD_ENRICHMENTS_UTILITY

**Tasks:**

1. **Replace the inline block** at [tables/ga4EventsEnhanced/index.js:323-381](../../tables/ga4EventsEnhanced/index.js#L323-L381) with:
   ```js
   const { steps: enrichmentSteps, joins: enrichmentJoins, columns: enrichmentColumns,
           columnNames: enrichmentColumnNames } = utils.buildEnrichments(mergedConfig.enrichments);
   const enrichmentExcludedColumns = [...enrichmentColumnNames];
   ```
   Note: `columnOwner` from the utility is intentionally not destructured — the only consumer of owner data is the collision-throw inside the utility itself.

2. **Preserve the overlap-filter block** at [tables/ga4EventsEnhanced/index.js:383-390](../../tables/ga4EventsEnhanced/index.js#L383-L390) (`eventDataEnrichmentExcept` / `sessionDataEnrichmentExcept`) verbatim — Q2 of the design doc explicitly keeps this inline.

3. **SQL byte-equivalence verification.** Generate SQL for representative configs (the same trio used in the previous refactor: default, with `excludedColumns: ['app_info', 'publisher']`, with `eventParamsToColumns: [{name: 'page_title', type: 'string'}]`), plus one with enrichments (single, composite-key, dedupe). Diff against baselines captured before the refactor — expect zero changes.

4. **Sprint A bug-repro check.** Regenerate SQL for the purely-additive `user_segment_test` enrichment config. Confirm the column lands in SELECT and is absent from every wildcard EXCEPT — preserves the fix from commit `0642088`.

**Acceptance criteria:**
- [ ] Inline enrichment block at [tables/ga4EventsEnhanced/index.js:323-381](../../tables/ga4EventsEnhanced/index.js#L323-L381) removed.
- [ ] `utils.buildEnrichments` invoked at the call site; outputs destructured into the same locally-scoped variable names (`enrichmentSteps`, `enrichmentJoins`, `enrichmentColumns`, `enrichmentColumnNames`) so downstream code is unchanged.
- [ ] All 485 tests pass (477 existing + 8 new from M1).
- [ ] Generated SQL byte-identical to baseline across 4 verification configs.
- [ ] Sprint A bug-repro (additive `user_segment_test`) still valid.
- [ ] Linting clean.

**Risks:**
- **Net behavior change.** None expected — the extracted function is mechanically identical to the inlined code. The SQL diff is the load-bearing verification.
- **Throw-path linkage.** Both throws now originate from `utils.js` instead of `tables/ga4EventsEnhanced/index.js`. Stack traces shift slightly but error messages are byte-identical, so [tests/enrichments.test.js](../../tests/enrichments.test.js) substring-matching assertions continue to pass.

## Success Metrics

- All tests passing: 485/485 (477 existing + 8 new unit tests).
- Linting clean.
- Net LOC: ~+50 in utils.js, ~+80 in tests/utils.test.js, ~−60 in tables/ga4EventsEnhanced/index.js → roughly +70 net (utility surface area worth the cost; future tables consume one line each).
- Documentation: [enrichment-cte-generation.md](enrichment-cte-generation.md) and this sprint plan move from `planned/` to `implemented/` after merge.
- No CHANGELOG entry needed — pure internal refactor; design doc and commit messages carry the rationale.

## Dependencies

None active. The previous `event-data-explicit-columns` sprint shipped through `0.9.0-dev.6` and unblocked this work by establishing the `buildPassThroughs` precedent and consolidating the file.

## Open Questions

None. The design doc resolved Q1 (return shape), Q2 (overlap-filter scope), Q3 (input is just the array), Q4 (placement in utils.js) before sprint planning began.

## Notes

- The two-milestone split is a natural dependency, not artificial. M1 introduces a tested utility that is callable but unused; M2 swaps the consumer. Each milestone produces a verifiable intermediate state.
- After this sprint lands, the planned `enhanced_events` explicit-pass-through refactor (the follow-up that subsumes the overlap-filter into `buildQualifiedPassThroughs`) becomes simpler — it operates on a leaner `index.js`.
- Sequencing rationale (this sprint first, pass-through refactor second) is documented at [enrichment-cte-generation.md → Sequencing](enrichment-cte-generation.md).

---

**Document created**: 2026-05-11
