# Sprint Plan: Custom CTEs via Configuration

## Summary
Add `customSteps: []` config field to let users append v2-shaped step objects to the pipeline. Rename `finalStep` → `enhanced_events` so custom CTEs have a stable handle to join against. Validation split per Q3/Q6: config-shape in `validation.js`, runtime-derived collision check in `_generateEnhancedEventsSQL`. Ships in v0.8.0 alongside `query-builder-v2`.

**Duration:** 1 day
**Dependencies:** None — `query-builder-v2` already landed in `0.8.0-dev.0` ([commit f1632da](https://github.com))
**Risk Level:** Low — small, well-specified change on a stable foundation

## Current Status Analysis

### Completed Recently
- **queryBuilder v2** (this session): ~250 LOC, 1 working session — refactor + migration + 36 unit tests
- Item list attribution row id fix: ~10 LOC, 1 day
- Query Builder Formatting: ~30 LOC, 0.5 days

### Velocity
- Comparable utility-level sprints: 30–50 LOC/day implementation, 100+ LOC/day for test files
- v2 sprint shipped at velocity well above the 2.5-day estimate (one session)
- Custom CTEs is much smaller than v2 — 0.75–1 day is realistic

### Remaining from Design Doc
All 6 design questions resolved. Implementation scope per the design doc's Files-to-Modify table (~190 LOC):

| File | Change | Est. LOC |
|---|---|---|
| [tables/ga4EventsEnhanced/index.js](tables/ga4EventsEnhanced/index.js) | Rename `finalStep` → `enhancedEventsStep`; runtime-derive reserved names; collision check; append `customSteps` | ~+15 / -3 |
| [tables/ga4EventsEnhanced/validation.js](tables/ga4EventsEnhanced/validation.js) | Layer 1 config-shape validation | ~+20 |
| [tables/ga4EventsEnhanced/config.js](tables/ga4EventsEnhanced/config.js) | Default `customSteps: []` | ~+1 |
| [tests/ga4EventsEnhanced.test.js](tests/ga4EventsEnhanced.test.js) | Pipeline-shape + collision tests | ~+80 |
| [tests/inputValidation.test.js](tests/inputValidation.test.js) | Layer 1 validation cases | ~+40 |
| README, AGENTS.md | `customSteps` docs + reserved-names contract | ~+30 |

## Proposed Milestones

Two milestones, single PR. M2 depends on M1.

### M1 — Implementation: rename + customSteps wiring + validation
**Goal:** Add `customSteps` config field, wire it into the pipeline, put both validation layers in place. Existing tests still pass after this milestone.
**Estimated:** ~40 LOC
**Duration:** 0.4 days

**Tasks:**
- [tables/ga4EventsEnhanced/index.js](tables/ga4EventsEnhanced/index.js):
  - Rename local var `finalStep` → `enhancedEventsStep`
  - Set `name: 'enhanced_events'` (was `'final'`)
  - Build `packageSteps` array; runtime-derive reserved set as `new Set(packageSteps.map(s => s.name))`
  - Collision check before appending: throw with offender + active reserved-set listed
  - Append `mergedConfig.customSteps ?? []` to steps array
- [tables/ga4EventsEnhanced/config.js](tables/ga4EventsEnhanced/config.js):
  - Add `customSteps: []` to the default config
- [tables/ga4EventsEnhanced/validation.js](tables/ga4EventsEnhanced/validation.js):
  - Layer 1 shape validation: `customSteps` is array (or undefined); each entry is non-null object with non-empty string `name`; names unique within `customSteps`
  - Explicitly NOT here: collision-with-package-names check (Layer 2 owns it per Q3)
- Manual smoke test: run `generateSql` with one raw + one structured custom step, confirm SQL renders correctly

**Acceptance Criteria:**
- [ ] All 8 existing test suites pass without modification (with empty `customSteps`, pipeline emits same SQL semantics; only the `final → enhanced_events` rename changes)
- [ ] Smoke test: small config with one custom step renders correctly via `generateSql`
- [ ] Layer 1 validation throws with clear messages for non-array, null entry, missing name, empty name, duplicate name within customSteps
- [ ] Layer 2 collision check throws with the runtime-derived reserved set in the error message

**Risks:**
- Rename must be consistent — every reference to `finalStep` in `_generateEnhancedEventsSQL` gets updated. Mitigation: grep before/after; existing dry-run tests catch missed references immediately.

### M2 — Tests + docs
**Goal:** Direct test coverage for the new feature including the conditional-reservation case, plus user-facing docs for `customSteps` and the reserved-names contract.
**Estimated:** ~150 LOC
**Duration:** 0.6 days
**Dependencies:** M1

**Tasks:**
- New test cases in [tests/ga4EventsEnhanced.test.js](tests/ga4EventsEnhanced.test.js):
  - Empty / undefined `customSteps` → pipeline equivalent to current behavior modulo rename
  - One raw step referencing `enhanced_events` → renders correctly
  - One structured step referencing `enhanced_events` → renders correctly
  - Multiple steps in mixed shapes → array order preserved, last is final SELECT
  - Custom step references `event_data` → SQL valid (BigQuery dry-run validates)
  - Collision: `name: 'event_data'` → throws
  - Collision: `name: 'enhanced_events'` → throws
  - **Conditional reservation** (the subtle case): `name: 'item_list_data'` without `itemListAttribution` → does NOT throw
  - **Conditional reservation**: `name: 'item_list_data'` with `itemListAttribution` enabled → throws
- New test cases in [tests/inputValidation.test.js](tests/inputValidation.test.js):
  - `customSteps: 'not an array'` → throws
  - `customSteps: [null]` → throws
  - `customSteps: [{}]` → throws (no name)
  - `customSteps: [{name: ''}]` → throws (empty name)
  - `customSteps: [{name: 'a', query: '...'}, {name: 'a', query: '...'}]` → throws (duplicate within customSteps)
- [README.md](README.md) + [AGENTS.md](AGENTS.md):
  - Section on `customSteps` with one worked example (UTM attribution from the design doc)
  - Reserved-names contract table (5 names, with the always-vs-conditional distinction)
  - Note on buffer-window access via pre-`enhanced_events` CTEs

**Acceptance Criteria:**
- [ ] ≥10 new test cases across the two test files
- [ ] All test suites green via `npm test`
- [ ] README + AGENTS document `customSteps` with at least one working example
- [ ] Reserved-names contract documented, including the conditional reservation rule
- [ ] No CHANGELOG file edits (per project convention — version history lives in git tags)

**Risks:**
- Test scope creep — keep to the cases enumerated above; defer extensive scenarios to follow-ups.
- Doc format consistency — match existing config-options style in README/AGENTS.

## Success Metrics
- All existing tests pass without modification (regression check on the rename)
- ≥10 new test cases covering pipeline shape + Layer 1 validation + Layer 2 collision (including conditional reservation)
- README + AGENTS document `customSteps` and the reserved-names contract
- Single PR with 2 commits (M1, M2) or a squash; up to author

## Dependencies
- None external. `query-builder-v2` already in `main`. M2 depends on M1.

## Open Questions
None — all 6 design questions resolved in the design doc.

## Notes
- Design doc estimated 0.5 days; this plan budgets 1 day to absorb doc-writing and the subtle conditional-reservation test cases.
- No version bump needed for this sprint — `0.8.0-dev.x` covers it; the stable `0.8.0` tag comes after this + any other planned 0.8.0 work lands.
- Sprint branch suggested: `sprint/custom-ctes` (matching the v2 pattern).
- The git tag annotation for `0.8.0` should summarize v2 + custom CTEs together for users (since they're a coupled story).

---

**Document created**: 2026-05-06
**Last updated**: 2026-05-06
