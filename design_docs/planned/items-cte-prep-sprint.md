# Sprint Plan: Items-CTE Prep — Rename + `event_date`

## Summary

Pure-refactor sprint that pulls two structural prerequisites out of [data-enrichments.md](data-enrichments.md) (Q16 and Q18) and ships them ahead of the larger feature work:

- Rename the ecom item CTEs to neutral multi-purpose names: `item_list_attribution` → `items_unnested`, `item_list_data` → `items_rebuilt`, `_item_list_attribution_row_id` → `_item_row_id`. The corresponding helper `itemListAttributionRowId` → `itemRowId`.
- Add `event_date` to `items_unnested` so future date-grained item joins (composite-key item-level enrichments) work without further plumbing.
- Bump to `0.9.0-dev.0` to signal the breaking change to the reserved-names contract.

**Duration:** 0.5 days
**Dependencies:** None — purely refactors existing item-list-attribution code
**Risk Level:** Low — mechanical rename + one column addition; semantic equivalence preserved; existing dry-run tests validate

## Why ship this first

The data-enrichments feature (Q1–Q19 in the design doc) is substantial. Bundling the rename inside it would force reviewers to verify both "did the rename break attribution?" and "did the new feature work?" simultaneously. Shipping the rename + `event_date` addition first means:

- Existing item-list-attribution behavior is verified semantically equivalent in isolation.
- The data-enrichments PR diff focuses purely on the new feature surface.
- The breaking change to the reserved-names contract (CTE renames) ships once, with clear release notes, instead of being entangled with a feature.

## Current Status Analysis

### Completed Recently
- Custom CTEs sprint (most recent comparable): ~250 LOC across implementation + tests, single session
- Query Builder v2 sprint: ~250 LOC, single session
- Query Builder Formatting (closest size precedent): ~30 LOC, 0.5 days

### Velocity
- Recent comparable utility-level sprints: 30–50 LOC implementation, 0.5–1 day
- This sprint is even smaller (mechanical rename + 1 column addition + version bump + doc touch-up)

### Scope (from grep across non-doc files)

34 occurrences of the names being renamed, across 5 files:

| File | Occurrences | Nature |
|---|---:|---|
| [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js) | 14 | CTE step definitions, joins, column references |
| [tests/customSteps.test.js](../../tests/customSteps.test.js) | 12 | Reserved-name collision tests (assertion strings + test cases) |
| [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js) | 4 | `itemListAttributionRowId` function (returns the renamed column) + the function itself is renamed |
| [README.md](../../README.md) | 3 | Reserved-names contract table in the customSteps section |
| [AGENTS.md](../../AGENTS.md) | 1 | Reserved-CTE-names stable-contract note |

## Proposed Milestones

Two milestones, single PR.

### M1: Rename ecom item CTEs, helper, version, and docs

**Goal:** Rename the three item-CTE-related identifiers across all production and test code; rename the helper `itemListAttributionRowId` → `itemRowId` (the row-id concept is shared between attribution and future enrichments); update the public reserved-names contract in README/AGENTS; bump to `0.9.0-dev.0` to signal the breaking change; refresh the data-enrichments design doc to reflect that the rename is now complete.

**Estimated:** ~40 LOC (mostly mechanical rename) + 1 LOC version bump + ~10 LOC doc touch-ups
**Duration:** 0.4 days

**Tasks:**
- [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js): rename in `itemListSteps` block, `itemListExcludedColumns` array, `enhancedEventsStep.joins`, `itemListOverrides`, and the `helpers.itemListAttributionRowId` call
- [helpers/ga4Transforms.js](../../helpers/ga4Transforms.js): rename function `itemListAttributionRowId` → `itemRowId`; update its module export and JSDoc; the function returns the renamed column (`_item_row_id`). `itemListAttributionExpr` keeps its name (it's attribution-specific in purpose).
- [helpers/index.js](../../helpers/index.js): update the re-export if it lists `itemListAttributionRowId` by name
- [tests/customSteps.test.js](../../tests/customSteps.test.js): update reserved-name collision tests — names AND assertion strings (12 touchpoints). Test descriptions like "item_list_data NOT reserved when itemListAttribution off" become "items_rebuilt NOT reserved when itemListAttribution off".
- [README.md](../../README.md): update reserved-names contract table in the customSteps section
- [AGENTS.md](../../AGENTS.md): update reserved-CTE-names stable-contract note
- [package.json](../../package.json): bump `version` from `0.8.0` to `0.9.0-dev.0`
- [design_docs/planned/data-enrichments.md](data-enrichments.md): update Q16 and surrounding prose to past-tense ("the rename happened in v0.9.0-dev.0") rather than future-tense ("the rename ships with v0.9.0")
- Run `npm test` — all 9 suites must pass

**Acceptance Criteria:**
- All 9 existing test suites pass; `tests/customSteps.test.js` modifications are limited to renaming strings (no test logic changes)
- Generated SQL is whitespace-equivalent to v0.8.0 modulo the renamed CTE/column identifiers (BigQuery dry-run validates correctness)
- `helpers.itemRowId` exported and consumed by `tables/ga4EventsEnhanced/index.js`; the old `itemListAttributionRowId` no longer exists
- README + AGENTS document the new names; old names appear nowhere in non-archive doc text outside data-enrichments.md's reasoning trail
- `package.json` is at `0.9.0-dev.0`
- `data-enrichments.md` describes the rename in past tense

**Risks:**
- Missing a reference site during rename — Mitigation: grep before/after; existing dry-run tests catch any reference that compiles to invalid SQL.
- Test assertion strings get out of sync with new names — Mitigation: tests run as part of the milestone gate; mismatches fail loudly.

### M2: Add `event_date` column to `items_unnested`

**Goal:** Extend `items_unnested.select.columns` with `event_date` so item-grained CTEs carry the date forward. No behavior change to attribution; pure additive prep for future composite-key item joins (data-enrichments Q18).

**Estimated:** ~5 LOC
**Duration:** 0.1 days
**Dependencies:** M1

**Tasks:**
- [tables/ga4EventsEnhanced/index.js](../../tables/ga4EventsEnhanced/index.js): add `event_date: 'event_date'` to `items_unnested.select.columns` (the renamed step from M1)
- Run `npm test` — all suites green
- Visual inspection of generated SQL with item-list-attribution enabled: confirm `event_date` appears in the items_unnested CTE select list

**Acceptance Criteria:**
- All 9 test suites green
- `items_unnested` SELECT includes `event_date`
- `items_rebuilt` doesn't reference `event_date` (current attribution doesn't need it; the column is just available for future joins)
- Attribution output is unchanged — same item structs, same coalesce override on `enhanced_events.items`

**Risks:**
- Adding the column changes the shape of `items_unnested` — Mitigation: it's a pure addition; nothing downstream consumes a fixed shape from this CTE today.

## Success Metrics

- All existing tests pass (assertion-string updates only; no functional test logic changes)
- Generated SQL semantically equivalent (modulo CTE/column name strings + the new `event_date` column in `items_unnested`)
- Reserved-names contract documented with new names in README and AGENTS
- `package.json` at `0.9.0-dev.0`
- `data-enrichments.md` reads coherently post-rename
- Single PR landed with two commits (M1, M2) or a squash

## Dependencies

- None external. M2 depends on M1 (small chain).

## Open Questions

None — all three resolved before sprint planning:

| # | Resolution |
|---|---|
| 1 | Rename `itemListAttributionRowId` → `itemRowId` (the row-id concept is shared between attribution and future enrichments). `itemListAttributionExpr` keeps its name. |
| 2 | Version bumps to `0.9.0-dev.0` as part of this sprint. The reserved-names contract change is a breaking change under 0.x semver. |
| 3 | `data-enrichments.md` is updated as part of this sprint to reflect that the rename has happened. |

## Notes

- Sprint branch suggested: `sprint/items-cte-prep`
- This sprint enables the larger data-enrichments work without coupling the breaking-change rename to the new feature
- `tests/customSteps.test.js`'s 12 touchpoints are the largest concentration — most are just renaming the strings used in collision-test assertions; one or two test names should be updated for readability
- Integration tests (`npm run test:integration`) install from the npm registry, so they only validate after the version is published; not a development-time gate

---

**Document created**: 2026-05-08
**Last updated**: 2026-05-08
