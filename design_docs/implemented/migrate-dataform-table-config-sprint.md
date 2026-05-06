# Sprint Plan: Migrate dataformTableConfig Defaults

## Summary
Move static `dataformTableConfig` defaults from inline hardcoded values in `createEnhancedEventsTable()` to `defaultConfig.js`, while keeping dynamic fields (name, schema, description, columns) computed at runtime. Prevent double-merge issues by extracting user overrides before the SQL config merge.

**Duration:** 1 day
**Dependencies:** None
**Risk Level:** Medium (config merge chain is sensitive — must verify output is identical)

## Current Status Analysis

### Completed Recently
- Schema lock unique naming: ~10 LOC in 1 commit
- Helpers module split: ~810 LOC refactored across 7 files in 1 commit
- JSDoc documentation: ~100 LOC across 4 files in 1 commit

### Velocity
- Recent average: Refactoring tasks complete in single sessions
- This is a focused refactor touching 3 files with ~30 LOC of actual changes

### Remaining from Design Doc
- Phase 1 — Add static defaults to `defaultConfig.js`: ~15 LOC
- Phase 2 — Extract user config before SQL merge: ~5 LOC
- Phase 3 — Refactor merge chain in `createTable`: ~15 LOC
- Verification — Comparison test: ~20 LOC

## Proposed Milestones

### Milestone 1: Add static defaults to defaultConfig.js
**Goal:** Centralize the static portion of `dataformTableConfig` in the same file as all other defaults
**Estimated:** ~15 LOC implementation
**Duration:** 15 minutes

**Tasks:**
- Add `dataformTableConfig` object to `ga4EventsEnhancedConfig` in `defaultConfig.js` with: `type`, `bigquery` (partitionBy, clusterBy, labels), `onSchemaChange`, `tags`
- Uncomment/replace the existing `// dataformTableConfig: {}` comment on line 41

**Acceptance Criteria:**
- [ ] Static defaults defined in `defaultConfig.js`
- [ ] Dynamic fields (`name`, `schema`, `description`, `columns`) NOT included in static defaults

### Milestone 2: Refactor createEnhancedEventsTable merge chain
**Goal:** Extract user `dataformTableConfig` before SQL merge to prevent double-merge and ensure user overrides are applied exactly once
**Estimated:** ~20 LOC changes
**Duration:** 30 minutes

**Tasks:**
- Extract `config.dataformTableConfig` into a separate variable before calling `mergeSQLConfigurations`
- Remove the inline `defaultDataformTableConfig` object (lines 327-342)
- Use `mergedConfig.dataformTableConfig` as the static defaults base (already merged from defaultConfig by `mergeSQLConfigurations`)
- Compute dynamic fields (`name`, `schema`, `description`, `columns`) separately
- Spread static defaults + dynamic fields as the base, merge user overrides on top via `mergeDataformTableConfigurations`

**Acceptance Criteria:**
- [ ] User `dataformTableConfig` overrides applied exactly once
- [ ] Static defaults come from `defaultConfig.js` via `mergedConfig`
- [ ] Dynamic fields computed from merged SQL config as before
- [ ] `setDefaults()` logic for name/schema preserved

**Risks:**
- Double merge if user overrides aren't extracted before SQL merge — Mitigation: Extract `config.dataformTableConfig` before `mergeSQLConfigurations` call

### Milestone 3: Verify identical output
**Goal:** Confirm the refactored merge chain produces byte-identical results to the current implementation
**Estimated:** ~20 LOC test script
**Duration:** 15 minutes

**Tasks:**
- Write a comparison script (similar to the helpers verification) that:
  - Requires the old `createEnhancedEventsTable` from git (via `git show HEAD:tables/ga4EventsEnhanced.js`)
  - Compares final `dataformTableConfig` output for: default config, config with partial overrides, config with full overrides
- Run `npm test` to confirm existing SQL validation tests pass

**Acceptance Criteria:**
- [ ] Default config produces identical output
- [ ] Partial user overrides (e.g., custom schema, custom tags) produce identical output
- [ ] Full user overrides produce identical output
- [ ] `npm test` passes
- [ ] SQLX deployment path unaffected (does not touch `dataformTableConfig`)

## Success Metrics
- All existing tests passing (`npm test`)
- Comparison test confirms identical output for all config patterns
- `defaultConfig.js` is the single source of truth for all static defaults
- No changes to public API or user-facing config shape

## Dependencies
- None — this is a purely internal refactor

## Open Questions
- Should `inputValidation.js` validate `dataformTableConfig` fields? Currently there's no validation for it. Could be deferred to a follow-up.

## Notes
- The `mergeSQLConfigurations` function deep-merges nested objects, so `dataformTableConfig` from `defaultConfig` will be properly merged with user input during SQL merge. The key is extracting the user's original overrides before that merge happens so they can be applied cleanly in the final `mergeDataformTableConfigurations` step.
- The `tags` array has special handling in `mergeDataformTableConfigurations` (concatenated and deduplicated), which is preserved by this approach.
