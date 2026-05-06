# Sprint Plan: Multi-Table Module Architecture

## Summary
Refactor the package so that adding a new table module (sessions, ecommerce) only requires files under `tables/<name>/` with zero changes to shared infrastructure. This is a pure refactoring sprint — no behavior or API changes.

**Duration:** 3-4 days
**Dependencies:** None (builds on incremental improvements already shipped in v0.4.2-v0.4.6)
**Risk Level:** Low (internal-only restructuring, byte-identical output required at each milestone)

## Current Status Analysis

### Completed Recently (v0.4.2-v0.4.6)
- Helpers split into contextual files (~900 LOC reorganized)
- `index.js` simplified to 3 exports
- Internal SQL generation separated from exported wrappers
- `dataformTableConfig` migrated to `defaultConfig.js`
- Input validation `skipDataformContextFields` option added
- Integration tests added (~1400 LOC)

### Velocity
- 19 active development days in last 30 days
- Mix of refactoring, bug fixes, and new test infrastructure
- Similar refactoring scope (helpers split) completed in a single commit

### Remaining from Design Doc
- Phase 1: Extract shared `createTable` lifecycle
- Phase 2: Make `documentation.js` table-agnostic
- Phase 3: Per-table config and validation

## Proposed Milestones

### Milestone 1: Extract shared `createTable` + convert to directory
**Goal:** Extract the generic createTable lifecycle into a shared function. Convert `tables/ga4EventsEnhanced.js` from a single file to `tables/ga4EventsEnhanced/` directory, since Phases 2-3 need to add files there.
**Estimated:** ~80 LOC new + ~50 LOC modified = ~130 LOC total changes
**Duration:** 1 day

**Tasks:**
1. Create `tables/ga4EventsEnhanced/` directory
2. Move `tables/ga4EventsEnhanced.js` → `tables/ga4EventsEnhanced/index.js` (rename only, no content changes)
3. Move `getDatasetName` from `tables/ga4EventsEnhanced/index.js` to `utils.js`
4. Create `createTable.js` with shared lifecycle (~40 LOC)
5. Refactor `tables/ga4EventsEnhanced/index.js` to delegate `createEnhancedEventsTable` to shared `createTable`
6. Run `npm test` + `npm run test:integration` — verify byte-identical SQL output
7. Verify `npm pack --dry-run` includes all files

**Acceptance Criteria:**
- [ ] `createTable.js` exists with shared lifecycle function
- [ ] `getDatasetName` is in `utils.js`
- [ ] `ga4EventsEnhanced.createTable(publish, config)` signature unchanged
- [ ] Generated SQL is byte-identical to pre-refactoring output
- [ ] All unit and integration tests pass
- [ ] `npm pack` includes all necessary files

**Risks:**
- `require` path changes when converting file to directory — Mitigation: Node.js resolves `require('./tables/ga4EventsEnhanced')` to `./tables/ga4EventsEnhanced/index.js` automatically, so no upstream changes needed.

### Milestone 2: Make `documentation.js` table-agnostic
**Goal:** Remove hardcoded column JSON imports from `documentation.js`. Move column metadata into the table module directory. Each table module provides its own column data.
**Estimated:** ~60 LOC new + ~80 LOC modified = ~140 LOC total changes
**Duration:** 1-2 days

**Tasks:**
1. Move `columns/` → `tables/ga4EventsEnhanced/columns/` (4 JSON files)
2. Parameterize `getColumnDescriptions(config)` → `getColumnDescriptions(config, columnMetadata)` where `columnMetadata = { descriptions, lineage, typicalUse }`
3. Extract GA4-specific sections from `getTableDescription` (event vocabulary, table features) into `tables/ga4EventsEnhanced/tableDescription.js`
4. Refactor `getTableDescription` into a generic `buildTableDescription(config, sections)` that accepts section providers
5. Wire up `tables/ga4EventsEnhanced/index.js` to load local column metadata and call parameterized documentation functions
6. Update `tests/documentation.test.js` to pass column metadata explicitly
7. Run all tests + integration tests
8. Update `package.json` `files` array: remove top-level `"columns"` entry (now under `"tables"`)

**Acceptance Criteria:**
- [ ] `documentation.js` has zero hardcoded `require('./columns/*.json')` imports
- [ ] Column JSON files live under `tables/ga4EventsEnhanced/columns/`
- [ ] `getColumnDescriptions` and `getTableDescription` accept column metadata as parameters
- [ ] GA4-specific description sections (event vocabulary, table features) are in `tables/ga4EventsEnhanced/tableDescription.js`
- [ ] Shared utilities (`composeDescription`, `getLineageText`, `buildConfigNotes`) remain in `documentation.js`
- [ ] Table description output is identical to pre-refactoring
- [ ] All tests pass

**Risks:**
- `documentation.js` is currently exported from `index.js` indirectly via `ga4EventsEnhanced` — Mitigation: the export is `columnDescriptions` on the documentation module, verify no external consumers rely on it directly.

### Milestone 3: Per-table config and validation
**Goal:** Move `ga4EventsEnhancedConfig` and `validateEnhancedEventsConfig` into the table module directory.
**Estimated:** ~20 LOC new + ~40 LOC modified = ~60 LOC total changes (mostly file moves)
**Duration:** 0.5-1 day

**Tasks:**
1. Create `tables/ga4EventsEnhanced/config.js` — move `ga4EventsEnhancedConfig` from `defaultConfig.js`
2. Create `tables/ga4EventsEnhanced/validation.js` — move `validateEnhancedEventsConfig` from `inputValidation.js`
3. Slim `defaultConfig.js` to only export `baseConfig`
4. Slim `inputValidation.js` to only export `validateBaseConfig`
5. Update `require` paths in `tables/ga4EventsEnhanced/index.js`
6. Verify `index.js` public API unchanged (it imports from `tables/ga4EventsEnhanced` which re-exports everything)
7. Run all tests

**Acceptance Criteria:**
- [ ] `defaultConfig.js` only contains `baseConfig`
- [ ] `inputValidation.js` only contains `validateBaseConfig`
- [ ] `ga4EventsEnhancedConfig` lives in `tables/ga4EventsEnhanced/config.js`
- [ ] `validateEnhancedEventsConfig` lives in `tables/ga4EventsEnhanced/validation.js`
- [ ] Public API (`index.js` exports) unchanged
- [ ] All tests pass

**Risks:**
- Low risk. Neither `ga4EventsEnhancedConfig` nor `validateEnhancedEventsConfig` are part of the public API. Internal path changes only.

## Success Metrics
- All unit tests passing (`npm test`)
- All integration tests passing (`npm run test:integration`)
- Generated SQL byte-identical before and after
- Table descriptions identical before and after
- `npm pack --dry-run` includes all files
- Public API unchanged: `{ helpers, ga4EventsEnhanced, setPreOperations }`
- Adding a new table module requires only files under `tables/<name>/`

## Dependencies
- Milestones are sequential: M1 → M2 (can run in parallel with M3) → M3
- M1 must complete first (directory conversion is prerequisite for M2 and M3)

## Open Questions
- Should `documentation.js` exported `columnDescriptions` object be preserved for backward compatibility, or is it purely internal? (Currently exported but not part of `index.js` public API)

## Notes
- This is a refactoring-only sprint. No SQL generation changes, no new features.
- At each milestone boundary, all tests must pass and output must be identical.
- The `tables/ga4EventsEnhanced.js` → `tables/ga4EventsEnhanced/index.js` rename is transparent to Node.js `require` resolution.
