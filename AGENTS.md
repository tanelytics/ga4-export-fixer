# Agent Instructions

## Package publishing

When adding a new `.js` file that is imported by any published module, add it to the `files` array in `package.json`. Otherwise it will be missing from the npm package.

## README automation

After editing `README.md`, run `npm run readme` to regenerate the table of contents. The TOC is auto-generated between `<!-- TOC -->` and `<!-- /TOC -->` markers — do not edit content between them manually.

## Validation conventions

Validation functions in `inputValidation.js` **throw** on invalid input. They do not return booleans. When wrapping a validation call, use try/catch — not a truthy check on the return value.

## Helpers structure

`helpers/` is a shared, flat library — do not spread helpers into individual table module directories. Code that's reusable across tables belongs in `helpers/`; only table-specific logic (validators, assertion builders, schema overrides) belongs inside `tables/<name>/`.

## Refactoring checklist

When renaming or moving a function, search the entire codebase for all occurrences: definition, `module.exports`, `require()` calls, `index.js` re-exports, comments, and README references.

## Design docs

Before drafting a new design doc, survey `design_docs/implemented/` and `design_docs/planned/` for related work, established terminology, and Future Work items the new doc may fulfill — cross-reference rather than duplicate. Match the existing voice (impersonal, factual, no first-person) and structure. Convention: docs are snapshots — the `Status` header stays `Planned` after shipping, no Implementation Report section is added, and folder placement (`planned/` vs `implemented/`) is the truth signal.

## Reserved CTE names — stable contract

The package's internal CTE names that `customSteps` may reference (`event_data`, `session_data`, `item_list_attribution`, `item_list_data`, `enhanced_events`) are a documented stable contract under the v0.8.x line. Renaming or removing any of these is a breaking change for users with `customSteps` — bundle such changes into a minor version bump and call them out in the release notes / git tag annotation. The active reserved set is derived at runtime from the `packageSteps` array in `_generateEnhancedEventsSQL`, so adding a new internal CTE doesn't require updating a separate validation list — the new name automatically becomes reserved.

## JSDoc maintenance

When asked to generate documentation, only generate the JSDoc comment above the function. When updating a function's definition, update its JSDoc in the same change.

## Column descriptions

Adding a column to the generated SQL requires a matching entry in **all three** JSON files under `tables/<tableName>/columns/`: `columnDescriptions.json`, `columnLineage.json`, `columnTypicalUse.json`. Missing entries silently produce incomplete documentation. Follow the GA4 documentation, GA4 BigQuery export documentation, and the transformation SQL logic as the source of truth.

## Dataform pre_operations

GA4 export priority is daily > fresh > intraday. When export types overlap on a given day, the highest-priority one wins.

Pre-operation date filters must work for all 7 combinations of enabled export types.

Pre-operations that inspect GA4-generated export table status should only run when the configuration declares `sourceTableType: 'GA4_EXPORT'`.
