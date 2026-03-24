# Agent Instructions

## Package publishing

When adding a new `.js` file that is imported by any published module, add it to the `files` array in `package.json`. Otherwise it will be missing from the npm package.

## README automation

After editing `README.md`, run `npm run readme` to regenerate the table of contents. The TOC is auto-generated between `<!-- TOC -->` and `<!-- /TOC -->` markers — do not edit content between them manually.

## Validation conventions

Validation functions in `inputValidation.js` **throw** on invalid input. They do not return booleans. When wrapping a validation call, use try/catch — not a truthy check on the return value.

`validateBaseConfig` covers fields from `baseConfig` in `defaultConfig.js`. `validateEnhancedEventsConfig` covers the full GA4 events enhanced config and calls `validateBaseConfig` internally. When adding new config fields to `defaultConfig.js`, add corresponding validation to `inputValidation.js`.

## Refactoring checklist

When renaming or moving a function, search the entire codebase for all occurrences: definition, `module.exports`, `require()` calls, `index.js` re-exports, comments, and README references.

## Generating documentation

When asked to generate documentation, only generate the JSDoc comment above the function. When making updates to a function's definitions, make sure that the JSDoc is also updated accordingly.

## Column descriptions

Whenever a new column is included in the generated SQL query, the corresponding column description should be added as well. If the column's value depends on the configuration, the used configuration setting should be visible in the description via the getColumnDescriptions function.

Column descriptions should follow the GA4 documentation, GA4 BigQuery export documentation, and the transformation SQL logic coming from the code base.