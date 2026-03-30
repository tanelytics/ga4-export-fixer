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

Whenever a new column is included in the generated SQL query, the corresponding column description should be added as well. Include the documentation in the files inside the columns folder using the designed format defined in documentation.getColumnDescriptions().

Column descriptions should follow the GA4 documentation, GA4 BigQuery export documentation, and the transformation SQL logic coming from the code base.

# Setting Dataform pre_operations

When querying GA4 export data the priority order of the exports is daily > fresh > intraday. The exports can overlap on specific days. In case of overlap, the data from the highest priority export table should be used.

The pre_operation date filters should work with all possible 7 combinations of export types enabled.

Some of the pre_operations check the status of the GA4 generated export tables. These pre_operations should only be set if the configuration declares that it's querying GA4 export data (sourceTableType: 'GA4_EXPORT').

# Writing tests

Test should cover the most important table generation logic:
1. Generating the main SQL, without errors
2. Generating the merged configuration and validating inputs
3. Generating and validating the pre_operations

Check that the test are updated accordingly if code is updated.