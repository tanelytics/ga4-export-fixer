/*
General utility functions
*/

/**
 * Merges multiple arrays into a single array containing only unique values.
 * 
 * - Accepts any number of array arguments, including undefined.
 * - Treats undefined arguments as empty arrays.
 * - Flattens the arrays and removes duplicate values, preserving order of first appearance.
 * 
 * @param {...Array} arrays - Arrays to merge.
 * @returns {Array} The merged array with unique values.
 */
const mergeUniqueArrays = (...arrays) => {
    // Convert undefined values to empty arrays before merging
    const validArrays = arrays.map(arr => arr === undefined ? [] : arr);
    return [...new Set(validArrays.flat())];
};

/**
 * Build SQL query from an array of steps with support for CTEs.
 * 
 * Each step should have:
 * - name: CTE name (required for CTEs)
 * - columns: Object with column definitions { alias: 'expression' }
 * - from: Source table/CTE
 * - where: Optional WHERE clause
 * - groupBy: Optional array of GROUP BY columns
 * - leftJoin: Optional array of join definitions { table, condition }
 * 
 * @param {Array<Object>} steps - Array of step objects defining the query structure
 * @returns {string} Generated SQL query
 */
const queryBuilder = (steps) => {
    // Helper function to turn step.columns into SQL string
    const columnsToSQL = (columns) => {
        return Object.entries(columns)
            // exclude all columns that have been explicitly set to undefined
            .filter(([key, value]) => value !== undefined)
            .map(([key, value]) => {
                // if the key and value are the same, return the value as is (i.e. no alias)
                if (key === value) {
                    return value;
                }
                // if the key starts with '[sql]', return the value as is (i.e. no alias)
                if (key.startsWith('[sql]')) {
                    return value;
                }
                return `${value} as ${key}`;
            })
            .join(',\n    ');
    };

    const selectSQL = (step) => {
        const leftJoinClauses = step.leftJoin ? step.leftJoin.map(join => `left join\n    ${join.table} ${join.condition}`) : [];
        const whereClause = step.where ? `where\n    ${step.where}` : '';
        const groupByClause = step.groupBy ? `group by\n    ${step.groupBy.join(', ')}` : '';

        return `select
    ${columnsToSQL(step.columns)}
from
    ${step.from}
${leftJoinClauses.join('\n')}
${whereClause}
${groupByClause}`;
    };

    let sql = "";
    if (steps.length === 1) {
        // Only one step, no CTE needed
        const step = steps[0];
        sql = selectSQL(step);
    } else {
        // Multiple steps, all but last are CTEs
        const ctes = steps.slice(0, -1).map(step => {
            return `${step.name} as (${selectSQL(step)})`;
        });
        const lastStep = steps[steps.length - 1];
        sql = `with ${ctes.join(',\n    ')}\n${selectSQL(lastStep)}`;
    }
    return sql;
};

/**
 * Deep merge SQL configuration objects with special handling for nested objects and arrays.
 * 
 * Rules:
 * - Nested objects are merged recursively key by key
 * - Arrays with a "default" counterpart (e.g. excludedEvents + defaultExcludedEvents)
 *   are merged with mergeUniqueArrays, with user values taking precedence
 * - Arrays that are themselves a "default" version, or have no default counterpart,
 *   are overwritten by user input
 * - Default values are preserved unless explicitly overridden (including with undefined)
 * - Explicitly setting a value to undefined in inputConfig will override the default
 * - Date fields: after merging, specific date fields (listed in dateFields) are processed via processDate().
 *   Only fields that are defined in the result object are processed. String dates (e.g. '20260101',
 *   '2026-01-01') are converted to BigQuery SQL CAST expressions; SQL expressions (e.g. 'current_date()')
 *   are passed through unchanged.
 * 
 * @param {Object} defaultConfig - The default configuration object
 * @param {Object} inputConfig - The input configuration to merge (optional)
 * @returns {Object} The merged configuration object
 */
const mergeSQLConfigurations = (defaultConfig, inputConfig = {}) => {
    // If inputConfig is not an object, return defaultConfig
    if (!inputConfig || typeof inputConfig !== 'object' || Array.isArray(inputConfig)) {
        return defaultConfig;
    }

    // the merged configuration object
    const result = { ...defaultConfig };

    for (const key in inputConfig) {
        if (!inputConfig.hasOwnProperty(key)) continue;

        const inputValue = inputConfig[key];
        const defaultValue = defaultConfig[key];

        // If the key doesn't exist in default, just add it
        if (!(key in defaultConfig)) {
            result[key] = inputValue;
            continue;
        }

        // Handle arrays: merge with default counterpart if one exists, otherwise overwrite
        if (Array.isArray(defaultValue) && Array.isArray(inputValue)) {
            // check if the array has a "default" counterpart
            // for example, excludedEvents and defaultExcludedEvents
            const defaultKey = 'default' + key.charAt(0).toUpperCase() + key.slice(1);
            if (!key.startsWith('default') && defaultKey in result) {
                result[key] = mergeUniqueArrays(inputValue, result[defaultKey]);
            } else {
                result[key] = inputValue;
            }
            continue;
        }

        // Handle nested objects: recursive merge
        if (
            defaultValue !== null &&
            inputValue !== null &&
            typeof defaultValue === 'object' &&
            typeof inputValue === 'object' &&
            !Array.isArray(defaultValue) &&
            !Array.isArray(inputValue)
        ) {
            result[key] = mergeSQLConfigurations(defaultValue, inputValue);
            continue;
        }

        // For all other cases (primitives, null, undefined), use input value
        result[key] = inputValue;
    }

    // process configuration date fields
    // BigQuery SQL statements are excepted
    // string dates such as '20260101' or '2026-01-01' are processed
    const dateFields = [
        'preOperations.dateRangeStartFullRefresh', 
        'preOperations.dateRangeEnd', 
        'preOperations.incrementalStartOverride', 
        'preOperations.incrementalEndOverride', 
        'testConfig.dateRangeStart', 
        'testConfig.dateRangeEnd'
    ];

    for (const path of dateFields) {
        const parts = path.split('.');
        let obj = result;
        for (let i = 0; i < parts.length - 1; i++) {
            if (obj == null || typeof obj !== 'object') break;
            obj = obj[parts[i]];
        }
        if (obj != null && typeof obj === 'object') {
            const key = parts[parts.length - 1];
            const value = obj[key];
            if (value !== undefined) {
                obj[key] = processDate(value);
            }
        }
    }

    // support different formats for passing the sourceTable path
    const fixSourceTable = (sourceTable) => {
        if (isDataformTableReferenceObject(sourceTable)) {
            return sourceTable;
        }
        if (typeof sourceTable === 'string') {
            const tablePath = sourceTable.replace(/[`"']/g, '').trim();
            if (/^[a-zA-Z0-9-]+\.[a-zA-Z0-9_]+(\.[^\.]+)?$/.test(tablePath)) {
                const project = tablePath.split('.')[0];
                const dataset = tablePath.split('.')[1];
                return `\`${project}.${dataset}.events_*\``;
            }
        }
        throw new Error(`sourceTable must be a Dataform table reference or a string. Supported string formats include: '\`project.dataset.events_*\`', 'project.dataset', 'project.dataset.events_*', for example. Received: ${JSON.stringify(sourceTable)}`);
    };

    // process the sourceTable to support different formats
    if (result.sourceTable) {
        result.sourceTable = fixSourceTable(result.sourceTable);
    }

    return result;
};

/**
 * Checks if a given object is a Dataform table reference object.
 *
 * A Dataform table reference object is expected to have the properties: 'name', and 'dataset'.
 *
 * @param {Object} obj - The object to check.
 * @returns {boolean} True if the object is a Dataform table reference, false otherwise.
 */
const isDataformTableReferenceObject = (obj) => {
    return obj &&
        typeof obj === 'object' &&
        Object.hasOwn(obj, 'name') &&
        // Dataform transforms the schema key to dataset key when using ctx.ref()
        (Object.hasOwn(obj, 'dataset') || Object.hasOwn(obj, 'schema'));
};


/**
 * Sets the Dataform context for a configuration object.
 *
 * This function updates the provided config object by resolving the `sourceTable` property. If the `sourceTable`
 * is a Dataform table reference object (with 'name', and 'dataset' properties), it uses `ctx.ref()` to
 * obtain the correct reference. Otherwise, it checks if `sourceTable` is a string in the format '`project.dataset.table`'.
 * If not, it throws an error. Finally, it sets the `self` and `incremental` properties on the config using
 * `ctx.self()` and `ctx.incremental()`, respectively.
 *
 * @param {Object} ctx - The Dataform context, contains methods for referencing tables and context information.
 * @param {Object} config - The configuration object to update with Dataform context and resolved table references.
 * @returns {Object} The updated configuration object with appropriate Dataform context set.
 * @throws {Error} If `sourceTable` is not a valid Dataform reference object or a correctly formatted string.
 */
const setDataformContext = (ctx, config) => {
    // if the sourceTable is a Dataform reference, use ctx.ref() to reference to it
    if (isDataformTableReferenceObject(config.sourceTable)) {
        config.sourceTable = ctx.ref(config.sourceTable);
    } else {
        // if the sourceTable is not a Dataform reference, it must be a string in the format '`project.dataset.table`'
        if (typeof config.sourceTable !== 'string' || !/^`[^\.]+\.[^\.]+\.[^\.]+`$/.test(config.sourceTable)) {
            throw new Error(`Failed to set Dataform context: config.sourceTable must be a Dataform table reference or a string in the format 'project.dataset.table'. Received: ${JSON.stringify(config.sourceTable)}`);
        }
    }

    config.self = ctx.self();
    config.incremental = ctx.incremental();

    return config;
};

/**
 * Deep merge Dataform table configuration objects.
 * 
 * Rules:
 * - Nested objects are merged recursively key by key (e.g. setting bigquery.partitionBy
 *   does not overwrite the whole bigquery object)
 * - Array fields are overwritten by user input by default
 * - The 'tags' array is an exception: default tags are preserved and user input is concatenated
 * - Default values are preserved unless the user input explicitly overrides them
 * 
 * @param {Object} defaultConfig - The default Dataform table configuration object
 * @param {Object} inputConfig - The user-provided table configuration to merge (optional)
 * @returns {Object} The merged Dataform table configuration object
 */
const mergeDataformTableConfigurations = (defaultConfig, inputConfig = {}) => {
    if (!inputConfig || typeof inputConfig !== 'object' || Array.isArray(inputConfig)) {
        return defaultConfig;
    }

    // Array keys where default values should be preserved and user input concatenated
    const concatenateArrayKeys = ['tags'];

    const deepMerge = (defaultObj, inputObj) => {
        const result = { ...defaultObj };

        for (const key in inputObj) {
            if (!inputObj.hasOwnProperty(key)) continue;

            const inputValue = inputObj[key];
            const defaultValue = defaultObj[key];

            // If the key doesn't exist in default, just add it
            if (!(key in defaultObj)) {
                result[key] = inputValue;
                continue;
            }

            // Handle arrays
            if (Array.isArray(defaultValue) && Array.isArray(inputValue)) {
                // Concatenate and deduplicate for specified keys, overwrite for all others
                result[key] = concatenateArrayKeys.includes(key)
                    ? mergeUniqueArrays(defaultValue, inputValue)
                    : inputValue;
                continue;
            }

            // Handle nested objects: recursive merge
            if (
                defaultValue !== null &&
                inputValue !== null &&
                typeof defaultValue === 'object' &&
                typeof inputValue === 'object' &&
                !Array.isArray(defaultValue) &&
                !Array.isArray(inputValue)
            ) {
                result[key] = deepMerge(defaultValue, inputValue);
                continue;
            }

            // For all other cases (primitives, null, undefined), use input value
            result[key] = inputValue;
        }

        return result;
    };

    return deepMerge(defaultConfig, inputConfig);
};

/**
 * Generates a SQL selection string for a given query step, excluding columns already defined elsewhere
 * or columns that should be excluded.
 *
 * This utility is helpful when joining tables/CTEs to avoid selecting duplicate or already-present columns.
 * 
 * @param {Object} step - The step object containing a `name` (CTE/table alias) and a `columns` object.
 * @param {string[]} [alreadyDefinedColumns=[]] - Columns that have already been defined and should be excluded from selection.
 * @param {string[]} [excludedColumns=[]] - Additional columns to explicitly exclude from selection.
 * @returns {string|undefined} A SQL select string (e.g. 'stepName.*' or 'stepName.* except (col1, col2)'), or undefined if all columns are excluded.
 */
const selectOtherColumns = (step, alreadyDefinedColumns = [], excludedColumns = []) => {
    const stepName = step.name;
    const stepColumns = Object.keys(step.columns);

    // Determine which columns to exclude: those already defined or explicitly excluded
    const exceptColumns = stepColumns.filter(
        column => alreadyDefinedColumns.includes(column) || excludedColumns.includes(column)
    );

    // If none of the columns have been defined or excluded, select them all
    if (exceptColumns.length === 0) {
        return `${stepName}.*`;
    }

    // If all columns have been defined or excluded, do not select any
    if (exceptColumns.length === stepColumns.length) {
        return;
    }

    // Otherwise, select all except the excluded/defined ones
    return `${stepName}.* except (${exceptColumns.join(', ')})`;
};


/**
 * Processes a date input string and returns a corresponding SQL date casting expression,
 * or passes through BigQuery SQL statements as-is.
 *
 * Supported formats:
 * - YYYYMMDD (e.g. "20260101"): returns a SQL CAST using the 'YYYYMMDD' date format.
 * - YYYY-MM-DD (e.g. "2026-01-01"): returns a SQL CAST using the 'YYYY-MM-DD' date format.
 * - BigQuery SQL statement: input that starts with an identifier (letters and underscores only)
 *   followed by parentheses (e.g. "current_date()", "current_date()-1", "date(2026, 1, 1)") is
 *   returned unchanged.
 *
 * @param {string} dateInput - The input date string or SQL expression to process.
 * @returns {string} A SQL date casting expression or the input SQL statement unchanged.
 * @throws {Error} If the input is not a non-empty string, or if the format is not supported.
 *
 * @example
 * processDate("20260101")           // returns "cast('20260101' as date format 'YYYYMMDD')"
 * processDate("2026-01-01")         // returns "cast('2026-01-01' as date format 'YYYY-MM-DD')"
 * processDate("current_date()")     // returns "current_date()"
 * processDate("date(2026, 1, 1)")   // returns "date(2026, 1, 1)"
 */
const processDate = (dateInput) => {
    if (typeof dateInput !== 'string') {
        throw new Error(`processDate: dateInput must be a string. Received type: ${typeof dateInput}, value: ${JSON.stringify(dateInput)}`);
    }
    if (dateInput.trim() === '') {
        throw new Error('processDate: dateInput cannot be an empty string.');
    }

    // If input is a string in the format YYYYMMDD (e.g., "20260101"), return as cast(... as date format 'YYYYMMDD')
    if (/^\d{8}$/.test(dateInput.trim())) {
        return `cast('${dateInput}' as date format 'YYYYMMDD')`;
    }

    // If input is a string in the format YYYY-MM-DD (e.g., "2026-01-01"), return as cast(... as date format 'YYYY-MM-DD')
    if (/^\d{4}-\d{2}-\d{2}$/.test(dateInput.trim())) {
        return `cast('${dateInput}' as date format 'YYYY-MM-DD')`;
    }

    // If input is a BigQuery SQL statement (identifier of letters/underscores followed by parens, e.g. "current_date()-1", "date(2026, 1, 1)"), return it as is
    if (/^[a-zA-Z_]+\s*\(/.test(dateInput.trim())) {
        return dateInput;
    }

    throw new Error(`processDate: Unsupported date input format: ${JSON.stringify(dateInput)}. Expected formats are: YYYYMMDD, YYYY-MM-DD, or BigQuery SQL statement.`);
};

module.exports = {
    mergeUniqueArrays,
    mergeSQLConfigurations,
    mergeDataformTableConfigurations,
    queryBuilder,
    isDataformTableReferenceObject,
    setDataformContext,
    selectOtherColumns,
    processDate
};