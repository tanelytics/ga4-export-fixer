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
 * - Arrays are concatenated and deduplicated
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

        // Handle arrays: concatenate and remove duplicates
        if (Array.isArray(defaultValue) && Array.isArray(inputValue)) {
            result[key] = mergeUniqueArrays(defaultValue, inputValue);
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
 * A Dataform table reference object is expected to have the properties: 'name', and 'schema'.
 *
 * @param {Object} obj - The object to check.
 * @returns {boolean} True if the object is a Dataform table reference, false otherwise.
 */
const isDataformTableReferenceObject = (obj) => {
    return obj &&
        typeof obj === 'object' &&
        Object.hasOwn(obj, 'name') &&
        Object.hasOwn(obj, 'schema');
};


/**
 * Sets the Dataform context for a configuration object.
 *
 * This function updates the provided config object by resolving the `sourceTable` property. If the `sourceTable`
 * is a Dataform table reference object (with 'name', and 'schema' properties), it uses `ctx.ref()` to
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

/**
 * Validates a GA4 export fixer configuration object.
 * Validation is performed on mergedConfig (default values merged with user input).
 * All fields are required in the merged config; optional fields are only optional for user input
 * and receive their values from the default configuration during merge.
 *
 * @param {Object} config - The merged configuration object to validate.
 * @throws {Error} If any configuration value is invalid or missing.
 */
const validateConfig = (config) => {
    if (!config || typeof config !== 'object' || Array.isArray(config)) {
        throw new Error(`config must be a non-null object. Received: ${JSON.stringify(config)}`);
    }

    // sourceTable - required; string or Dataform table reference
    if (config.sourceTable === undefined || config.sourceTable === null) {
        throw new Error("config.sourceTable is required. Provide a Dataform table reference (using the ref() function) or a string in format '`project.dataset.table`'.");
    }
    if (isDataformTableReferenceObject(config.sourceTable)) {
        // Valid Dataform reference
    } else if (typeof config.sourceTable === 'string') {
        if (!config.sourceTable.trim()) {
            throw new Error("config.sourceTable must be a non-empty string. Received empty string.");
        }
        if (!/^`[^\.]+\.[^\.]+\.[^\.]+`$/.test(config.sourceTable.trim())) {
            throw new Error(`config.sourceTable must be in the format '\`project.dataset.table\`' (with backticks). Received: ${JSON.stringify(config.sourceTable)}`);
        }
    } else {
        throw new Error(`config.sourceTable must be a Dataform table reference object or a string in format '\`project.dataset.table\`'. Received: ${JSON.stringify(config.sourceTable)}`);
    }

    // self - required when using Dataform; must be valid format
    // config.self is required when config.test === true and must be a non-empty string in format '\`project.dataset.table\`' (using the ref() function)
    if (config.test !== true) {
        if (typeof config.self !== 'string' || !config.self.trim() || !/^`[^`]+`$/.test(config.self.trim())) {
            throw new Error(`config.self is required when config.test === true and must be a non-empty string in format '\`project.dataset.table\`' (using the ref() function). Received: ${JSON.stringify(config.self)}`);
        }
    }

    // incremental - required when using Dataform; must be boolean
    if (typeof config.incremental !== 'boolean') {
        throw new Error(`config.incremental must be a boolean. Received: ${JSON.stringify(config.incremental)}`);
    }

    // schemaLock - optional; must be undefined or a string in "YYYYMMDD" format (e.g., "20260101")
    if (typeof config.schemaLock !== 'undefined') {
        if (typeof config.schemaLock !== 'string' || !/^\d{8}$/.test(config.schemaLock)) {
            throw new Error(`config.schemaLock must be a string in "YYYYMMDD" format (e.g., "20260101"). Received: ${JSON.stringify(config.schemaLock)}`);
        }
        // Must be a valid date
        const year = parseInt(config.schemaLock.slice(0, 4), 10);
        const month = parseInt(config.schemaLock.slice(4, 6), 10);
        const day = parseInt(config.schemaLock.slice(6, 8), 10);
        const date = new Date(year, month - 1, day);
        if (date.getFullYear() !== year || date.getMonth() !== month - 1 || date.getDate() !== day) {
            throw new Error(`config.schemaLock must be a valid date. Received: ${JSON.stringify(config.schemaLock)}`);
        }
        // Must be at least 20241009
        if (config.schemaLock < "20241009") {
            throw new Error(`config.schemaLock must be a date string equal to or greater than "20241009". Received: ${JSON.stringify(config.schemaLock)}`);
        }
    }

    // includedExportTypes - required
    if (typeof config.includedExportTypes === 'undefined') {
        throw new Error("config.includedExportTypes is required.");
    }
    if (!config.includedExportTypes || typeof config.includedExportTypes !== 'object' || Array.isArray(config.includedExportTypes)) {
        throw new Error(`config.includedExportTypes must be an object. Received: ${JSON.stringify(config.includedExportTypes)}`);
    }
    for (const key of ['daily', 'intraday']) {
        // fresh not requred at the moment
        if (!(key in config.includedExportTypes)) {
            throw new Error(`config.includedExportTypes.${key} is required.`);
        }
        if (typeof config.includedExportTypes[key] !== 'boolean') {
            throw new Error(`config.includedExportTypes.${key} must be a boolean. Received: ${JSON.stringify(config.includedExportTypes[key])}`);
        }
    }

    // timezone - required
    if (typeof config.timezone === 'undefined') {
        throw new Error("config.timezone is required.");
    }
    if (typeof config.timezone !== 'string' || !config.timezone.trim()) {
        throw new Error(`config.timezone must be a non-empty string (e.g. 'Etc/UTC', 'Europe/Helsinki'). Received: ${JSON.stringify(config.timezone)}`);
    }

    // customTimestampParam - optional; must be undefined or a non-empty string
    if (typeof config.customTimestampParam !== 'undefined') {
        if (typeof config.customTimestampParam !== 'string' || !config.customTimestampParam.trim()) {
            throw new Error(`config.customTimestampParam must be a non-empty string when provided. Received: ${JSON.stringify(config.customTimestampParam)}`);
        }
    }

    // dataIsFinal - required
    if (typeof config.dataIsFinal === 'undefined') {
        throw new Error("config.dataIsFinal is required.");
    }
    if (typeof config.dataIsFinal !== 'object' || Array.isArray(config.dataIsFinal)) {
        throw new Error(`config.dataIsFinal must be an object. Received: ${JSON.stringify(config.dataIsFinal)}`);
    }
    if (typeof config.dataIsFinal.detectionMethod === 'undefined') {
        throw new Error("config.dataIsFinal.detectionMethod is required.");
    }
    if (typeof config.dataIsFinal.detectionMethod !== 'string' || (config.dataIsFinal.detectionMethod !== 'EXPORT_TYPE' && config.dataIsFinal.detectionMethod !== 'DAY_THRESHOLD')) {
        throw new Error(`config.dataIsFinal.detectionMethod must be 'EXPORT_TYPE' or 'DAY_THRESHOLD'. Received: ${JSON.stringify(config.dataIsFinal.detectionMethod)}`);
    }
    if (
        config.dataIsFinal.detectionMethod === 'DAY_THRESHOLD' && 
        typeof config.dataIsFinal.dayThreshold === 'undefined'
    ) {
        throw new Error("config.dataIsFinal.dayThreshold is required when detectionMethod is 'DAY_THRESHOLD'.");
    }
    if (
        config.dataIsFinal.detectionMethod === 'DAY_THRESHOLD' && 
        (typeof config.dataIsFinal.dayThreshold !== 'number' || !Number.isInteger(config.dataIsFinal.dayThreshold) || config.dataIsFinal.dayThreshold < 0)
    ) {
        throw new Error(`config.dataIsFinal.dayThreshold must be a non-negative integer. Received: ${JSON.stringify(config.dataIsFinal.dayThreshold)}`);
    }

    // test - optional; when defined, must be a boolean
    if (typeof config.test !== 'undefined' && typeof config.test !== 'boolean') {
        throw new Error(`config.test must be a boolean when defined. Received: ${JSON.stringify(config.test)}`);
    }

    // testConfig - optional; when included, must be an object with optional dateRangeStart and dateRangeEnd
    if (typeof config.testConfig !== 'undefined') {
        if (!config.testConfig || typeof config.testConfig !== 'object' || Array.isArray(config.testConfig)) {
            throw new Error(`config.testConfig must be an object when included. Received: ${JSON.stringify(config.testConfig)}`);
        }
        if (config.testConfig.dateRangeStart !== undefined && (typeof config.testConfig.dateRangeStart !== 'string' || !config.testConfig.dateRangeStart.trim())) {
            throw new Error(`config.testConfig.dateRangeStart must be a non-empty string (SQL date expression) when provided. Received: ${JSON.stringify(config.testConfig.dateRangeStart)}`);
        }
        if (config.testConfig.dateRangeEnd !== undefined && (typeof config.testConfig.dateRangeEnd !== 'string' || !config.testConfig.dateRangeEnd.trim())) {
            throw new Error(`config.testConfig.dateRangeEnd must be a non-empty string (SQL date expression) when provided. Received: ${JSON.stringify(config.testConfig.dateRangeEnd)}`);
        }
    }

    // bufferDays - required
    if (typeof config.bufferDays !== 'number' || !Number.isInteger(config.bufferDays) || config.bufferDays < 0) {
        throw new Error(`config.bufferDays must be a non-negative integer. Received: ${JSON.stringify(config.bufferDays)}`);
    }

    // preOperations - required
    if (config.preOperations === undefined) {
        throw new Error("config.preOperations is required.");
    }
    if (!config.preOperations || typeof config.preOperations !== 'object' || Array.isArray(config.preOperations)) {
        throw new Error(`config.preOperations must be an object. Received: ${JSON.stringify(config.preOperations)}`);
    }
    if (config.preOperations.numberOfPreviousDaysToScan === undefined) {
        throw new Error("config.preOperations.numberOfPreviousDaysToScan is required.");
    }
    const v = config.preOperations.numberOfPreviousDaysToScan;
    if (typeof v !== 'number' || isNaN(v) || !Number.isInteger(v) || v < 0) {
        throw new Error(`config.preOperations.numberOfPreviousDaysToScan must be a non-negative integer. Received: ${JSON.stringify(v)}`);
    }
    if (config.preOperations.dateRangeStartFullRefresh === undefined || config.preOperations.dateRangeStartFullRefresh === null) {
        throw new Error("config.preOperations.dateRangeStartFullRefresh is required.");
    }
    if (typeof config.preOperations.dateRangeStartFullRefresh !== 'string' || !config.preOperations.dateRangeStartFullRefresh.trim()) {
        throw new Error(`config.preOperations.dateRangeStartFullRefresh must be a non-empty string (SQL date expression). Received: ${JSON.stringify(config.preOperations.dateRangeStartFullRefresh)}`);
    }
    if (config.preOperations.dateRangeEnd === undefined || config.preOperations.dateRangeEnd === null) {
        throw new Error("config.preOperations.dateRangeEnd is required.");
    }
    if (typeof config.preOperations.dateRangeEnd !== 'string' || !config.preOperations.dateRangeEnd.trim()) {
        throw new Error(`config.preOperations.dateRangeEnd must be a non-empty string (SQL date expression). Received: ${JSON.stringify(config.preOperations.dateRangeEnd)}`);
    }
    if (config.preOperations.incrementalStartOverride !== undefined && config.preOperations.incrementalStartOverride !== null && config.preOperations.incrementalStartOverride !== '') {
        if (typeof config.preOperations.incrementalStartOverride !== 'string' || !config.preOperations.incrementalStartOverride.trim()) {
            throw new Error(`config.preOperations.incrementalStartOverride must be a non-empty string when provided. Received: ${JSON.stringify(config.preOperations.incrementalStartOverride)}`);
        }
    }
    if (config.preOperations.incrementalEndOverride !== undefined && config.preOperations.incrementalEndOverride !== null && config.preOperations.incrementalEndOverride !== '') {
        if (typeof config.preOperations.incrementalEndOverride !== 'string' || !config.preOperations.incrementalEndOverride.trim()) {
            throw new Error(`config.preOperations.incrementalEndOverride must be a non-empty string when provided. Received: ${JSON.stringify(config.preOperations.incrementalEndOverride)}`);
        }
    }

    // Array fields - all required
    const stringArrayKeys = ['defaultExcludedEventParams', 'excludedEventParams', 'sessionParams', 'defaultExcludedEvents', 'excludedEvents', 'excludedColumns'];
    for (const key of stringArrayKeys) {
        if (config[key] === undefined) {
            throw new Error(`config.${key} is required.`);
        }
        if (!Array.isArray(config[key])) {
            throw new Error(`config.${key} must be an array. Received: ${JSON.stringify(config[key])}`);
        }
        for (let i = 0; i < config[key].length; i++) {
            if (typeof config[key][i] !== 'string' || !config[key][i].trim()) {
                throw new Error(`config.${key}[${i}] must be a non-empty string. Received: ${JSON.stringify(config[key][i])}`);
            }
        }
    }

    // eventParamsToColumns - required
    if (config.eventParamsToColumns === undefined) {
        throw new Error("config.eventParamsToColumns is required.");
    }
    if (!Array.isArray(config.eventParamsToColumns)) {
        throw new Error(`config.eventParamsToColumns must be an array. Received: ${JSON.stringify(config.eventParamsToColumns)}`);
    }
    const validEventParamTypes = ['string', 'int', 'int64', 'double', 'float', 'float64'];
    for (let i = 0; i < config.eventParamsToColumns.length; i++) {
        const item = config.eventParamsToColumns[i];
        if (!item || typeof item !== 'object' || Array.isArray(item)) {
            throw new Error(`config.eventParamsToColumns[${i}] must be an object with 'name' and 'type' properties. Received: ${JSON.stringify(item)}`);
        }
        if (!item.name || typeof item.name !== 'string' || !item.name.trim()) {
            throw new Error(`config.eventParamsToColumns[${i}].name must be a non-empty string. Received: ${JSON.stringify(item.name)}`);
        }
        if (item.type !== undefined && item.type !== null) {
            if (!validEventParamTypes.includes(item.type)) {
                throw new Error(`config.eventParamsToColumns[${i}].type must be one of: ${validEventParamTypes.join(', ')}. Received: ${JSON.stringify(item.type)}`);
            }
        }
        if (item.columnName !== undefined && item.columnName !== null && item.columnName !== '') {
            if (typeof item.columnName !== 'string' || !item.columnName.trim()) {
                throw new Error(`config.eventParamsToColumns[${i}].columnName must be a non-empty string when provided. Received: ${JSON.stringify(item.columnName)}`);
            }
        }
    }
};

module.exports = {
    mergeUniqueArrays,
    mergeSQLConfigurations,
    mergeDataformTableConfigurations,
    queryBuilder,
    isDataformTableReferenceObject,
    setDataformContext,
    selectOtherColumns,
    processDate,
    validateConfig
};