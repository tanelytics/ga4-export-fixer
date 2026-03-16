const { isDataformTableReferenceObject } = require('./utils.js');

/**
 * Validates the base configuration fields shared across all table types.
 * These correspond to the fields defined in baseConfig (defaultConfig.js):
 * self, incremental, test, testConfig, and preOperations.
 *
 * @param {Object} config - The merged configuration object to validate.
 * @throws {Error} If any base configuration value is invalid or missing.
 */
const validateBaseConfig = (config) => {
    if (!config || typeof config !== 'object' || Array.isArray(config)) {
        throw new Error(`config must be a non-null object. Received: ${JSON.stringify(config)}`);
    }

    // self - required, must be valid format
    if (config.test !== true) {
        if (typeof config.self !== 'string' || !config.self.trim() || !/^`[^`]+`$/.test(config.self.trim())) {
            throw new Error(`config.self is required when config.test !== true and must be a non-empty string in format '\`project.dataset.table\`' (using the ref() function). Received: ${JSON.stringify(config.self)}`);
        }
    }

    // incremental - required, must be boolean
    if (typeof config.incremental !== 'boolean') {
        throw new Error(`config.incremental must be a boolean. Received: ${JSON.stringify(config.incremental)}`);
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
const validateEnhancedEventsConfig = (config) => {
  try {
    if (!config || typeof config !== 'object' || Array.isArray(config)) {
        throw new Error(`config must be a non-null object. Received: ${JSON.stringify(config)}`);
    }

    // base config fields (self, incremental, test, testConfig, preOperations)
    validateBaseConfig(config);

    /*
    Rest of the validations are related to ga4_events_enhanced table specific fields
    */

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
        if (!(key in config.includedExportTypes)) {
            throw new Error(`config.includedExportTypes.${key} is required.`);
        }
        if (typeof config.includedExportTypes[key] !== 'boolean') {
            throw new Error(`config.includedExportTypes.${key} must be a boolean. Received: ${JSON.stringify(config.includedExportTypes[key])}`);
        }
    }
    if (!config.includedExportTypes.daily && !config.includedExportTypes.intraday) {
        throw new Error("At least one of config.includedExportTypes.daily or config.includedExportTypes.intraday must be true.");
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
    // EXPORT_TYPE detection relies on daily export metadata; intraday-only requires DAY_THRESHOLD instead.
    if (
        config.includedExportTypes.intraday &&
        !config.includedExportTypes.daily &&
        config.dataIsFinal.detectionMethod !== 'DAY_THRESHOLD'
    ) {
        throw new Error(`config.dataIsFinal.detectionMethod must be 'DAY_THRESHOLD' when only intraday export is enabled (config.includedExportTypes.daily is false). A dayThreshold of 1 is recommended for intraday-only configurations. Received: ${JSON.stringify(config.dataIsFinal.detectionMethod)}`);
    }

    // bufferDays - required
    if (typeof config.bufferDays !== 'number' || !Number.isInteger(config.bufferDays) || config.bufferDays < 0) {
        throw new Error(`config.bufferDays must be a non-negative integer. Received: ${JSON.stringify(config.bufferDays)}`);
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
  } catch (e) {
    e.message = `Config validation: ${e.message}`;
    throw e;
  }
};

module.exports = {
    validateBaseConfig,
    validateEnhancedEventsConfig
};
