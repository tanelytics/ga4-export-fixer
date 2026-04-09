/**
 * Validates the base configuration fields shared across all table types.
 * These correspond to the fields defined in baseConfig (defaultConfig.js):
 * self, incremental, test, testConfig, and preOperations.
 *
 * @param {Object} config - The merged configuration object to validate.
 * @param {Object} [options] - Validation options.
 * @param {boolean} [options.skipDataformContextFields=false] - Skip validation of `self` and `incremental`,
 *   which are set by Dataform's publish() context rather than user input.
 * @throws {Error} If any base configuration value is invalid or missing.
 */
const validateBaseConfig = (config, options = {}) => {
    if (!config || typeof config !== 'object' || Array.isArray(config)) {
        throw new Error(`config must be a non-null object. Received: ${JSON.stringify(config)}`);
    }

    if (!options.skipDataformContextFields) {
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
    if (config.preOperations.numberOfDaysToProcess !== undefined) {
        const nd = config.preOperations.numberOfDaysToProcess;
        if (typeof nd !== 'number' || isNaN(nd) || !Number.isInteger(nd) || nd < 1) {
            throw new Error(`config.preOperations.numberOfDaysToProcess must be a positive integer when defined. Received: ${JSON.stringify(nd)}`);
        }
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

module.exports = {
    validateBaseConfig,
};
