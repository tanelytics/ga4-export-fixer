const constants = require('../constants');
const { baseConfig } = require('../defaultConfig');

// Filter the export tables by date range
/**
 * Generates a SQL filter condition for selecting GA4 export tables based on the export type and a date range.
 *
 * This helper produces SQL snippets to be used in WHERE clauses, ensuring only tables within the provided date range and export type are included.
 *
 * - For 'daily' exports: Matches table suffixes formatted as YYYYMMDD (e.g., 20240101).
 * - For 'fresh' exports: Matches table suffixes prefixed with 'fresh_' followed by the date (e.g., fresh_20240101).
 * - For 'intraday' exports: Matches table suffixes prefixed with 'intraday_' followed by the date (e.g., intraday_20240101).
 *
 * @param {'daily'|'fresh'|'intraday'} exportType - The type of export table.
 * @param {string} start - The start date value as a SQL date expression (e.g. 'current_date()-1').
 * @param {string} end - The end date value as a SQL date expression (e.g. 'current_date()').
 * @returns {string} SQL condition to restrict tables by _table_suffix to the appropriate date range and export type.
 *
 * @throws {Error} If exportType is not supported, or if start/end are not defined.
 */
const ga4ExportDateFilter = (exportType, start, end) => {
  if (exportType !== 'intraday' && exportType !== 'daily' && exportType !== 'fresh') {
    throw new Error(
      `ga4ExportDateFilter: Unsupported exportType '${exportType}'. Supported values are 'daily', 'fresh', and 'intraday'.`
    );
  }
  if (typeof start === 'undefined' || typeof end === 'undefined') {
    throw new Error("ga4ExportDateFilter: 'start' and 'end' parameters must be defined.");
  }

  const prefix = exportType === 'daily' ? '' : `'${exportType}_' || `;
  return `(_table_suffix >= ${prefix}cast(${start} as string format "YYYYMMDD") and _table_suffix <= ${prefix}cast(${end} as string format "YYYYMMDD"))`;
};

/**
 * Builds a `_table_suffix` WHERE clause for GA4 BigQuery export tables (daily, fresh, and/or intraday).
 *
 * Date boundaries are resolved differently depending on the mode:
 *   - **test** -- literal dates from `config.testConfig`
 *   - **incremental** -- BigQuery variable placeholders set by pre-operations
 *   - **full refresh** -- static dates from `config.preOperations`
 *
 * `bufferDays` is subtracted from the daily start date so sessions that span
 * midnight are not partially excluded.
 *
 * Export priority: daily > fresh > intraday. Each lower-priority export only
 * provides data not already covered by a higher-priority one.
 *
 * When fresh and daily are both enabled, the fresh start date comes from
 * `FRESH_DATE_RANGE_START_VARIABLE` (first day with fresh but no daily table).
 *
 * When fresh and intraday are both enabled, intraday rows are filtered by
 * `event_timestamp > fresh_max_event_timestamp` to avoid duplicating fresh data.
 *
 * When only daily and intraday are enabled (no fresh), the existing
 * `INTRADAY_DATE_RANGE_START_VARIABLE` checkpoint logic is preserved.
 *
 * @param {Object} config
 * @param {boolean}  config.test                      - Use literal test dates.
 * @param {Object}   config.testConfig                - `{ dateRangeStart, dateRangeEnd }`.
 * @param {boolean}  config.incremental               - Use BigQuery variable placeholders.
 * @param {Object}   config.preOperations             - `{ dateRangeStartFullRefresh, dateRangeEnd }`.
 * @param {Object}   config.includedExportTypes       - `{ daily: boolean, fresh: boolean, intraday: boolean }`.
 * @param {number}  [config.bufferDays=0]             - Extra days subtracted from the start date.
 * @returns {string} SQL fragment for a WHERE clause.
 */
const ga4ExportDateFilters = (config) => {
  const bufferDays = config.bufferDays || 0;

  const getStartDate = () => {
    if (config.test) {
      return config.testConfig.dateRangeStart;
    }
    if (config.incremental) {
      return constants.DATE_RANGE_START_VARIABLE;
    }
    return config.preOperations.dateRangeStartFullRefresh;
  };

  const getEndDate = () => {
    if (config.test) {
      return config.testConfig.dateRangeEnd;
    }
    if (config.incremental) {
      return constants.DATE_RANGE_END_VARIABLE;
    }
    if (config.preOperations.numberOfDaysToProcess !== undefined) {
      return `least(${config.preOperations.dateRangeStartFullRefresh}+${config.preOperations.numberOfDaysToProcess}-1, current_date())`;
    }
    return config.preOperations.dateRangeEnd;
  };

  const getFreshStartDate = () => {
    // Fresh tables persist alongside daily tables (unlike intraday which gets deleted),
    // so the checkpoint variable is needed even in test mode to avoid duplicate data.
    if (config.includedExportTypes.fresh && config.includedExportTypes.daily) {
      return constants.FRESH_DATE_RANGE_START_VARIABLE;
    }
    if (config.includedExportTypes.fresh && !config.includedExportTypes.daily) {
      return getStartDate();
    }
  };

  const getIntradayStartDate = () => {
    // When fresh is enabled: intraday starts from the same point as fresh.
    // Fresh tables persist alongside intraday tables, so the checkpoint is
    // needed even in test mode to avoid duplicate data.
    if (config.includedExportTypes.fresh) {
      return getFreshStartDate();
    }
    // For non-fresh paths, test mode skips pre-operation variables.
    if (config.test) {
      return config.testConfig.dateRangeStart;
    }
    // When daily+intraday without fresh: use the existing date-based checkpoint
    if (config.includedExportTypes.intraday && config.includedExportTypes.daily) {
      return constants.INTRADAY_DATE_RANGE_START_VARIABLE;
    }
    // Intraday-only: reuse the daily start-date logic with bufferDays
    if (config.includedExportTypes.intraday && !config.includedExportTypes.daily) {
      return `${getStartDate()}-${bufferDays}`;
    }
  };

  const getIntradayFilter = () => {
    const intradayStart = getIntradayStartDate();
    const suffixFilter = ga4ExportDateFilter('intraday', intradayStart, end);

    // When fresh is also enabled, add timestamp condition to avoid duplicating fresh data.
    // Applied even in test mode because fresh and intraday tables coexist for the same days.
    if (config.includedExportTypes.fresh) {
      return `(${suffixFilter} and event_timestamp > coalesce(${constants.FRESH_MAX_EVENT_TIMESTAMP_VARIABLE}, 0))`;
    }

    return suffixFilter;
  };

  const dailyStart = `${getStartDate()}-${bufferDays}`;
  const freshStart = getFreshStartDate();
  const end = getEndDate();

  const dateFilters = [
    config.includedExportTypes.daily ? ga4ExportDateFilter('daily', dailyStart, end) : null,
    config.includedExportTypes.fresh ? ga4ExportDateFilter('fresh', freshStart, end) : null,
    config.includedExportTypes.intraday ? getIntradayFilter() : null,
  ];

  return `(
    ${dateFilters.filter(filter => !!filter).join(' or ')}
  )`;
};

/**
 * Generates a SQL filter condition for restricting event data to a specific date range.
 *
 * This function is used to dynamically create a WHERE clause for filtering the `event_date`
 * based on the provided configuration. It handles three primary scenarios:
 *   1. **Test Mode (`config.test`)**: Uses explicit start and end dates from the test configuration.
 *   2. **Incremental Refresh (`config.incremental`)**: Uses BigQuery variable placeholders
 *      for efficient incremental queries (`constants.DATE_RANGE_START_VARIABLE` and
 *      `constants.DATE_RANGE_END_VARIABLE`).
 *   3. **Full Refresh (default)**: Uses static start and end dates from the standard config,
 *      generally for full table rebuilds.
 *
 * This behavior ensures that query cost estimation in BigQuery remains accurate by avoiding
 * variable use in non-incremental queries.
 *
 * @param {Object} config - Configuration object controlling the date filter logic.
 *   @param {boolean} [config.test] - If true, uses explicit test dates.
 *   @param {Object} [config.testConfig] - Contains `dateRangeStart` and `dateRangeEnd` for testing.
 *   @param {boolean} [config.incremental] - If true, uses variable placeholders for incremental queries.
 *   @param {Object} [config.preOperations] - Contains full refresh date range values.
 * @returns {string} - SQL condition string to filter the query by date range.
 */
const incrementalDateFilter = (config) => {
  const setDateRange = (start, end) => {
    return `(event_date >= ${start} and event_date <= ${end})`;
  };

  // test mode
  if (config.test) {
    const testStart = config?.testConfig?.dateRangeStart || baseConfig.testConfig.dateRangeStart;
    const testEnd = config?.testConfig?.dateRangeEnd || baseConfig.testConfig.dateRangeEnd;

    return setDateRange(testStart, testEnd);
  }

  // incremental mode
  if (config.incremental) {
    return setDateRange(constants.DATE_RANGE_START_VARIABLE, constants.DATE_RANGE_END_VARIABLE);
  }

  // full refresh mode
  const fullRefreshStart = config?.preOperations?.dateRangeStartFullRefresh || baseConfig.preOperations.dateRangeStartFullRefresh;
  const fullRefreshEnd = config?.preOperations?.numberOfDaysToProcess !== undefined
    ? `least(${fullRefreshStart}+${config.preOperations.numberOfDaysToProcess}-1, current_date())`
    : (config?.preOperations?.dateRangeEnd || baseConfig.preOperations.dateRangeEnd);

  return setDateRange(fullRefreshStart, fullRefreshEnd);
};

module.exports = {
  ga4ExportDateFilter,
  ga4ExportDateFilters,
  incrementalDateFilter
};
