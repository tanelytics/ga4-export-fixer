const helpers = require('../../../helpers/index.js');

/**
 * Builds a _table_suffix date filter for the assertion's raw-side query.
 *
 * Uses the low-level ga4ExportDateFilter() helper per enabled export type
 * over a caller-provided lookback window. Intentionally separate from the
 * pipeline's ga4ExportDateFilters() which depends on incremental state
 * and BigQuery pre-operation variables.
 *
 * @param {Object} includedExportTypes - { daily: boolean, fresh: boolean, intraday: boolean }
 * @param {number} lookbackDays - Number of days to look back from current_date().
 * @returns {string} SQL fragment for a WHERE clause
 */
const buildAssertionDateFilter = (includedExportTypes, lookbackDays) => {
    const start = `date_sub(current_date(), interval ${lookbackDays} day)`;
    const end = 'current_date()';

    const filters = [
        includedExportTypes.daily ? helpers.ga4ExportDateFilter('daily', start, end) : null,
        includedExportTypes.fresh ? helpers.ga4ExportDateFilter('fresh', start, end) : null,
        includedExportTypes.intraday ? helpers.ga4ExportDateFilter('intraday', start, end) : null,
    ].filter(Boolean);

    return filters.join(' or ');
};

/**
 * Builds a deduplicated raw-source subquery for assertion use.
 *
 * Replicates what setPreOperations() does at pipeline time, without access
 * to its BigQuery variables. Covers all seven combinations of
 * includedExportTypes {daily, fresh, intraday}:
 *
 *   - qualify dense_rank() over (partition by date, order by _table_suffix) = 1
 *     picks the highest-priority table per day. Alphabetical order gives
 *     daily ('20260115') < fresh ('fresh_20260115') < intraday ('intraday_20260115'),
 *     matching the pipeline's daily > fresh > intraday priority.
 *   - When fresh and intraday are both enabled, intraday rows with
 *     event_timestamp > max(fresh.event_timestamp) for the same date are
 *     additionally admitted — matching the FRESH_MAX_EVENT_TIMESTAMP boundary.
 *
 * @param {Object} mergedConfig - Merged table configuration.
 * @param {number} lookbackDays - Number of days to look back from current_date().
 * @returns {string} SQL fragment: a parenthesized subquery usable in a FROM clause.
 */
const buildDedupedRawSource = (mergedConfig, lookbackDays) => {
    const dateFilter = buildAssertionDateFilter(mergedConfig.includedExportTypes, lookbackDays);
    const freshAndIntraday = mergedConfig.includedExportTypes.fresh && mergedConfig.includedExportTypes.intraday;

    const intradayException = freshAndIntraday
        ? `
        or (
            starts_with(_table_suffix, 'intraday_')
            and dense_rank() over (
                partition by regexp_extract(_table_suffix, r'[0-9]+')
                order by _table_suffix
            ) = 2
            and event_timestamp > max(if(starts_with(_table_suffix, 'fresh_'), event_timestamp, null)) over (
                partition by regexp_extract(_table_suffix, r'[0-9]+')
            )
        )`
        : '';

    return `(
    select
        *
    from
        ${mergedConfig.sourceTable}
    where
        (${dateFilter})
    qualify
        dense_rank() over (
            partition by regexp_extract(_table_suffix, r'[0-9]+')
            order by _table_suffix
        ) = 1${intradayException}
)`;
};

module.exports = { buildAssertionDateFilter, buildDedupedRawSource };
