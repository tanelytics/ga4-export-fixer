const helpers = require('../../../helpers/index.js');
const utils = require('../../../utils.js');
const { ga4EventsEnhancedConfig } = require('../config.js');
const { validateEnhancedEventsConfig } = require('../validation.js');

const defaultConfig = { ...ga4EventsEnhancedConfig };

// Ecommerce events that carry item data (excluding refund — refunds reverse revenue
// and are handled separately in some pipelines, but item_revenue on refund rows
// should still reconcile 1:1 between enhanced and raw).
const ecommerceEvents = helpers.ga4EcommerceEvents.map(e => `'${e}'`).join(', ');

/**
 * Builds a _table_suffix date filter for the assertion's raw-side query.
 *
 * Uses the low-level ga4ExportDateFilter() helper per enabled export type
 * with a fixed 5-day lookback window. This is intentionally separate from
 * the pipeline's ga4ExportDateFilters() which depends on incremental state
 * and BigQuery pre-operation variables.
 *
 * @param {Object} includedExportTypes - { daily: boolean, fresh: boolean, intraday: boolean }
 * @returns {string} SQL fragment for a WHERE clause
 */
const buildAssertionDateFilter = (includedExportTypes) => {
    const start = 'date_sub(current_date(), interval 5 day)';
    const end = 'current_date()';

    const filters = [
        includedExportTypes.daily ? helpers.ga4ExportDateFilter('daily', start, end) : null,
        includedExportTypes.fresh ? helpers.ga4ExportDateFilter('fresh', start, end) : null,
        includedExportTypes.intraday ? helpers.ga4ExportDateFilter('intraday', start, end) : null,
    ].filter(Boolean);

    return filters.join(' or ');
};

/**
 * Generates a SQL assertion query that reconciles item_revenue between the
 * enhanced events table and the raw GA4 export data.
 *
 * The query compares item_revenue grouped by (event_date, item_id) for the
 * last 5 days of final data. Returns mismatched rows — 0 rows means the
 * assertion passes.
 *
 * @param {string} tableRef - Fully qualified reference to the enhanced table
 *   (e.g., ctx.ref('ga4_events_enhanced_123456789') in Dataform, or a backtick-quoted string).
 * @param {Object} mergedConfig - Merged table configuration (after merge + validation).
 * @returns {string} SQL query returning violating rows
 */
const _generateItemRevenueAssertionSql = (tableRef, mergedConfig) => {
    // excluded events filter (same logic as the enhanced table pipeline)
    const excludedEvents = mergedConfig.excludedEvents;
    const excludedEventsSQL = excludedEvents.length > 0
        ? `and event_name not in (${excludedEvents.map(e => `'${e}'`).join(', ')})`
        : '';

    // data_is_final condition for the raw side
    const dataIsFinalCondition = helpers.isFinalData(
        mergedConfig.dataIsFinal.detectionMethod,
        mergedConfig.dataIsFinal.dayThreshold
    );

    // date filter for the raw side (per-export-type, fixed 5-day window)
    const dateFilter = buildAssertionDateFilter(mergedConfig.includedExportTypes);

    return `with enhanced_revenue as (
    select
        event_date,
        item.item_id,
        sum(item.item_revenue) as total_item_revenue,
        count(*) as item_count
    from
        ${tableRef},
        unnest(items) as item
    where
        data_is_final = true
        and event_date >= date_sub(current_date(), interval 5 day)
        and event_name in (${ecommerceEvents})
    group by event_date, item.item_id
),
raw_revenue as (
    select
        cast(event_date as date format 'YYYYMMDD') as event_date,
        item.item_id,
        sum(item.item_revenue) as total_item_revenue,
        count(*) as item_count
    from
        ${mergedConfig.sourceTable},
        unnest(items) as item
    where
        (${dateFilter})
        ${excludedEventsSQL}
        and event_name in (${ecommerceEvents})
        and ${dataIsFinalCondition}
        and cast(event_date as date format 'YYYYMMDD') >= date_sub(current_date(), interval 5 day)
    group by event_date, item.item_id
)
select
    coalesce(e.event_date, r.event_date) as event_date,
    coalesce(e.item_id, r.item_id) as item_id,
    e.total_item_revenue as enhanced_revenue,
    r.total_item_revenue as raw_revenue,
    e.item_count as enhanced_count,
    r.item_count as raw_count
from
    enhanced_revenue e
full outer join
    raw_revenue r using(event_date, item_id)
where
    round(coalesce(e.total_item_revenue, 0), 2) != round(coalesce(r.total_item_revenue, 0), 2)
    or e.item_count != r.item_count
    or e.event_date is null
    or r.event_date is null`;
};

/**
 * Generates an item_revenue reconciliation assertion SQL query.
 *
 * Merges the provided config with defaults, validates, then generates a SQL
 * query comparing item_revenue between the enhanced table and raw export data.
 *
 * @param {string} tableRef - Fully qualified reference to the enhanced table.
 * @param {Object} config - User-provided table configuration.
 * @returns {string} SQL query returning violating rows (0 rows = pass)
 */
const generateItemRevenueAssertionSql = (tableRef, config) => {
    if (!tableRef || typeof tableRef !== 'string' || !tableRef.trim()) {
        throw new Error('assertions.itemRevenue: tableRef is required and must be a non-empty string (e.g., ctx.ref(\'table_name\') or \'`project.dataset.table`\').');
    }
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);

    // The assertion interpolates sourceTable directly into SQL (no Dataform ctx available).
    // If sourceTable is still a Dataform reference object, it would render as [object Object].
    if (utils.isDataformTableReferenceObject(mergedConfig.sourceTable)) {
        throw new Error(
            'assertions.itemRevenue: config.sourceTable is a Dataform table reference object, but assertions do not have access to Dataform context to resolve it. ' +
            'Resolve it with ctx.ref() before passing it to the assertion:\n\n' +
            '  .query(ctx => ga4EventsEnhanced.assertions.itemRevenue(\n' +
            '    ctx.ref(\'enhanced_table_name\'),\n' +
            '    { ...config, sourceTable: ctx.ref(config.sourceTable) }\n' +
            '  ))'
        );
    }

    validateEnhancedEventsConfig(mergedConfig, { skipDataformContextFields: true });
    return _generateItemRevenueAssertionSql(tableRef, mergedConfig);
};

module.exports = { generateItemRevenueAssertionSql };
