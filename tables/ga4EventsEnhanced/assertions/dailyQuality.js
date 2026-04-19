const helpers = require('../../../helpers/index.js');
const utils = require('../../../utils.js');
const { ga4EventsEnhancedConfig } = require('../config.js');
const { validateEnhancedEventsConfig } = require('../validation.js');
const { buildDedupedRawSource } = require('./shared.js');

const defaultConfig = { ...ga4EventsEnhancedConfig };

const ASSERTION_LOOKBACK_DAYS = 5;

/**
 * Generates a SQL assertion query that validates daily data quality between the
 * enhanced events table and the raw GA4 export data.
 *
 * The query compares session count, event count, total item_revenue, and total
 * purchase_revenue aggregated per (event_date, data_is_final) for the last 5
 * days. Returns violating rows -- 0 rows means the assertion passes.
 *
 * Six violation types are detected:
 * - MISSING_DAY: Raw data has events but enhanced table has none for this day
 * - SESSION_COUNT_MISMATCH: Final data session count differs
 * - EVENT_COUNT_MISMATCH: Final data event count differs
 * - ITEM_REVENUE_MISMATCH: Final data total item_revenue differs
 * - PURCHASE_REVENUE_MISMATCH: Final data total ecommerce.purchase_revenue differs
 *   (raw side applies fixEcommerceStruct() to mirror the enhanced pipeline's fix)
 * - NON_FINAL_EXCESS_EVENTS: Non-final enhanced data has more events than raw
 *
 * @param {string} tableRef - Fully qualified reference to the enhanced table
 * @param {Object} mergedConfig - Merged table configuration (after merge + validation)
 * @returns {string} SQL query returning violating rows
 */
const _generateDailyQualityAssertionSql = (tableRef, mergedConfig) => {
    const excludedEvents = mergedConfig.excludedEvents;
    const excludedEventsSQL = excludedEvents.length > 0
        ? `event_name not in (${excludedEvents.map(e => `'${e}'`).join(', ')})`
        : 'true';

    const dataIsFinalCondition = helpers.isFinalData(
        mergedConfig.dataIsFinal.detectionMethod,
        mergedConfig.dataIsFinal.dayThreshold
    );

    const dedupedRawSource = buildDedupedRawSource(mergedConfig, ASSERTION_LOOKBACK_DAYS);

    return `with enhanced_daily as (
    select
        event_date,
        data_is_final,
        count(distinct session_id) as session_count,
        count(*) as event_count,
        coalesce(sum((select sum(item.item_revenue) from unnest(items) as item)), 0) as total_item_revenue,
        coalesce(sum(ecommerce.purchase_revenue), 0) as total_purchase_revenue
    from
        ${tableRef}
    where
        event_date >= date_sub(current_date(), interval ${ASSERTION_LOOKBACK_DAYS} day)
    group by event_date, data_is_final
),
raw_daily as (
    select
        cast(event_date as date format 'YYYYMMDD') as event_date,
        ${dataIsFinalCondition} as data_is_final,
        count(distinct concat(user_pseudo_id, cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string))) as session_count,
        count(*) as event_count,
        coalesce(sum((select sum(item.item_revenue) from unnest(items) as item)), 0) as total_item_revenue,
        coalesce(sum(${helpers.fixEcommerceStruct()}.purchase_revenue), 0) as total_purchase_revenue
    from
        ${dedupedRawSource}
    where
        ${excludedEventsSQL}
    group by event_date, data_is_final
),
daily_comparison as (
    select
        coalesce(e.event_date, r.event_date) as event_date,
        coalesce(e.data_is_final, r.data_is_final) as data_is_final,
        e.session_count as enhanced_sessions,
        r.session_count as raw_sessions,
        e.event_count as enhanced_events,
        r.event_count as raw_events,
        round(e.total_item_revenue, 2) as enhanced_item_revenue,
        round(r.total_item_revenue, 2) as raw_item_revenue,
        round(e.total_purchase_revenue, 2) as enhanced_purchase_revenue,
        round(r.total_purchase_revenue, 2) as raw_purchase_revenue
    from
        enhanced_daily e
    full outer join
        raw_daily r using(event_date, data_is_final)
)
select
    event_date,
    data_is_final,
    enhanced_sessions,
    raw_sessions,
    enhanced_events,
    raw_events,
    enhanced_item_revenue,
    raw_item_revenue,
    enhanced_purchase_revenue,
    raw_purchase_revenue,
    violation_type
from
    daily_comparison,
    unnest([
        if(enhanced_events is null and raw_events > 0, 'MISSING_DAY', null),
        if(data_is_final = true and enhanced_sessions != raw_sessions, 'SESSION_COUNT_MISMATCH', null),
        if(data_is_final = true and enhanced_events != raw_events, 'EVENT_COUNT_MISMATCH', null),
        if(data_is_final = true and enhanced_item_revenue != raw_item_revenue, 'ITEM_REVENUE_MISMATCH', null),
        if(data_is_final = true and enhanced_purchase_revenue != raw_purchase_revenue, 'PURCHASE_REVENUE_MISMATCH', null),
        if(data_is_final = false and coalesce(enhanced_events, 0) > coalesce(raw_events, 0), 'NON_FINAL_EXCESS_EVENTS', null)
    ]) as violation_type
where
    violation_type is not null`;
};

/**
 * Generates a daily quality assertion SQL query.
 *
 * Merges the provided config with defaults, validates, then generates a SQL
 * query comparing daily aggregates (session count, event count, item_revenue,
 * ecommerce.purchase_revenue) between the enhanced table and raw export data,
 * and checks for missing days and non-final data inflation.
 *
 * @param {string} tableRef - Fully qualified reference to the enhanced table.
 * @param {Object} config - User-provided table configuration.
 * @returns {string} SQL query returning violating rows (0 rows = pass)
 */
const generateDailyQualityAssertionSql = (tableRef, config) => {
    if (!tableRef || typeof tableRef !== 'string' || !tableRef.trim()) {
        throw new Error('assertions.dailyQuality: tableRef is required and must be a non-empty string (e.g., ctx.ref(\'table_name\') or \'`project.dataset.table`\').');
    }
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);

    if (utils.isDataformTableReferenceObject(mergedConfig.sourceTable)) {
        throw new Error(
            'assertions.dailyQuality: config.sourceTable is a Dataform table reference object, but assertions do not have access to Dataform context to resolve it. ' +
            'Resolve it with ctx.ref() before passing it to the assertion:\n\n' +
            '  .query(ctx => ga4EventsEnhanced.assertions.dailyQuality(\n' +
            '    ctx.ref(\'enhanced_table_name\'),\n' +
            '    { ...config, sourceTable: ctx.ref(config.sourceTable) }\n' +
            '  ))'
        );
    }

    validateEnhancedEventsConfig(mergedConfig, { skipDataformContextFields: true });
    return _generateDailyQualityAssertionSql(tableRef, mergedConfig);
};

module.exports = { generateDailyQualityAssertionSql, _generateDailyQualityAssertionSql };
