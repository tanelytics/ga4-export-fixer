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
 * The query runs two aggregations for the last 5 days and unions the violations:
 *   - Day grain: (event_date, data_is_final) -- session/event counts, item_revenue,
 *     ecommerce.purchase_revenue.
 *   - Item-id grain: (event_date, item_id) on purchase events for days both sides
 *     consider final -- validates per-item_id revenue and item-row count.
 *
 * Returns violating rows -- 0 rows means the assertion passes.
 *
 * Eight violation types are detected:
 * - MISSING_DAY: Raw data has events but enhanced table has none for this day
 * - SESSION_COUNT_MISMATCH: Final data session count differs
 * - EVENT_COUNT_MISMATCH: Final data event count differs
 * - ITEM_REVENUE_MISMATCH: Final data total item_revenue differs
 * - PURCHASE_REVENUE_MISMATCH: Final data total ecommerce.purchase_revenue differs
 *   (raw side applies fixEcommerceStruct() to mirror the enhanced pipeline's fix)
 * - NON_FINAL_EXCESS_EVENTS: Non-final enhanced data has more events than raw
 * - ITEM_REVENUE_MISMATCH_BY_ID: Per-item_id item_revenue differs on a shared-final day
 * - ITEM_COUNT_MISMATCH_BY_ID: Per-item_id purchase item-row count differs on a shared-final day
 *
 * Day-level rows leave item_id / enhanced_item_count / raw_item_count NULL.
 * Item-id-level rows leave session / event / purchase_revenue columns NULL.
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
),
enhanced_items as (
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
        and event_date >= date_sub(current_date(), interval ${ASSERTION_LOOKBACK_DAYS} day)
        and event_name = 'purchase'
    group by event_date, item.item_id
),
raw_items as (
    select
        cast(event_date as date format 'YYYYMMDD') as event_date,
        item.item_id,
        sum(item.item_revenue) as total_item_revenue,
        count(*) as item_count
    from
        ${dedupedRawSource},
        unnest(items) as item
    where
        ${excludedEventsSQL}
        and event_name = 'purchase'
        and ${dataIsFinalCondition}
    group by event_date, item.item_id
),
shared_final_days as (
    select event_date
    from daily_comparison
    where data_is_final = true
        and enhanced_events is not null
        and raw_events is not null
),
item_comparison as (
    select
        coalesce(e.event_date, r.event_date) as event_date,
        coalesce(e.item_id, r.item_id) as item_id,
        round(e.total_item_revenue, 2) as enhanced_item_revenue,
        round(r.total_item_revenue, 2) as raw_item_revenue,
        e.item_count as enhanced_item_count,
        r.item_count as raw_item_count
    from
        enhanced_items e
    full outer join
        raw_items r using(event_date, item_id)
    where
        coalesce(e.event_date, r.event_date) in (select event_date from shared_final_days)
)
select
    event_date,
    data_is_final,
    null as item_id,
    enhanced_sessions,
    raw_sessions,
    enhanced_events,
    raw_events,
    enhanced_item_revenue,
    raw_item_revenue,
    enhanced_purchase_revenue,
    raw_purchase_revenue,
    cast(null as int64) as enhanced_item_count,
    cast(null as int64) as raw_item_count,
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
    violation_type is not null
union all
select
    event_date,
    true as data_is_final,
    item_id,
    cast(null as int64) as enhanced_sessions,
    cast(null as int64) as raw_sessions,
    cast(null as int64) as enhanced_events,
    cast(null as int64) as raw_events,
    enhanced_item_revenue,
    raw_item_revenue,
    cast(null as float64) as enhanced_purchase_revenue,
    cast(null as float64) as raw_purchase_revenue,
    enhanced_item_count,
    raw_item_count,
    violation_type
from
    item_comparison,
    unnest([
        if(round(coalesce(enhanced_item_revenue, 0), 2) != round(coalesce(raw_item_revenue, 0), 2), 'ITEM_REVENUE_MISMATCH_BY_ID', null),
        if(coalesce(enhanced_item_count, 0) != coalesce(raw_item_count, 0), 'ITEM_COUNT_MISMATCH_BY_ID', null)
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
 * plus a per-item_id revenue/row-count check on purchase events for shared-final
 * days. Also checks for missing days and non-final data inflation.
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
