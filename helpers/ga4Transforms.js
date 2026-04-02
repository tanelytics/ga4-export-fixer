const { unnestEventParam } = require('./params');

/*
Common identifiers
*/

const sessionId = `concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id'))`;

/*
Ecommerce
*/

/**
 * Fixes and normalizes the ecommerce struct extracted from GA4 event data.
 *
 * This helper returns a SQL expression that:
 *   - Ensures `ecommerce.transaction_id` is set to NULL if it has the placeholder string '(not set)';
 *   - For 'purchase' events, normalizes `ecommerce.purchase_revenue` by:
 *       * Removing NaN values;
 *       * Filling missing purchase revenue (an old GA4 bug) with the event parameter 'value', safely cast as FLOAT64;
 *   - Leaves other fields in the ecommerce struct unchanged.
 *
 * The result is a new struct with the same shape as 'ecommerce' but with cleaned transaction_id and purchase_revenue.
 *
 * @returns {string} A SQL snippet for SELECT AS STRUCT ... REPLACE to normalize ecommerce fields.
 *
 * @example
 *   fixEcommerceStruct()
 *   // => SQL string that can be used in a SELECT list to normalize ecommerce columns
 */
const fixEcommerceStruct = () => {
  return `(select as struct ecommerce.* replace(
    if(ecommerce.transaction_id <> '(not set)', ecommerce.transaction_id, null)  as transaction_id,
    if(
      event_name = 'purchase',
      coalesce(
        -- fix possible NaN values
        if(is_nan(ecommerce.purchase_revenue), null, ecommerce.purchase_revenue),
        -- fix an old ga4 bug where purchase_revenue was missing
        safe_cast(${unnestEventParam('value')} as float64)
      ),
      null
    ) as purchase_revenue
  ))`;
};

/*
Check if GA4 data is "final" and is not expected to change anymore
*/

/**
 * Generates a SQL expression to determine whether GA4 export data can be considered "final" (not subject to further change).
 *
 * Two detection methods are supported:
 *   - 'EXPORT_TYPE': Checks the table suffix; returns FALSE for intraday or "fresh" tables, TRUE for finalized data.
 *   - 'DAY_THRESHOLD': Considers data final if a configurable number of days has passed since event_date.
 *
 * @param {'EXPORT_TYPE'|'DAY_THRESHOLD'} detectionMethod - The method to use for finality determination.
 *        'EXPORT_TYPE': Uses patterns in _table_suffix (e.g., 'intraday_%', 'fresh_%').
 *        'DAY_THRESHOLD': Uses date difference between the current date and event_date.
 * @param {number} [dayThreshold=3] - (Only for 'DAY_THRESHOLD') Number of days after which data is considered final.
 * @returns {string} SQL expression that evaluates to TRUE if the data is final, otherwise FALSE.
 *
 * @throws {Error} If an unsupported detectionMethod is provided.
 *
 * @example
 *   // Checks based on export type
 *   isFinalData('EXPORT_TYPE')
 *   // => "if(_table_suffix like 'intraday_%' or _table_suffix like 'fresh_%', false, true)"
 *
 *   // Checks using a custom day threshold
 *   isFinalData('DAY_THRESHOLD', 5)
 *   // => "if(date_diff(current_date(), cast(event_date as date format 'YYYYMMDD'), day) > 5, true, false)"
 */
const isFinalData = (detectionMethod, dayThreshold) => {
  if (detectionMethod !== 'EXPORT_TYPE' && detectionMethod !== 'DAY_THRESHOLD') {
    throw new Error(`isFinalData: Unsupported detectionMethod '${detectionMethod}'. Supported values are 'EXPORT_TYPE' and 'DAY_THRESHOLD'.`);
  }

  if (detectionMethod === 'DAY_THRESHOLD') {
    if (typeof dayThreshold === 'undefined') {
      throw new Error("isFinalData: 'dayThreshold' is required when using 'DAY_THRESHOLD' detectionMethod.");
    }
    if (!Number.isInteger(dayThreshold) || dayThreshold < 0) {
      throw new Error("isFinalData: 'dayThreshold' must be an integer greater than or equal to 0 when using 'DAY_THRESHOLD' detectionMethod.");
    }
  }

  if (detectionMethod === 'EXPORT_TYPE') {
    return 'if(_table_suffix like \'intraday_%\' or _table_suffix like \'fresh_%\', false, true)';
  }

  if (detectionMethod === 'DAY_THRESHOLD') {
    return `if(date_diff(current_date(), cast(event_date as date format 'YYYYMMDD'), day) > ${dayThreshold}, true, false)`;
  }
};

/**
 * Checks whether a given column name is part of the standard/expected GA4 BigQuery export columns.
 *
 * The list of recognized GA4 export columns is based on the official schema as of 2026-02-18.
 * This function can be used to filter or validate column names when processing GA4 data exports.
 *
 * @param {string} columnName - The name of the column to check.
 * @returns {boolean} True if the column name is a GA4 export column, otherwise false.
 */
const isGa4ExportColumn = (columnName) => {
  // list updated 2026-02-18
  const ga4ExportColumns = [
    "event_date",
    "event_timestamp",
    "event_name",
    "event_params",
    "event_previous_timestamp",
    "event_value_in_usd",
    "event_bundle_sequence_id",
    "event_server_timestamp_offset",
    "user_id",
    "user_pseudo_id",
    "privacy_info",
    "user_properties",
    "user_first_touch_timestamp",
    "user_ltv",
    "device",
    "geo",
    "app_info",
    "traffic_source",
    "stream_id",
    "platform",
    "event_dimensions",
    "ecommerce",
    "items",
    "collected_traffic_source",
    "is_active_user",
    "batch_event_index",
    "batch_page_id",
    "batch_ordering_id",
    "session_traffic_source_last_click",
    "publisher"
  ];
  return ga4ExportColumns.includes(columnName);
};

const getGa4ExportType = (tableSuffix) => {
  return `case
      when ${tableSuffix} like 'intraday_%' then 'intraday'
      when ${tableSuffix} like 'fresh_%' then 'fresh'
      when regexp_contains(${tableSuffix}, r'^\\d{8}$') then 'daily'
    end`;
};

module.exports = {
  sessionId,
  fixEcommerceStruct,
  isFinalData,
  isGa4ExportColumn,
  getGa4ExportType
};
