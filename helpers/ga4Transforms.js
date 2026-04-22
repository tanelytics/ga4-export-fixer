const { unnestEventParam } = require('./params');

/**
 * SQL expression that builds a session ID by concatenating `user_pseudo_id` with the `ga_session_id` event parameter.
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
 * @param {number} [dayThreshold] - (Only for 'DAY_THRESHOLD') Number of days after which data is considered final. Required when detectionMethod is 'DAY_THRESHOLD'.
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

/**
 * Generates a SQL CASE expression that determines the GA4 export type from a table suffix.
 *
 * Returns 'intraday' for suffixes like 'intraday_%', 'fresh' for 'fresh_%',
 * and 'daily' for 8-digit date suffixes (YYYYMMDD).
 *
 * @param {string} tableSuffix - SQL expression or column reference for the table suffix (e.g., '_table_suffix').
 * @returns {string} SQL CASE expression that evaluates to 'intraday', 'fresh', or 'daily'.
 */
const getGa4ExportType = (tableSuffix) => {
  return `case
      when ${tableSuffix} like 'intraday_%' then 'intraday'
      when ${tableSuffix} like 'fresh_%' then 'fresh'
      when regexp_contains(${tableSuffix}, r'^\\d{8}$') then 'daily'
    end`;
};

/**
 * Generates a SQL LAST_VALUE window function that attributes item list fields
 * (item_list_name, item_list_id, item_list_index) from select_item/select_promotion
 * events to downstream ecommerce events using a lookback window.
 *
 * Returns a struct containing all three attributed fields via a single window sort.
 *
 * @param {'SESSION'|'TIME'} lookbackType - Window scope: session-based or time-based
 * @param {string} timestampColumn - Column to order by ('event_timestamp' or 'event_custom_timestamp')
 * @param {number} [lookbackTimeMs] - Lookback window in milliseconds (required when lookbackType is 'TIME')
 * @returns {string} SQL expression that evaluates to a struct with item_list_name, item_list_id, item_list_index
 */
const itemListAttributionExpr = (lookbackType, timestampColumn, lookbackTimeMs) => {
  const selectEvents = `event_name in ('select_item', 'select_promotion')`;
  const structExpr = `struct(item.item_list_name, item.item_list_id, item.item_list_index)`;

  let partitionBy;
  let frameBounds;

  if (lookbackType === 'SESSION') {
    partitionBy = 'session_id, item.item_id';
    frameBounds = 'rows between unbounded preceding and current row';
  } else {
    // TIME-based: range window in microseconds
    const lookbackMicros = lookbackTimeMs * 1000;
    partitionBy = 'user_pseudo_id, item.item_id';
    frameBounds = `range between ${lookbackMicros} preceding and current row`;
  }

  return `last_value(
      if(${selectEvents}, ${structExpr}, null) ignore nulls
    ) over(
      partition by ${partitionBy}
      order by ${timestampColumn} asc
      ${frameBounds}
    )`;
};

/**
 * Generates a SQL expression for a deterministic hash-based row id used by the
 * item list attribution join. Only computed for events in `ecommerceEventsFilter`;
 * other events get NULL.
 *
 * The row_number() window keeps the id stable across CTE re-evaluations:
 * BigQuery may inline the CTE and re-run the window per reference, so without
 * a stable ordering the two sides of the downstream join could hash differently.
 * partition by event_name avoids a single-partition bottleneck.
 * Residual collisions (identical event_timestamp + identical items) are safe —
 * the rows are interchangeable, so arbitrary row number assignment between them
 * produces the same result.
 *
 * @param {string} ecommerceEventsFilter - Comma-separated, quoted list of event names
 *        (e.g., "'purchase', 'add_to_cart'").
 * @returns {string} SQL expression that evaluates to the row id or NULL.
 */
const itemListAttributionRowId = (ecommerceEventsFilter) => {
  return `if(
      event_name in (${ecommerceEventsFilter}),
      farm_fingerprint(concat(
        user_pseudo_id,
        cast(event_timestamp as string),
        event_name,
        to_json_string(items),
        cast(row_number() over(
          partition by event_name, user_pseudo_id
          order by event_timestamp, to_json_string(items)
        ) as string)
      )),
      null
    )`;
};

/**
 * Official GA4 ecommerce events that carry item data.
 * Based on: https://developers.google.com/analytics/devguides/collection/ga4/ecommerce
 */
const ga4EcommerceEvents = [
  'view_item_list',
  'select_item',
  'view_promotion',
  'select_promotion',
  'view_item',
  'add_to_wishlist',
  'add_to_cart',
  'remove_from_cart',
  'view_cart',
  'begin_checkout',
  'add_shipping_info',
  'add_payment_info',
  'purchase',
  'refund',
];

module.exports = {
  sessionId,
  fixEcommerceStruct,
  isFinalData,
  isGa4ExportColumn,
  getGa4ExportType,
  itemListAttributionExpr,
  itemListAttributionRowId,
  ga4EcommerceEvents
};
