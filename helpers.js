const constants = require('./constants');

/*
Unnesting parameters
*/

// unnest any parameter from the selected params array
const unnestParam = (keyName, paramsArray, dataType) => {
  if (typeof keyName !== 'string' || keyName.trim() === '') {
    throw new Error("unnestParam: 'keyName' is required and must be a non-empty string.");
  }
  if (typeof paramsArray !== 'string' || paramsArray.trim() === '') {
    throw new Error("unnestParam: 'paramsArray' is required and must be a non-empty string.");
  }
  
  if (dataType) {
    // return the value from the selected column
    if (dataType === 'string') {
      return `(select value.string_value from unnest(${paramsArray}) where key = '${keyName}')`;
    } else if (dataType === 'int' || dataType === 'int64') {
      return `(select value.int_value from unnest(${paramsArray}) where key = '${keyName}')`;
    } else if (dataType === 'double') {
      return `(select value.double_value from unnest(${paramsArray}) where key = '${keyName}')`;
    } else if (dataType === 'float' || dataType === 'float64') {
      return `(select value.float_value from unnest(${paramsArray}) where key = '${keyName}')`;
    }

    throw new Error(`unnestParam: Unsupported dataType '${dataType}'. Supported values are 'string', 'int', 'int64', 'double', 'float', and 'float64'.`);
  } else {
    // return the value from the column that has data, cast as string
    return `(select coalesce(value.string_value, cast(value.int_value as string), cast(value.double_value as string), cast(value.float_value as string)) from unnest(${paramsArray}) where key = '${keyName}')`;
  }
};

// event_params

// unnest a param from the event_params array
const unnestEventParam = (keyName, dataType) => {
  return unnestParam(keyName, 'event_params', dataType);
};

/*
Common identifiers
*/

const sessionId = `concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id'))`;

/*
Date and time
*/

const eventDate = `cast(event_date as date format 'YYYYMMDD')`;

// get the most accurate event timestamp
/**
 * Returns a SQL expression for the event timestamp in microseconds.
 *
 * If a custom event parameter is provided (e.g., a parameter collected as a JavaScript timestamp in milliseconds using Date.now()),
 * this function will attempt to extract its value (via event_params) and convert it to microseconds by multiplying by 1000.
 * If the custom parameter is not present or null, the function falls back to the default 'event_timestamp' field.
 *
 * Usage of customTimestampParameter is intended for event parameters that carry a JS timestamp in milliseconds (for example, set using Date.now()).
 * 
 * @param {string} [customTimestampParameter] - Name of an event parameter containing a JS timestamp in milliseconds (e.g., collected via Date.now()).
 * @returns {string} SQL expression for the event timestamp in microseconds.
 */
const getEventTimestampMicros = (customTimestampParameter) => {
  if (typeof customTimestampParameter !== 'undefined' && (typeof customTimestampParameter !== 'string' || customTimestampParameter.trim() === '')) {
    throw new Error("getEventTimestampMicros: customTimestampParameter must be undefined or a non-empty string.");
  }

  if (customTimestampParameter) {
    return `coalesce((select value.int_value from unnest(event_params) where key = '${customTimestampParameter}')*1000, event_timestamp)`;
  }
  return 'event_timestamp';
};

// datetime in the local time zone
/**
 * Returns a SQL expression representing the event's local datetime (in the specified time zone), 
 * derived from the default event_timestamp field.
 * 
 * - This function always uses the exported GA4 event_timestamp (in microseconds) for datetime calculation.
 * - No custom timestamp parameter from event_params is used; the extraction is strictly from event_timestamp.
 * - The returned expression converts event_timestamp to a TIMESTAMP, then extracts the DATETIME in the desired time zone.
 *
 * @param {Object} config - Optional configuration with a timezone property (defaults to 'Etc/UTC').
 * @param {string} [config.timezone] - IANA time zone string (e.g., 'Europe/Helsinki'). Defaults to 'Etc/UTC'.
 * @returns {string} SQL expression for the local datetime of the event.
 *
 * @example
 *   getEventDateTime({ timezone: 'Europe/Helsinki' })
 *   // => "extract(datetime from timestamp_micros(event_timestamp) at time zone 'Europe/Helsinki')"
 */
const getEventDateTime = (config) => {
  const timezone = config?.timezone || 'Etc/UTC';
  return `extract(datetime from timestamp_micros(${getEventTimestampMicros()}) at time zone '${timezone}')`;
};

// Filter the export tables by date range
/**
 * Generates a SQL filter condition for selecting GA4 export tables based on the export type ('intraday' or 'daily') and a date range.
 *
 * This helper produces SQL snippets to be used in WHERE clauses, ensuring only tables within the provided date range and export type are included.
 * 
 * - For 'daily' exports: Matches table suffixes formatted as YYYYMMDD (e.g., 20240101).
 * - For 'intraday' exports: Matches table suffixes prefixed with 'intraday_' followed by the date (e.g., intraday_20240101).
 * - Throws an error for unsupported export types or if start/end dates are undefined.
 *
 * @param {'intraday'|'daily'} exportType - The type of export table; either 'intraday' or 'daily'.
 * @param {string} start - The start date value as a SQL date expression (e.g. 'current_date()-1').
 * @param {string} end - The end date value as a SQL date expression (e.g. 'current_date()').
 * @returns {string} SQL condition to restrict tables by _table_suffix to the appropriate date range and export type.
 *
 * @throws {Error} If exportType is not 'intraday' or 'daily', or if start/end are not defined.
 *
 * @example
 *   ga4ExportDateFilter('daily', 'current_date()-1', 'current_date()')
 *   // => "(_table_suffix >= cast(current_date()-1 as string format \"YYYYMMDD\") and _table_suffix <= cast(current_date() as string format \"YYYYMMDD\"))"
 *
 *   ga4ExportDateFilter('intraday', 'current_date()-1', 'current_date()')
 *   // => "(_table_suffix >= 'intraday_' || cast(current_date()-1 as string format \"YYYYMMDD\") and _table_suffix <= 'intraday_' || cast(current_date() as string format \"YYYYMMDD\"))"
 */
const ga4ExportDateFilter = (exportType, start, end) => {
  if (exportType !== 'intraday' && exportType !== 'daily') {
    throw new Error(
      `ga4ExportDateFilter: Unsupported exportType '${exportType}'. Supported values are 'intraday' and 'daily'.`
    );
  }
  if (typeof start === 'undefined' || typeof end === 'undefined') {
    throw new Error("ga4ExportDateFilter: 'start' and 'end' parameters must be defined.");
  }
  
  if (exportType === 'intraday') {
    return `(_table_suffix >= 'intraday_' || cast(${start} as string format "YYYYMMDD") and _table_suffix <= 'intraday_' || cast(${end} as string format "YYYYMMDD"))`;
  }
  if (exportType === 'daily') {
    return `(_table_suffix >= cast(${start} as string format "YYYYMMDD") and _table_suffix <= cast(${end} as string format "YYYYMMDD"))`;
  }
};

/**
 * Builds a `_table_suffix` WHERE clause for GA4 BigQuery export tables (daily and/or intraday).
 *
 * Date boundaries are resolved differently depending on the mode:
 *   - **test** -- literal dates from `config.testConfig`
 *   - **incremental** -- BigQuery variable placeholders set by pre-operations
 *   - **full refresh** -- static dates from `config.preOperations`
 *
 * `bufferDays` is subtracted from the daily start date so sessions that span
 * midnight are not partially excluded.
 *
 * When both daily and intraday exports are enabled, the intraday start date
 * comes from a dedicated variable (`INTRADAY_DATE_RANGE_START_VARIABLE`) so
 * intraday tables that already have a corresponding daily table are excluded.
 * When only intraday is enabled, the daily start-date logic (including buffer
 * days) is reused instead.
 *
 * @param {Object} config
 * @param {boolean}  config.test                      - Use literal test dates.
 * @param {Object}   config.testConfig                - `{ dateRangeStart, dateRangeEnd }`.
 * @param {boolean}  config.incremental               - Use BigQuery variable placeholders.
 * @param {Object}   config.preOperations             - `{ dateRangeStartFullRefresh, dateRangeEnd }`.
 * @param {Object}   config.includedExportTypes       - `{ daily: boolean, intraday: boolean }`.
 * @param {number}  [config.bufferDays=0]             - Extra days subtracted from the start date.
 * @returns {string} SQL fragment for a WHERE clause.
 */
const ga4ExportDateFilters = (config) => {
  const bufferDays = config.bufferDays || 0;

  const getStartDate = () => {
    //test mode
    if (config.test) {
      return config.testConfig.dateRangeStart;
    }
    if (config.incremental) {
      return constants.DATE_RANGE_START_VARIABLE;
    }
    // full refresh
    return config.preOperations.dateRangeStartFullRefresh;
  };

  const getEndDate = () => {
    // test mode, avoid using a BigQuery variable
    if (config.test) {
      return config.testConfig.dateRangeEnd;
    }
    // use checkpoint variable with incremental refresh -> allows pre processing any part of the table without having to do a full refresh
    if (config.incremental) {
      return constants.DATE_RANGE_END_VARIABLE;
    }
    // full refresh
    return config.preOperations.dateRangeEnd;
  };

  const getIntradayStartDate = () => {
    // In test mode, skip pre-operations even though intraday and daily tables may temporarily overlap.
    if (config.test) {
      return config.testConfig.dateRangeStart;
    }
    // Dedicated variable excludes intraday tables that overlap with already-processed daily tables.
    if (config.includedExportTypes.intraday && config.includedExportTypes.daily) {
      return constants.INTRADAY_DATE_RANGE_START_VARIABLE;
    }
    // Without daily export, reuse the daily start-date logic and apply bufferDays
    // (buffer is normally only applied to the daily start date).
    if (config.includedExportTypes.intraday && !config.includedExportTypes.daily) {
      // use the same start date as if daily export was in use
      // include the buffer days as well (not included otherwise for intraday data)
      return `${getStartDate()}-${bufferDays}`;
    }
  };

  const dailyStart = `${getStartDate()}-${bufferDays}`;
  const intradayStart = getIntradayStartDate();
  const end = getEndDate();
  

  const dateFilters = [
    config.includedExportTypes.daily ? ga4ExportDateFilter('daily', dailyStart, end) : null,
    config.includedExportTypes.intraday ? ga4ExportDateFilter('intraday', intradayStart, end) : null,
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
const finalDataFilter = (config) => {
  const setDateRange = (start, end) => {
    return `(event_date >= ${start} and event_date <= ${end})`;
  };

  if (config.test) {
    return setDateRange(config.testConfig.dateRangeStart, config.testConfig.dateRangeEnd);
  }
  if (config.incremental) {
    return setDateRange(constants.DATE_RANGE_START_VARIABLE, constants.DATE_RANGE_END_VARIABLE);
  }
    
  return setDateRange(config.preOperations.dateRangeStartFullRefresh, config.preOperations.dateRangeEnd);
};

/*
Page details
*/

/**
 * Generates a SQL expression to extract the hostname from a URL.
 *
 * This function returns a BigQuery SQL string that:
 *   1. Removes the HTTP or HTTPS scheme from the start of the URL using regexp_replace.
 *   2. Extracts the hostname (the first part before the next '/') using regexp_extract.
 *
 * Example usage (in SQL context):
 *   SELECT ${extractUrlHostname('my_url_column')} AS hostname
 *
 * @param {string} url - The SQL expression or column reference containing the URL.
 * @returns {string} - BigQuery SQL expression for extracting the hostname from the input URL.
 */
const extractUrlHostname = (url) => {
  return `regexp_extract(
    regexp_replace(
      ${url},
      r'^https?://',
      ''
    ),
    r'^[^/]+'
  )`;
};

/**
 * Generates a SQL expression to extract the path component from a URL.
 *
 * This function returns a BigQuery SQL string that:
 *   1. Removes the scheme and hostname (e.g., http(s)://domain) from the URL using regexp_replace.
 *   2. Removes any query ('?') or fragment ('#') from the resulting string.
 *   3. Trims whitespace from the result.
 *
 * Example usage (in SQL context):
 *   SELECT ${extractUrlPath('my_url_column')} AS path
 *
 * @param {string} url - The SQL expression or column reference containing the URL.
 * @returns {string} - BigQuery SQL expression for extracting the path component from the input URL.
 */
const extractUrlPath = (url) => {
  return `trim(
    regexp_replace(
      regexp_replace(
        ${url},
        r'^https?://[^/]+',
        ''
      ),
      r'[\\?#].*',
      ''
    )
  )`;
};

/**
 * Generates a SQL expression to extract the query component from a URL.
 *
 * This function returns a BigQuery SQL string that:
 *   1. Uses regexp_extract to retrieve the query string (the part starting with '?', up to but not including a fragment '#', if present) from the input URL.
 *   2. Trims leading/trailing whitespace from the extracted query string.
 *
 * Example usage (in SQL context):
 *   SELECT ${extractUrlQuery('my_url_column')} AS url_query
 *
 * @param {string} url - The SQL expression or column reference containing the URL.
 * @returns {string} - BigQuery SQL expression for extracting the query string from the input URL, including the leading '?' if present.
 */
const extractUrlQuery = (url) => {
  return `trim(regexp_extract(${url}, r'\\?[^#]+'))`;
};

/**
 * Generates a SQL expression to parse the query parameters of a URL into an array of structs (key-value pairs).
 *
 * This function:
 *   1. Extracts the query string from the given URL using {@link extractUrlQuery}.
 *   2. Splits the query string on '&' to separate individual key-value pairs.
 *   3. Splits each pair on '=' to extract the parameter key and value.
 *   4. Returns an array of STRUCTs with fields "key" and "value".
 *
 * Example usage (in SQL context):
 *   SELECT ${extractUrlQueryParams('my_url_column')} AS query_params
 *
 * Output schema:
 *   ARRAY<STRUCT<key STRING, value STRING>>
 *
 * @param {string} url - The SQL expression or column reference containing the URL.
 * @returns {string} - BigQuery SQL expression producing an array of key/value structs for the query parameters.
 */
const extractUrlQueryParams = (url) => {
  return `array(
        (
          select
            as struct split(keyval, '=') [safe_offset(0)] as key,
            split(keyval, '=') [safe_offset(1)] as value
          from
            unnest(
              split(
                ${extractUrlQuery(url)},
                '&'
              )
            ) as keyval
        )
      )`;
};

/**
 * Generates a SQL expression that extracts detailed page information from a given URL.
 *
 * This function produces a BigQuery SQL struct containing the following fields:
 *   - hostname: The hostname part of the URL (e.g., 'www.example.com')
 *   - path: The path portion of the URL (e.g., '/about/team')
 *   - query: The raw query string from the URL, including the leading '?', if present (e.g., '?id=123')
 *   - query_params: An array of STRUCT<key STRING, value STRING> representing parsed key/value pairs from the query string
 *
 * If no URL is provided, the function defaults to extracting the URL from the `page_location` event parameter.
 * All fields are derived via helper functions that generate appropriate BigQuery SQL expressions.
 *
 * Example usage (in SQL context):
 *   SELECT ${extractPageDetails('my_url_column')} AS page_details
 *
 * Output schema (STRUCT):
 *   {
 *     hostname: STRING,
 *     path: STRING,
 *     query: STRING,
 *     query_params: ARRAY<STRUCT<key STRING, value STRING>>
 *   }
 *
 * @param {string} [url] - (Optional) SQL expression or column reference for the URL to extract details from.
 *                         If not provided, defaults to unnesting the 'page_location' event parameter as a string.
 * @returns {string} BigQuery SQL expression yielding a STRUCT of hostname, path, query, and query_params from the URL.
 */
const extractPageDetails = (url) => {
  url = url || `${unnestEventParam('page_location', 'string')}`;

  return `(select as struct
    ${extractUrlHostname(url)} as hostname,
    ${extractUrlPath(url)} as path,
    ${extractUrlQuery(url)} as query,
    ${extractUrlQueryParams(url)} as query_params
  )`;
};

/*
Handling event and session parameters
*/

// filter the event_params array by the selected parameters
const filterEventParams = (params, filterType) => {
  if (!Array.isArray(params) || !params.every(p => typeof p === 'string')) {
    throw new Error("filterEventParams: 'params' must be an array of strings (empty array allowed).");
  }

  if (filterType !== 'include' && filterType !== 'exclude') {
    throw new Error("filterEventParams: 'filterType' must be 'include' or 'exclude'.");
  }

  const filterParams = params.map(p => `'${p}'`).join(', ');

  if (filterType === 'include') {
    return `array(select as struct * from unnest(event_params) where key in (${filterParams}))`;
  }

  if (filterType === 'exclude') {
    if (!params || params.length === 0) {
      return 'event_params';
    }

    return `array(select as struct * from unnest(event_params) where key not in (${filterParams}))`;
  }
};

/**
 * Generates a BigQuery SQL expression that aggregates specified session parameters across events,
 * returning, for each parameter, the most recent (last non-null) value by timestamp. If a parameter
 * does not appear, a dummy struct with null values for all types is returned for that key.
 *
 * This is useful for building an array of session parameter structs for analytic purposes,
 * ensuring proper presence of all expected keys and null placeholders where values are missing.
 *
 * The resulting SQL expression yields an ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>>>.
 *
 * @param {string[]} paramNames - Array of parameter names (keys) to aggregate.
 * @param {string} paramsArray - SQL expression or column reference representing the array of session parameters to aggregate.
 * @param {string} timestampColumn - SQL expression or column indicating the timestamp associated with each parameter, used for ordering.
 * @returns {string} SQL expression that produces an array of parameter structs with their last values or null if not present.
 */
const aggregateSessionParams = (paramNames, paramsArray, timestampColumn) => {
  // Validate paramNames
  if (!Array.isArray(paramNames) || !paramNames.every(p => typeof p === 'string')) {
    throw new Error("aggregateSessionParams: 'paramNames' must be an array of strings (empty array allowed).");
  }
  // Validate paramsArray
  if (typeof paramsArray !== 'string' || paramsArray.trim() === '') {
    throw new Error("aggregateSessionParams: 'paramsArray' must be a non-empty string reference to a SQL field or expression.");
  }
  // Validate timestampColumn
  if (typeof timestampColumn !== 'string' || timestampColumn.trim() === '') {
    throw new Error("aggregateSessionParams: 'timestampColumn' must be a non-empty string reference to a SQL field or expression.");
  }

  if (paramNames.length > 0) {
    const sessionParamStructs = paramNames.map(p => {
      return `ifnull(
      -- get the last non-null value for the parameter
      array_agg(
        (select as struct * from unnest(${paramsArray}) where key = '${p}') ignore nulls
        order by ${timestampColumn} desc
        limit 1
      )[safe_offset(0)],
      -- if no value is found, return a dummy value
      (
        select as struct 
          '${p}' as key, 
          (
            select as struct
              cast(null as string) as string_value,
              cast(null as int64) as int_value,
              cast(null as float64) as float_value,
              cast(null as float64) as double_value
          ) as value
      )
    )`;
    });

    return `[
      ${sessionParamStructs.join(',\n    ')}
    ]`;
  } else {
    // declare the session_params in the schema even if no session params are specified
    return `cast([] as array<struct<key string, value struct<string_value string, int_value int64, float_value float64, double_value float64>>>)`;
  }
};

/**
 * Produces a SQL expression that returns an array of session parameter structs
 * from the given paramsArray, excluding any where all value fields are null.
 *
 * This helper is useful for cleaning up session_params or event_params arrays
 * by removing elements whose value is entirely null (i.e., string_value, int_value,
 * float_value, and double_value are all null). The resulting array contains
 * only parameter entries with at least one non-null value.
 *
 * @param {string} paramsArray - The name of the array field or SQL expression to unnest (e.g. 'session_params' or 'event_params').
 * @returns {string} SQL expression that yields an array of non-null parameter structs.
 *
 * @example
 *   excludeNullSessionParams('session_params')
 *   // => "array(select as struct * from unnest(session_params) where value.string_value is not null or value.int_value is not null or value.float_value is not null or value.double_value is not null)"
 */
const excludeNullSessionParams = (paramsArray) => {
  if (typeof paramsArray !== 'string' || paramsArray.trim() === '') {
    throw new Error("excludeNullSessionParams: 'paramsArray' is required and must be a non-empty string.");
  }

  return `array(select as struct * from unnest(${paramsArray}) where value.string_value is not null or value.int_value is not null or value.float_value is not null or value.double_value is not null)`;
};

/*
Aggregation
*/

/**
 * Generates a SQL aggregation expression for a specified column and aggregation type, 
 * optionally using a timestamp column for ordering 'first' or 'last' values.
 *
 * Supported aggregation types:
 * - 'max': Returns the maximum value of the column.
 * - 'min': Returns the minimum value of the column.
 * - 'first': Returns the first non-null value of the column, ordered by the timestampColumn ascending.
 * - 'last': Returns the last non-null value of the column, ordered by the timestampColumn descending.
 * - 'any': Returns any (typically arbitrary) value of the column (uses BigQuery's any_value).
 *
 * Throws an error if required parameters are missing or an unsupported aggregation type is requested.
 *
 * @param {string} column - The name of the column to aggregate.
 * @param {string} aggregateType - Type of aggregation ('max', 'min', 'first', 'last', or 'any').
 * @param {string} timestampColumn - Column to use for ordering when aggregateType is 'first' or 'last'.
 * @returns {string} A SQL expression for the requested aggregation.
 * @throws {Error} If required parameters are missing or an unsupported aggregateType is provided.
 *
 * @example
 *   aggregateValue('user_id', 'last', 'event_timestamp')
 *   // => SQL expression for the last user_id by event_timestamp.
 */
const aggregateValue = (column, aggregateType, timestampColumn) => {
  if (typeof column === 'undefined' || typeof timestampColumn === 'undefined') {
    throw new Error("aggregateValue: 'column' and 'timestampColumn' are required parameters and must be defined.");
  }
  
  if (aggregateType === 'max') {
    return `max(${column})`;
  }

  if (aggregateType === 'min') {
    return `min(${column})`;
  }

  if (aggregateType === 'first') {
    return `array_agg(
    ${column} ignore nulls 
    order by ${timestampColumn} 
    limit 1
  )[safe_offset(0)]`;
  }

  if (aggregateType === 'last') {
    return `array_agg(
    ${column} ignore nulls 
    order by ${timestampColumn} desc 
    limit 1
  )[safe_offset(0)]`;
  }

  if (aggregateType === 'any') {
    return `any_value(${column})`;
  }

  throw new Error(`aggregateValue: Unsupported aggregateType '${aggregateType}'. Supported values are 'max', 'min', 'first', 'last', and 'any'.`);
};

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

module.exports = {
  eventDate,
  getEventDateTime,
  getEventTimestampMicros,
  unnestEventParam,
  sessionId,
  aggregateValue,
  fixEcommerceStruct,
  isFinalData,
  ga4ExportDateFilter,
  ga4ExportDateFilters,
  filterEventParams,
  aggregateSessionParams,
  excludeNullSessionParams,
  finalDataFilter,
  extractPageDetails,
  extractUrlHostname,
  extractUrlPath,
  extractUrlQuery,
  extractUrlQueryParams,
  isGa4ExportColumn
}