/*
Handling event and session parameters
*/

/**
 * Generates a SQL expression that filters the `event_params` array by the given parameter names.
 *
 * When filterType is 'include', only parameters whose key matches one of the given names are kept.
 * When filterType is 'exclude', parameters whose key matches are removed. If the params array is
 * empty with 'exclude', the original `event_params` column is returned unfiltered.
 *
 * @param {string[]} params - Array of event parameter names to include or exclude.
 * @param {'include'|'exclude'} filterType - Whether to include or exclude the listed parameters.
 * @returns {string} SQL expression that produces a filtered event_params array.
 * @throws {Error} If params is not an array of strings, or if filterType is not 'include' or 'exclude'.
 */
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
 * @param {'max'|'min'|'first'|'last'|'any'} aggregateType - Type of aggregation.
 * @param {string} [timestampColumn] - Column to use for ordering. Required when aggregateType is 'first' or 'last'.
 * @returns {string} A SQL expression for the requested aggregation.
 * @throws {Error} If required parameters are missing or an unsupported aggregateType is provided.
 *
 * @example
 *   aggregateValue('user_id', 'last', 'event_timestamp')
 *   // => SQL expression for the last user_id by event_timestamp.
 */
const aggregateValue = (column, aggregateType, timestampColumn) => {
  if (typeof column === 'undefined') {
    throw new Error("aggregateValue: 'column' is a required parameter and must be defined.");
  }
  if (typeof aggregateType === 'undefined') {
    throw new Error("aggregateValue: 'aggregateType' is a required parameter and must be defined.");
  }
  if ((aggregateType === 'first' || aggregateType === 'last') && typeof timestampColumn === 'undefined') {
    throw new Error(`aggregateValue: 'timestampColumn' is required when aggregateType is '${aggregateType}'.`);
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

/**
 * Generates SQL aggregation expressions for an array of column specifications.
 *
 * Each item in the array is passed to {@link aggregateValue} and optionally aliased.
 * The results are joined with commas for use in a SELECT clause.
 *
 * @param {Object[]} values - Array of aggregation specifications.
 * @param {string} values[].column - The column to aggregate.
 * @param {'max'|'min'|'first'|'last'|'any'} values[].aggregateType - Type of aggregation.
 * @param {string} [values[].timestampColumn] - Column for ordering. Required for 'first'/'last'.
 * @param {string} [values[].alias] - Optional output alias (appended as `AS alias`).
 * @returns {string} Comma-separated SQL aggregation expressions.
 * @throws {Error} If values is not an array.
 */
const aggregateValues = (values) => {
  if (Array.isArray(values)) {
    return values.map(value => {
      const sqlExpression = aggregateValue(value.column, value.aggregateType, value.timestampColumn)
      return `${sqlExpression}${value.alias ? ` as ${value.alias}` : ''}`;
    }).join(',\n ');
  }
  throw new Error("aggregateValues: 'values' must be an array of objects with 'column', 'aggregateType', and 'timestampColumn' properties.");
};

module.exports = {
  filterEventParams,
  aggregateSessionParams,
  excludeNullSessionParams,
  aggregateValue,
  aggregateValues
};
