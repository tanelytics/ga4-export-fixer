/*
Unnesting parameters
*/

/**
 * Generates a SQL subquery to extract a value from a parameter array by key.
 *
 * When a dataType is provided, the value is extracted from the corresponding typed column
 * (e.g., `value.string_value`, `value.int_value`). When omitted, a coalesce across all
 * value columns is returned, cast as a string.
 *
 * @param {string} keyName - The parameter key to look up in the array.
 * @param {string} paramsArray - The SQL expression for the parameter array to unnest (e.g., 'event_params').
 * @param {string} [dataType] - Optional data type: 'string', 'int', 'int64', 'double', 'float', or 'float64'.
 *                               If omitted, returns the value converted to a string.
 * @returns {string} SQL subquery expression that extracts the parameter value.
 * @throws {Error} If keyName or paramsArray is not a non-empty string, or if dataType is unsupported.
 */
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

/**
 * Extracts a value from the `event_params` array by key.
 *
 * Supported types: 'string', 'int', 'int64', 'double', 'float', 'float64'.
 * If omitted, returns the value converted to a string.
 *
 * @param {string} keyName - The event parameter key to look up.
 * @param {string} [dataType] - Optional data type for the extracted value.
 * @returns {string} SQL subquery expression that extracts the event parameter value.
 */
const unnestEventParam = (keyName, dataType) => {
  return unnestParam(keyName, 'event_params', dataType);
};

/**
 * Extracts a value from the `session_params` array by key.
 *
 * Supported types: 'string', 'int', 'int64', 'double', 'float', 'float64'.
 * If omitted, returns the value converted to a string.
 *
 * @param {string} keyName - The session parameter key to look up.
 * @param {string} [dataType] - Optional data type for the extracted value.
 * @returns {string} SQL subquery expression that extracts the session parameter value.
 */
const unnestSessionParam = (keyName, dataType) => {
  return unnestParam(keyName, 'session_params', dataType);
};

module.exports = {
  unnestEventParam,
  unnestSessionParam
};
