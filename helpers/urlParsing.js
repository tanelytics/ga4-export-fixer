const { unnestEventParam } = require('./params');

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

module.exports = {
  extractUrlHostname,
  extractUrlPath,
  extractUrlQuery,
  extractUrlQueryParams,
  extractPageDetails
};
