/*
Date and time
*/

/**
 * SQL expression that casts the GA4 `event_date` string column to a DATE using YYYYMMDD format.
 */
const eventDate = `cast(event_date as date format 'YYYYMMDD')`;

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

/**
 * Returns a SQL expression representing the event's local datetime (in the specified time zone),
 * derived from the default event_timestamp field.
 *
 * - This function always uses the exported GA4 event_timestamp (in microseconds) for datetime calculation.
 * - No custom timestamp parameter from event_params is used; the extraction is strictly from event_timestamp.
 * - The returned expression converts event_timestamp to a TIMESTAMP, then extracts the DATETIME in the desired time zone.
 *
 * @param {Object} [config] - Optional configuration with a timezone property (defaults to 'Etc/UTC').
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

module.exports = {
  eventDate,
  getEventTimestampMicros,
  getEventDateTime
};
