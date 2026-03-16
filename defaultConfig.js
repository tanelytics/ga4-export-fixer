/*
These are the configuration defaults that can be extended.

For example, load the defaults in ga4EventsEnhanced.js and then extend them with whatever is psecific to the table.
After that, extend the configuration further with the user's configuration.
*/

/*
The base configuration. Input config validation should always check these fields.
*/
const baseConfig = {
    self: undefined,
    incremental: undefined,
    test: false,
    testConfig: {
        dateRangeStart: 'current_date()-1',
        dateRangeEnd: 'current_date()',
    },
    preOperations: {
        dateRangeStartFullRefresh: 'date(2000, 1, 1)',
        dateRangeEnd: 'current_date()',
        // incrementalStartOverride and incrementalEndOverride are used to override the date range start and end for incremental refresh
        // this is useful if you want to re-process only a specific date range
        incrementalStartOverride: undefined,
        incrementalEndOverride: undefined,
        numberOfPreviousDaysToScan: 10,
    },
};

/*
The default configuration for the GA4 Events Enhanced table.
*/
const ga4EventsEnhancedConfig = {
    ...baseConfig,
    sourceTable: undefined,
    sourceTableType: 'GA4_EXPORT', // used with pre operations to detect if ga4 export specific pre operations are needed
    // optional but recommended
    schemaLock: undefined,
    // only used with js tables
    // dataformTableConfig: {},
    // optional
    includedExportTypes: {
        daily: true,
        intraday: true,
        fresh: false,
    },
    timezone: 'Etc/UTC',
    customTimestampParam: undefined,
    dataIsFinal: {
        detectionMethod: 'EXPORT_TYPE', // or 'DAY_THRESHOLD'
        dayThreshold: 4 // only used if detectionMethod is 'DAY_THRESHOLD'
    },
    // number of additional days to take in for taking into account sessions that overlap days
    bufferDays: 1,
    // these parameters are excluded by default because they've been made available in other columns
    defaultExcludedEventParams: [
        'page_location',
        'ga_session_id',
        //'custom_event_timestamp', // removed if customTimestampParam is used
    ],
    excludedEventParams: [],
    eventParamsToColumns: [
        //{name: 'page_location', type: 'string', columnName: 'page_location2'},
    ],
    sessionParams: [],
    defaultExcludedEvents: [],
    // session_start and first_visit are excluded via the excludedEvents array
    // this allows the user to include them if needed
    excludedEvents: [
        'session_start',
        'first_visit'
    ],
    defaultExcludedColumns: [
        'event_dimensions', // legacy column, not needed
        'traffic_source', // renamed to user_traffic_source
        'session_id'
    ],
    // exclude these columns when extracting raw data from the export tables
    excludedColumns: [],
};

module.exports = {
    baseConfig,
    ga4EventsEnhancedConfig,
};