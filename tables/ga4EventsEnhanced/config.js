const { baseConfig } = require('../../defaultConfig.js');

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
    dataformTableConfig: {
        type: 'incremental',
        bigquery: {
            partitionBy: 'event_date',
            clusterBy: ['event_name', 'session_id', 'page_location', 'data_is_final'],
            labels: {
                'ga4_export_fixer': 'true'
            }
        },
        onSchemaChange: 'EXTEND',
        tags: ['ga4_export_fixer'],
    },
    // optional
    includedExportTypes: {
        daily: true,
        fresh: false,
        intraday: true,
    },
    timezone: 'Etc/UTC',
    customTimestampParam: undefined,
    dataIsFinal: {
        detectionMethod: 'DAY_THRESHOLD', // 'EXPORT_TYPE' or 'DAY_THRESHOLD'
        dayThreshold: 3 // only used if detectionMethod is 'DAY_THRESHOLD'
        // according to GA4 documentation, the data up to 72 hours old is subject to possible changes
        // in reality, there have been cases where the data has changed even after 72 hours (4 day window would have covered these)
    },
    // optional item list attribution - disabled by default (compute-heavy, only useful for ecommerce sites)
    itemListAttribution: undefined,
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

module.exports = { ga4EventsEnhancedConfig };
