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
        numberOfDaysToProcess: undefined,
    },
};

module.exports = {
    baseConfig,
};