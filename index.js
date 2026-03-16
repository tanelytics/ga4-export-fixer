const helpers = require('./helpers.js');
const ga4EventsEnhanced = require('./tables/ga4EventsEnhanced.js');
const preOperations = require('./preOperations.js');
const { validateConfig } = require('./inputValidation.js');
const { mergeSQLConfigurations } = require('./utils.js');

// export setPreOperations with default configuration for usage with downstream tables
const setPreOperations = (config) => {
  if (!config || !config.self) {
    throw new Error('setPreOperations: config.self is required. Pass the table\'s "self()" reference in the config object.');
  }
  if (config.incremental === undefined || config.incremental === null) {
    throw new Error('setPreOperations: config.incremental is required. Pass a boolean indicating whether the table uses incremental mode.');
  }

  /*
  Todo: consider improving the validateConfig function to cover this use case as well
  */

  const defaultConfig = {
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
      numberOfPreviousDaysToScan: 10,
    },
  };

  const mergedConfig = mergeSQLConfigurations(defaultConfig, config);

  return preOperations.setPreOperations(mergedConfig);
};

module.exports = {
  helpers,
  ga4EventsEnhanced,
  setPreOperations,
  validateConfig
};
