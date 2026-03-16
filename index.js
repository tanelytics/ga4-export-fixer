const helpers = require('./helpers.js');
const ga4EventsEnhanced = require('./tables/ga4EventsEnhanced.js');
const preOperations = require('./preOperations.js');
const { validateConfig } = require('./inputValidation.js');
const { mergeSQLConfigurations } = require('./utils.js');
const { baseConfig } = require('./defaultConfig.js');

// export setPreOperations with default configuration for usage with downstream tables
const setPreOperations = (config) => {
  if (!config || !config.self) {
    throw new Error('setPreOperations: config.self is required. Pass the table\'s "self()" reference in the config object.');
  }
  if (typeof config.incremental !== 'boolean') {
    throw new Error('setPreOperations: config.incremental is required. Pass a boolean indicating whether the table uses incremental mode.');
  }

  /*
  Todo:
  - validation for baseConfig -> include in this function
  */

  const mergedConfig = mergeSQLConfigurations(baseConfig, config);

  return preOperations.setPreOperations(mergedConfig);
};

module.exports = {
  helpers,
  ga4EventsEnhanced,
  setPreOperations,
  validateConfig
};
