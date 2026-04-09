const helpers = require('./helpers/index.js');
const ga4EventsEnhanced = require('./tables/ga4EventsEnhanced.js');
const { setPreOperations: setPreOperationsRaw } = require('./preOperations.js');
const { validateBaseConfig } = require('./inputValidation.js');
const { mergeSQLConfigurations } = require('./utils.js');
const { baseConfig } = require('./defaultConfig.js');

// export setPreOperations with default configuration for usage with downstream tables
const setPreOperations = (config) => {
  // merge the input config with the defaults
  const mergedConfig = mergeSQLConfigurations(baseConfig, config);

  // do input validation on the merged config
  validateBaseConfig(mergedConfig);

  return setPreOperationsRaw(mergedConfig);
};

module.exports = {
  helpers,
  ga4EventsEnhanced,
  setPreOperations,
};
