const helpers = require('./helpers.js');
const ga4EventsEnhanced = require('./tables/ga4EventsEnhanced.js');
const preOperations = require('./preOperations.js');

// export setPreOperations with default configuration for usage with downstream tables
const setPreOperations = (self, incremental, preOperationsConfig) => {
  const defaultPreOperationsConfig = {
    dateRangeStartFullRefresh: 'date(2000, 1, 1)',
    dateRangeEnd: 'current_date()',
    numberOfPreviousDaysToScan: 10,
  };
  
  const config = {
    self,
    incremental,
    preOperations: {...defaultPreOperationsConfig, ...preOperationsConfig}
  };

  return preOperations.setPreOperations(config);
};

module.exports = {
  helpers,
  ga4EventsEnhanced,
  setPreOperations
};
