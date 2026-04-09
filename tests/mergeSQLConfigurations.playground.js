const { mergeSQLConfigurations } = require('../utils');
const { baseConfig } = require('../defaultConfig');
const { validateBaseConfig } = require('../inputValidation');
const { ga4EventsEnhancedConfig } = require('../tables/ga4EventsEnhanced/config');
const { validateEnhancedEventsConfig } = require('../tables/ga4EventsEnhanced/validation');

const log = (label, result, validationFunction) => {
  console.log(`\n--- ${label} ---`);
  console.log(JSON.stringify(result, null, 2));
  if (validationFunction) {
    try {
      validationFunction(result);
      console.log('Validation: PASSED');
    } catch (e) {
      console.log(`Validation: FAILED - ${e.message}`);
    }
  }
};

// 1. Empty input -- only defaults come through
log('Empty input', mergeSQLConfigurations(
  { timezone: 'Etc/UTC', bufferDays: 1 },
  {}
));

// 2. Scalar override
log('Scalar override (timezone)', mergeSQLConfigurations(
  { timezone: 'Etc/UTC', bufferDays: 1 },
  { timezone: 'Europe/Helsinki' }
));

// 3. Nested object partial override
log('Nested partial override (preOperations)', mergeSQLConfigurations(
  {
    preOperations: {
      dateRangeStartFullRefresh: 'date(2000, 1, 1)',
      dateRangeEnd: 'current_date()',
      numberOfPreviousDaysToScan: 10,
    },
  },
  {
    preOperations: { numberOfPreviousDaysToScan: 3 },
  }
));

// 4. Array with default counterpart
log('Array merge (excludedEvents + defaultExcludedEvents)', mergeSQLConfigurations(
  {
    defaultExcludedEvents: ['session_start', 'first_visit'],
    excludedEvents: [],
  },
  {
    excludedEvents: ['scroll', 'click'],
  }
));

// 5. Date string processing
log('Date string processing (YYYYMMDD)', mergeSQLConfigurations(
  {
    preOperations: { dateRangeStartFullRefresh: 'date(2000, 1, 1)' },
  },
  {
    preOperations: { dateRangeStartFullRefresh: '20260101' },
  }
));

// 6. Using setPreOperations in a downstream table
log('Using setPreOperations in a downstream table', mergeSQLConfigurations(
  {
    ...baseConfig,
  },
  {
    self: '`project.dataset.ga4_sessions`',
    incremental: true,
  },
), validateBaseConfig);

// 7. Default GA4 Events Enhanced configuration
log('Default GA4 Events Enhanced configuration', mergeSQLConfigurations(
  ga4EventsEnhancedConfig,
  {
    sourceTable: '`project.dataset.ga4_sessions`',
    schemaLock: '20260101',
    self: '`project.dataset.ga4_events_enhanced`',
    incremental: true,
  },
), validateEnhancedEventsConfig);