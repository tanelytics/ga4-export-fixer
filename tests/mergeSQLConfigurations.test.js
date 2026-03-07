const assert = require('assert');
const { mergeSQLConfigurations } = require('../utils');

let passed = 0;
let failed = 0;

const test = (name, fn) => {
  try {
    fn();
    passed++;
    console.log(`  PASS: ${name}`);
  } catch (err) {
    failed++;
    console.error(`  FAIL: ${name}`);
    console.error(`        ${err.message}\n`);
  }
};

// ---------------------------------------------------------------------------
// 1. Edge cases for inputConfig
// ---------------------------------------------------------------------------
console.log('\n1. Edge cases for inputConfig\n');

test('returns defaultConfig when inputConfig is undefined', () => {
  const defaults = { a: 1, b: 2 };
  const result = mergeSQLConfigurations(defaults, undefined);
  assert.deepStrictEqual(result, defaults);
});

test('returns defaultConfig when inputConfig is null', () => {
  const defaults = { a: 1 };
  assert.deepStrictEqual(mergeSQLConfigurations(defaults, null), defaults);
});

test('returns defaultConfig when inputConfig is a string', () => {
  const defaults = { a: 1 };
  assert.deepStrictEqual(mergeSQLConfigurations(defaults, 'hello'), defaults);
});

test('returns defaultConfig when inputConfig is an array', () => {
  const defaults = { a: 1 };
  assert.deepStrictEqual(mergeSQLConfigurations(defaults, [1, 2]), defaults);
});

test('returns defaultConfig unchanged when inputConfig is empty object', () => {
  const defaults = { timezone: 'Etc/UTC', bufferDays: 1 };
  const result = mergeSQLConfigurations(defaults, {});
  assert.deepStrictEqual(result, defaults);
});

// ---------------------------------------------------------------------------
// 2. Primitive overrides
// ---------------------------------------------------------------------------
console.log('\n2. Primitive overrides\n');

test('scalar value in inputConfig overrides default', () => {
  const defaults = { timezone: 'Etc/UTC', bufferDays: 1 };
  const result = mergeSQLConfigurations(defaults, { timezone: 'Europe/Helsinki' });
  assert.strictEqual(result.timezone, 'Europe/Helsinki');
  assert.strictEqual(result.bufferDays, 1);
});

test('explicitly passing undefined overrides default value', () => {
  const defaults = { timezone: 'Etc/UTC' };
  const result = mergeSQLConfigurations(defaults, { timezone: undefined });
  assert.strictEqual(result.timezone, undefined);
});

test('keys not in defaultConfig are added to result', () => {
  const defaults = { a: 1 };
  const result = mergeSQLConfigurations(defaults, { newKey: 'new' });
  assert.strictEqual(result.newKey, 'new');
  assert.strictEqual(result.a, 1);
});

// ---------------------------------------------------------------------------
// 3. Nested object merging
// ---------------------------------------------------------------------------
console.log('\n3. Nested object merging\n');

test('nested objects are merged recursively, not replaced', () => {
  const defaults = {
    preOperations: {
      dateRangeEnd: 'current_date()',
      numberOfPreviousDaysToScan: 10,
    },
  };
  const result = mergeSQLConfigurations(defaults, {
    preOperations: { numberOfPreviousDaysToScan: 5 },
  });
  assert.strictEqual(result.preOperations.numberOfPreviousDaysToScan, 5);
  assert.strictEqual(result.preOperations.dateRangeEnd, 'current_date()');
});

test('deeply nested keys can be overridden individually', () => {
  const defaults = {
    level1: { level2: { a: 1, b: 2 } },
  };
  const result = mergeSQLConfigurations(defaults, {
    level1: { level2: { b: 99 } },
  });
  assert.strictEqual(result.level1.level2.a, 1);
  assert.strictEqual(result.level1.level2.b, 99);
});

// ---------------------------------------------------------------------------
// 4. Array handling -- default counterpart exists
// ---------------------------------------------------------------------------
console.log('\n4. Array handling -- default counterpart exists\n');

test('excludedEvents is merged with defaultExcludedEvents', () => {
  const defaults = {
    defaultExcludedEvents: ['session_start', 'first_visit'],
    excludedEvents: [],
  };
  const result = mergeSQLConfigurations(defaults, {
    excludedEvents: ['scroll'],
  });
  assert.deepStrictEqual(result.excludedEvents, ['scroll', 'session_start', 'first_visit']);
});

test('user values appear before default values in merged array', () => {
  const defaults = {
    defaultExcludedEvents: ['a', 'b'],
    excludedEvents: [],
  };
  const result = mergeSQLConfigurations(defaults, {
    excludedEvents: ['c', 'd'],
  });
  assert.deepStrictEqual(result.excludedEvents, ['c', 'd', 'a', 'b']);
});

test('duplicates between user and default arrays are removed', () => {
  const defaults = {
    defaultExcludedEvents: ['session_start', 'first_visit'],
    excludedEvents: [],
  };
  const result = mergeSQLConfigurations(defaults, {
    excludedEvents: ['first_visit', 'scroll'],
  });
  assert.deepStrictEqual(result.excludedEvents, ['first_visit', 'scroll', 'session_start']);
});

test('excludedEventParams is merged with defaultExcludedEventParams', () => {
  const defaults = {
    defaultExcludedEventParams: ['page_location', 'ga_session_id'],
    excludedEventParams: [],
  };
  const result = mergeSQLConfigurations(defaults, {
    excludedEventParams: ['custom_param'],
  });
  assert.deepStrictEqual(result.excludedEventParams, ['custom_param', 'page_location', 'ga_session_id']);
});

test('excludedColumns is merged with defaultExcludedColumns', () => {
  const defaults = {
    defaultExcludedColumns: ['event_dimensions', 'traffic_source'],
    excludedColumns: [],
  };
  const result = mergeSQLConfigurations(defaults, {
    excludedColumns: ['items'],
  });
  assert.deepStrictEqual(result.excludedColumns, ['items', 'event_dimensions', 'traffic_source']);
});

test('when user does not provide the array, default empty array is preserved', () => {
  const defaults = {
    defaultExcludedEvents: ['session_start'],
    excludedEvents: [],
  };
  const result = mergeSQLConfigurations(defaults, {});
  assert.deepStrictEqual(result.excludedEvents, []);
});

// ---------------------------------------------------------------------------
// 5. Array handling -- no default counterpart
// ---------------------------------------------------------------------------
console.log('\n5. Array handling -- no default counterpart\n');

test('sessionParams is overwritten entirely by user input', () => {
  const defaults = { sessionParams: ['old'] };
  const result = mergeSQLConfigurations(defaults, { sessionParams: ['new1', 'new2'] });
  assert.deepStrictEqual(result.sessionParams, ['new1', 'new2']);
});

test('eventParamsToColumns is overwritten entirely by user input', () => {
  const defaults = { eventParamsToColumns: [{ name: 'old', type: 'string' }] };
  const input = [{ name: 'new', type: 'int' }];
  const result = mergeSQLConfigurations(defaults, { eventParamsToColumns: input });
  assert.deepStrictEqual(result.eventParamsToColumns, input);
});

// ---------------------------------------------------------------------------
// 6. Array handling -- default version itself is overridden
// ---------------------------------------------------------------------------
console.log('\n6. Array handling -- default version itself is overridden\n');

test('passing defaultExcludedEvents directly overwrites the default array', () => {
  const defaults = {
    defaultExcludedEvents: ['session_start', 'first_visit'],
    excludedEvents: [],
  };
  const result = mergeSQLConfigurations(defaults, {
    defaultExcludedEvents: ['custom_only'],
  });
  assert.deepStrictEqual(result.defaultExcludedEvents, ['custom_only']);
});

test('overriding default array also affects the merged counterpart', () => {
  const defaults = {
    defaultExcludedEvents: ['session_start', 'first_visit'],
    excludedEvents: [],
  };
  const result = mergeSQLConfigurations(defaults, {
    defaultExcludedEvents: ['custom_only'],
    excludedEvents: ['scroll'],
  });
  assert.deepStrictEqual(result.defaultExcludedEvents, ['custom_only']);
  // excludedEvents merges with the (now-overridden) defaultExcludedEvents
  assert.deepStrictEqual(result.excludedEvents, ['scroll', 'custom_only']);
});

// ---------------------------------------------------------------------------
// 7. Date field processing
// ---------------------------------------------------------------------------
console.log('\n7. Date field processing\n');

test('YYYYMMDD string date is converted to SQL CAST expression', () => {
  const defaults = {
    preOperations: { dateRangeStartFullRefresh: 'date(2000, 1, 1)' },
  };
  const result = mergeSQLConfigurations(defaults, {
    preOperations: { dateRangeStartFullRefresh: '20260101' },
  });
  assert.strictEqual(result.preOperations.dateRangeStartFullRefresh, "cast('20260101' as date format 'YYYYMMDD')");
});

test('YYYY-MM-DD string date is converted to SQL CAST expression', () => {
  const defaults = {
    testConfig: { dateRangeStart: 'current_date()-1' },
  };
  const result = mergeSQLConfigurations(defaults, {
    testConfig: { dateRangeStart: '2026-03-07' },
  });
  assert.strictEqual(result.testConfig.dateRangeStart, "cast('2026-03-07' as date format 'YYYY-MM-DD')");
});

test('SQL expressions in date fields are passed through unchanged', () => {
  const defaults = {
    preOperations: { dateRangeEnd: 'current_date()' },
  };
  const result = mergeSQLConfigurations(defaults, {
    preOperations: { dateRangeEnd: 'date_sub(current_date(), interval 1 day)' },
  });
  assert.strictEqual(result.preOperations.dateRangeEnd, 'date_sub(current_date(), interval 1 day)');
});

test('undefined date fields are not processed and cause no error', () => {
  const defaults = {
    preOperations: {
      dateRangeStartFullRefresh: 'date(2000, 1, 1)',
      incrementalStartOverride: undefined,
    },
  };
  const result = mergeSQLConfigurations(defaults, {});
  assert.strictEqual(result.preOperations.incrementalStartOverride, undefined);
});

// ---------------------------------------------------------------------------
// 8. sourceTable normalization
// ---------------------------------------------------------------------------
console.log('\n8. sourceTable normalization\n');

test('string sourceTable "project.dataset" is normalized to backtick format', () => {
  const defaults = { sourceTable: undefined };
  const result = mergeSQLConfigurations(defaults, { sourceTable: 'my-project.my_dataset' });
  assert.strictEqual(result.sourceTable, '`my-project.my_dataset.events_*`');
});

test('string sourceTable with events_* is normalized to backtick format', () => {
  const defaults = { sourceTable: undefined };
  const result = mergeSQLConfigurations(defaults, { sourceTable: 'my-project.my_dataset.events_*' });
  assert.strictEqual(result.sourceTable, '`my-project.my_dataset.events_*`');
});

test('Dataform reference object sourceTable is preserved as-is', () => {
  const defaults = { sourceTable: undefined };
  const ref = { name: 'events_*', dataset: 'analytics_123' };
  const result = mergeSQLConfigurations(defaults, { sourceTable: ref });
  assert.deepStrictEqual(result.sourceTable, ref);
});

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
console.log('\n---');
console.log(`Total: ${passed + failed}  Passed: ${passed}  Failed: ${failed}`);
if (failed > 0) {
  process.exit(1);
} else {
  console.log('All tests passed.\n');
}
