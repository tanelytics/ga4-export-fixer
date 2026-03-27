/**
 * Tests for setPreOperations and the export status evaluation SQL.
 *
 * Group 1 – Variable declaration tests (pure Node.js, no BigQuery).
 *   Verifies which DECLARE statements, DELETE, and CREATE TABLE appear
 *   for each combination of export types, refresh mode, test mode, and
 *   sourceTableType.
 *
 * Group 2 – Export status evaluation tests (BigQuery execution, 0 bytes).
 *   Replaces INFORMATION_SCHEMA.TABLES with UNNEST of simulated table
 *   names and executes the query against BigQuery to verify the evaluated
 *   date values returned by getExportDateRangeStart.
 */

require('dotenv').config({ path: require('path').join(__dirname, '.env') });

const assert = require('assert');
const { setPreOperations, _internal } = require('../preOperations');
const { getExportDateRangeStart } = _internal;

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

const asyncTest = async (name, fn) => {
  try {
    await fn();
    passed++;
    console.log(`  PASS: ${name}`);
  } catch (err) {
    failed++;
    console.error(`  FAIL: ${name}`);
    console.error(`        ${err.message}\n`);
  }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const extractDeclaredVariables = (sql) => {
  const matches = [...sql.matchAll(/declare\s+(\w+)\s+default/g)];
  return matches.map(m => m[1]).sort();
};

const hasDeleteStatement = (sql) => sql.includes('delete from');
const hasCreateStatement = (sql) => sql.includes('create or replace table');

const daysAgoStr = (n) => {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - n);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}${m}${day}`;
};

const expectedDate = (n) => {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - n);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
};

const ga4Config = (overrides) => ({
  sourceTable: '`p.d.events_*`',
  sourceTableType: 'GA4_EXPORT',
  self: '`p.d.result`',
  test: false,
  incremental: true,
  includedExportTypes: { daily: true, fresh: false, intraday: true },
  preOperations: {
    dateRangeStartFullRefresh: "date(2000,1,1)",
    dateRangeEnd: 'current_date()',
    numberOfPreviousDaysToScan: 10,
    numberOfDaysToProcess: undefined,
  },
  testConfig: {
    dateRangeStart: 'current_date()-1',
    dateRangeEnd: 'current_date()',
  },
  ...overrides,
});

const INCREMENTAL_BASE = ['date_range_end', 'date_range_start', 'last_partition_date'];

// ---------------------------------------------------------------------------
// Group 1: Variable declaration tests
// ---------------------------------------------------------------------------

console.log('\n1. Incremental mode - variable declarations\n');

test('Daily only, incremental: base variables + DELETE', () => {
  const sql = setPreOperations(ga4Config({
    includedExportTypes: { daily: true, fresh: false, intraday: false },
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), INCREMENTAL_BASE);
  assert.strictEqual(hasDeleteStatement(sql), true);
  assert.strictEqual(hasCreateStatement(sql), false);
});

test('Fresh only, incremental: base variables + DELETE', () => {
  const sql = setPreOperations(ga4Config({
    includedExportTypes: { daily: false, fresh: true, intraday: false },
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), INCREMENTAL_BASE);
  assert.strictEqual(hasDeleteStatement(sql), true);
});

test('Intraday only, incremental: base variables + DELETE', () => {
  const sql = setPreOperations(ga4Config({
    includedExportTypes: { daily: false, fresh: false, intraday: true },
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), INCREMENTAL_BASE);
  assert.strictEqual(hasDeleteStatement(sql), true);
});

test('Daily+Intraday, incremental: base + intraday_date_range_start + DELETE', () => {
  const sql = setPreOperations(ga4Config({
    includedExportTypes: { daily: true, fresh: false, intraday: true },
  }));
  assert.deepStrictEqual(
    extractDeclaredVariables(sql),
    [...INCREMENTAL_BASE, 'intraday_date_range_start'].sort()
  );
  assert.strictEqual(hasDeleteStatement(sql), true);
});

test('Daily+Fresh, incremental: base + fresh_date_range_start + DELETE', () => {
  const sql = setPreOperations(ga4Config({
    includedExportTypes: { daily: true, fresh: true, intraday: false },
  }));
  assert.deepStrictEqual(
    extractDeclaredVariables(sql),
    [...INCREMENTAL_BASE, 'fresh_date_range_start'].sort()
  );
  assert.strictEqual(hasDeleteStatement(sql), true);
});

test('Fresh+Intraday, incremental: base + fresh_max_event_timestamp + DELETE', () => {
  const sql = setPreOperations(ga4Config({
    includedExportTypes: { daily: false, fresh: true, intraday: true },
  }));
  assert.deepStrictEqual(
    extractDeclaredVariables(sql),
    [...INCREMENTAL_BASE, 'fresh_max_event_timestamp'].sort()
  );
  assert.strictEqual(hasDeleteStatement(sql), true);
});

test('All three, incremental: base + fresh_date_range_start + fresh_max_event_timestamp + DELETE', () => {
  const sql = setPreOperations(ga4Config({
    includedExportTypes: { daily: true, fresh: true, intraday: true },
  }));
  assert.deepStrictEqual(
    extractDeclaredVariables(sql),
    [...INCREMENTAL_BASE, 'fresh_date_range_start', 'fresh_max_event_timestamp'].sort()
  );
  assert.strictEqual(hasDeleteStatement(sql), true);
});

// ---------------------------------------------------------------------------

console.log('\n2. Full refresh mode - variable declarations\n');

test('Daily only, full refresh: no variables, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    incremental: false,
    includedExportTypes: { daily: true, fresh: false, intraday: false },
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), []);
  assert.strictEqual(hasDeleteStatement(sql), false);
});

test('Fresh only, full refresh: no variables, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    incremental: false,
    includedExportTypes: { daily: false, fresh: true, intraday: false },
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), []);
  assert.strictEqual(hasDeleteStatement(sql), false);
});

test('Intraday only, full refresh: no variables, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    incremental: false,
    includedExportTypes: { daily: false, fresh: false, intraday: true },
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), []);
  assert.strictEqual(hasDeleteStatement(sql), false);
});

test('Daily+Intraday, full refresh: intraday_date_range_start only, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    incremental: false,
    includedExportTypes: { daily: true, fresh: false, intraday: true },
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), ['intraday_date_range_start']);
  assert.strictEqual(hasDeleteStatement(sql), false);
});

test('Daily+Fresh, full refresh: fresh_date_range_start only, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    incremental: false,
    includedExportTypes: { daily: true, fresh: true, intraday: false },
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), ['fresh_date_range_start']);
  assert.strictEqual(hasDeleteStatement(sql), false);
});

test('Fresh+Intraday, full refresh: fresh_max_event_timestamp only, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    incremental: false,
    includedExportTypes: { daily: false, fresh: true, intraday: true },
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), ['fresh_max_event_timestamp']);
  assert.strictEqual(hasDeleteStatement(sql), false);
});

test('All three, full refresh: fresh_date_range_start + fresh_max_event_timestamp, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    incremental: false,
    includedExportTypes: { daily: true, fresh: true, intraday: true },
  }));
  assert.deepStrictEqual(
    extractDeclaredVariables(sql),
    ['fresh_date_range_start', 'fresh_max_event_timestamp'].sort()
  );
  assert.strictEqual(hasDeleteStatement(sql), false);
});

// ---------------------------------------------------------------------------

console.log('\n3. Test mode - variable declarations\n');

test('Test mode, daily+intraday (no fresh): empty string', () => {
  const sql = setPreOperations(ga4Config({
    test: true,
    incremental: false,
    includedExportTypes: { daily: true, fresh: false, intraday: true },
  }));
  assert.strictEqual(sql, '');
});

test('Test mode, daily only: empty string', () => {
  const sql = setPreOperations(ga4Config({
    test: true,
    incremental: false,
    includedExportTypes: { daily: true, fresh: false, intraday: false },
  }));
  assert.strictEqual(sql, '');
});

test('Test mode, fresh+daily: fresh_date_range_start only, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    test: true,
    incremental: false,
    includedExportTypes: { daily: true, fresh: true, intraday: false },
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), ['fresh_date_range_start']);
  assert.strictEqual(hasDeleteStatement(sql), false);
  assert.strictEqual(hasCreateStatement(sql), false);
});

test('Test mode, fresh+intraday: fresh_max_event_timestamp only, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    test: true,
    incremental: false,
    includedExportTypes: { daily: false, fresh: true, intraday: true },
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), ['fresh_max_event_timestamp']);
  assert.strictEqual(hasDeleteStatement(sql), false);
});

test('Test mode, all three: fresh_date_range_start + fresh_max_event_timestamp only, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    test: true,
    incremental: false,
    includedExportTypes: { daily: true, fresh: true, intraday: true },
  }));
  assert.deepStrictEqual(
    extractDeclaredVariables(sql),
    ['fresh_date_range_start', 'fresh_max_event_timestamp'].sort()
  );
  assert.strictEqual(hasDeleteStatement(sql), false);
});

// ---------------------------------------------------------------------------

console.log('\n4. Downstream tables (sourceTableType !== GA4_EXPORT)\n');

test('Downstream, incremental: base variables + DELETE, no export-specific vars', () => {
  const sql = setPreOperations(ga4Config({
    sourceTableType: 'DOWNSTREAM',
    incremental: true,
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), INCREMENTAL_BASE);
  assert.strictEqual(hasDeleteStatement(sql), true);
  assert.strictEqual(hasCreateStatement(sql), false);
});

test('Downstream, full refresh: no variables, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    sourceTableType: 'DOWNSTREAM',
    incremental: false,
  }));
  assert.deepStrictEqual(extractDeclaredVariables(sql), []);
  assert.strictEqual(hasDeleteStatement(sql), false);
});

test('Downstream, test mode: empty string', () => {
  const sql = setPreOperations(ga4Config({
    sourceTableType: 'DOWNSTREAM',
    test: true,
    incremental: false,
  }));
  assert.strictEqual(sql, '');
});

// ---------------------------------------------------------------------------

console.log('\n5. Schema lock\n');

test('GA4_EXPORT with schemaLock, full refresh: CREATE present, no DELETE', () => {
  const sql = setPreOperations(ga4Config({
    incremental: false,
    schemaLock: '20260101',
    includedExportTypes: { daily: true, fresh: false, intraday: false },
  }));
  assert.strictEqual(hasCreateStatement(sql), true);
  assert.strictEqual(hasDeleteStatement(sql), false);
});

test('GA4_EXPORT with schemaLock, test mode: no CREATE (empty string)', () => {
  const sql = setPreOperations(ga4Config({
    test: true,
    incremental: false,
    schemaLock: '20260101',
    includedExportTypes: { daily: true, fresh: false, intraday: false },
  }));
  assert.strictEqual(sql, '');
});

test('Downstream with schemaLock: no CREATE', () => {
  const sql = setPreOperations(ga4Config({
    sourceTableType: 'DOWNSTREAM',
    incremental: false,
    schemaLock: '20260101',
  }));
  assert.strictEqual(hasCreateStatement(sql), false);
});

// ---------------------------------------------------------------------------
// Group 2: Export status evaluation tests (BigQuery execution)
// ---------------------------------------------------------------------------

const simulateExportStatuses = (config, targetExportType, simulatedTableNames) => {
  const sql = getExportDateRangeStart(config, targetExportType);
  return sql.replace(
    /from\s+`[^`]+\.INFORMATION_SCHEMA\.TABLES`/,
    `from UNNEST([${simulatedTableNames.map(t => `'${t}'`).join(', ')}]) as table_name`
  );
};

const runExportStatusTests = async () => {
  const { BigQuery } = require('@google-cloud/bigquery');
  const bigquery = new BigQuery();
  const location = process.env.BIGQUERY_LOCATION || 'US';

  const executeQuery = async (sql) => {
    const [rows] = await bigquery.query({ query: sql, location });
    const value = rows[0] ? Object.values(rows[0])[0] : null;
    return value && value.value ? value.value : value;
  };

  const config = ga4Config({ incremental: false });

  console.log('\n6. Export status evaluation (BigQuery)\n');

  // Scenario 1: Daily through yesterday, fresh+intraday for yesterday and today → fresh target = today
  await asyncTest('Fresh target: daily through D-1, fresh+intraday D-1..D-0 → D-0', async () => {
    const sql = simulateExportStatuses(config, 'fresh', [
      `events_${daysAgoStr(1)}`,
      `events_fresh_${daysAgoStr(1)}`,
      `events_fresh_${daysAgoStr(0)}`,
      `events_intraday_${daysAgoStr(1)}`,
      `events_intraday_${daysAgoStr(0)}`,
    ]);
    const result = await executeQuery(sql);
    assert.strictEqual(result, expectedDate(0));
  });

  // Scenario 2: Same tables, intraday target → today
  await asyncTest('Intraday target: daily through D-1, fresh+intraday D-1..D-0 → D-0', async () => {
    const sql = simulateExportStatuses(config, 'intraday', [
      `events_${daysAgoStr(1)}`,
      `events_fresh_${daysAgoStr(1)}`,
      `events_fresh_${daysAgoStr(0)}`,
      `events_intraday_${daysAgoStr(1)}`,
      `events_intraday_${daysAgoStr(0)}`,
    ]);
    const result = await executeQuery(sql);
    assert.strictEqual(result, expectedDate(0));
  });

  // Scenario 3: Daily covers everything including today → fresh target = null
  await asyncTest('Fresh target: daily covers all days → null', async () => {
    const sql = simulateExportStatuses(config, 'fresh', [
      `events_${daysAgoStr(1)}`,
      `events_${daysAgoStr(0)}`,
      `events_fresh_${daysAgoStr(0)}`,
      `events_intraday_${daysAgoStr(0)}`,
    ]);
    const result = await executeQuery(sql);
    assert.strictEqual(result, null);
  });

  // Scenario 4: Daily covers everything → intraday target = null
  await asyncTest('Intraday target: daily covers all days → null', async () => {
    const sql = simulateExportStatuses(config, 'intraday', [
      `events_${daysAgoStr(1)}`,
      `events_${daysAgoStr(0)}`,
      `events_fresh_${daysAgoStr(0)}`,
      `events_intraday_${daysAgoStr(0)}`,
    ]);
    const result = await executeQuery(sql);
    assert.strictEqual(result, null);
  });

  // Scenario 5: No daily at all → fresh target = earliest fresh day
  await asyncTest('Fresh target: no daily, fresh D-2..D-1 → D-2', async () => {
    const sql = simulateExportStatuses(config, 'fresh', [
      `events_fresh_${daysAgoStr(2)}`,
      `events_fresh_${daysAgoStr(1)}`,
      `events_intraday_${daysAgoStr(1)}`,
      `events_intraday_${daysAgoStr(0)}`,
    ]);
    const result = await executeQuery(sql);
    assert.strictEqual(result, expectedDate(2));
  });

  // Scenario 6: No daily → intraday target = earliest intraday-without-daily day
  await asyncTest('Intraday target: no daily, intraday D-1..D-0 → D-1', async () => {
    const sql = simulateExportStatuses(config, 'intraday', [
      `events_fresh_${daysAgoStr(2)}`,
      `events_fresh_${daysAgoStr(1)}`,
      `events_intraday_${daysAgoStr(1)}`,
      `events_intraday_${daysAgoStr(0)}`,
    ]);
    const result = await executeQuery(sql);
    assert.strictEqual(result, expectedDate(1));
  });

  // Scenario 7: Gap in daily → fresh target = first non-daily day
  await asyncTest('Fresh target: daily D-4..D-3, fresh D-2..D-1 → D-2', async () => {
    const sql = simulateExportStatuses(config, 'fresh', [
      `events_${daysAgoStr(4)}`,
      `events_${daysAgoStr(3)}`,
      `events_fresh_${daysAgoStr(2)}`,
      `events_fresh_${daysAgoStr(1)}`,
      `events_intraday_${daysAgoStr(1)}`,
      `events_intraday_${daysAgoStr(0)}`,
    ]);
    const result = await executeQuery(sql);
    assert.strictEqual(result, expectedDate(2));
  });

  // Scenario 8: Gap in daily → intraday target = first intraday-without-daily day
  await asyncTest('Intraday target: daily D-4..D-3, intraday D-1..D-0 → D-1', async () => {
    const sql = simulateExportStatuses(config, 'intraday', [
      `events_${daysAgoStr(4)}`,
      `events_${daysAgoStr(3)}`,
      `events_fresh_${daysAgoStr(2)}`,
      `events_fresh_${daysAgoStr(1)}`,
      `events_intraday_${daysAgoStr(1)}`,
      `events_intraday_${daysAgoStr(0)}`,
    ]);
    const result = await executeQuery(sql);
    assert.strictEqual(result, expectedDate(1));
  });

  // Scenario 9: Old intraday beyond 5-day window is excluded
  await asyncTest('Intraday target: D-8 intraday excluded by 5-day limit → D-0', async () => {
    const sql = simulateExportStatuses(config, 'intraday', [
      `events_fresh_${daysAgoStr(1)}`,
      `events_intraday_${daysAgoStr(8)}`,
      `events_intraday_${daysAgoStr(0)}`,
    ]);
    const result = await executeQuery(sql);
    assert.strictEqual(result, expectedDate(0));
  });

  // Scenario 10: Old fresh is NOT limited by 5-day window
  await asyncTest('Fresh target: D-8 fresh NOT excluded (no lower date bound) → D-8', async () => {
    const sql = simulateExportStatuses(config, 'fresh', [
      `events_fresh_${daysAgoStr(8)}`,
      `events_intraday_${daysAgoStr(0)}`,
    ]);
    const result = await executeQuery(sql);
    assert.strictEqual(result, expectedDate(8));
  });
};

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

const run = async () => {
  await runExportStatusTests();

  console.log('\n---');
  console.log(`Total: ${passed + failed}  Passed: ${passed}  Failed: ${failed}`);

  if (failed > 0) {
    console.error('Some tests failed.\n');
    process.exit(1);
  } else {
    console.log('All tests passed.\n');
    process.exit(0);
  }
};

run();
