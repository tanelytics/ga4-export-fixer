/**
 * Tests for input validation modules:
 * - inputValidation.js (validateBaseConfig)
 * - tables/ga4EventsEnhanced/validation.js (validateEnhancedEventsConfig)
 *
 * Covers every error path, boundary condition, and the skipDataformContextFields option.
 * Pure Node.js — no BigQuery calls.
 */

const assert = require('assert');
const { validateBaseConfig } = require('../inputValidation');
const { validateEnhancedEventsConfig } = require('../tables/ga4EventsEnhanced/validation');

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
// Helpers
// ---------------------------------------------------------------------------

const validBaseConfig = (overrides = {}) => ({
    self: '`project.dataset.table`',
    incremental: false,
    test: false,
    testConfig: { dateRangeStart: 'current_date()-1', dateRangeEnd: 'current_date()' },
    preOperations: {
        dateRangeStartFullRefresh: 'date(2000, 1, 1)',
        dateRangeEnd: 'current_date()',
        numberOfPreviousDaysToScan: 10,
    },
    ...overrides,
});

const validEnhancedConfig = (overrides = {}) => ({
    ...validBaseConfig(),
    sourceTable: '`project.dataset.events_*`',
    sourceTableType: 'GA4_EXPORT',
    schemaLock: undefined,
    includedExportTypes: { daily: true, fresh: false, intraday: true },
    timezone: 'Etc/UTC',
    customTimestampParam: undefined,
    dataIsFinal: { detectionMethod: 'DAY_THRESHOLD', dayThreshold: 3 },
    bufferDays: 1,
    defaultExcludedEventParams: ['page_location', 'ga_session_id'],
    excludedEventParams: [],
    eventParamsToColumns: [],
    sessionParams: [],
    defaultExcludedEvents: [],
    excludedEvents: ['session_start', 'first_visit'],
    defaultExcludedColumns: ['event_dimensions', 'traffic_source', 'session_id'],
    excludedColumns: [],
    ...overrides,
});

// ---------------------------------------------------------------------------
// 1. validateBaseConfig — config object type
// ---------------------------------------------------------------------------

console.log('\n1. validateBaseConfig — config object type\n');

test('rejects null', () => {
    assert.throws(() => validateBaseConfig(null), /config must be a non-null object/);
});

test('rejects undefined', () => {
    assert.throws(() => validateBaseConfig(undefined), /config must be a non-null object/);
});

test('rejects array', () => {
    assert.throws(() => validateBaseConfig([]), /config must be a non-null object/);
});

test('rejects string', () => {
    assert.throws(() => validateBaseConfig('hello'), /config must be a non-null object/);
});

test('rejects number', () => {
    assert.throws(() => validateBaseConfig(42), /config must be a non-null object/);
});

// ---------------------------------------------------------------------------
// 2. validateBaseConfig — self and incremental (Dataform context fields)
// ---------------------------------------------------------------------------

console.log('\n2. validateBaseConfig — self and incremental\n');

test('rejects missing self when test !== true', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ self: undefined })),
        /config\.self is required/
    );
});

test('rejects non-string self', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ self: 123 })),
        /config\.self is required/
    );
});

test('rejects self without backtick format', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ self: 'project.dataset.table' })),
        /config\.self is required/
    );
});

test('rejects empty string self', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ self: '' })),
        /config\.self is required/
    );
});

test('accepts valid self with backticks', () => {
    validateBaseConfig(validBaseConfig({ self: '`project.dataset.table`' }));
});

test('skips self validation when test === true', () => {
    validateBaseConfig(validBaseConfig({ test: true, self: undefined }));
});

test('rejects non-boolean incremental', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ incremental: 'false' })),
        /config\.incremental must be a boolean/
    );
});

test('rejects undefined incremental', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ incremental: undefined })),
        /config\.incremental must be a boolean/
    );
});

test('accepts boolean incremental (true)', () => {
    validateBaseConfig(validBaseConfig({ incremental: true }));
});

test('accepts boolean incremental (false)', () => {
    validateBaseConfig(validBaseConfig({ incremental: false }));
});

test('skips self and incremental with skipDataformContextFields', () => {
    validateBaseConfig(
        validBaseConfig({ self: undefined, incremental: undefined }),
        { skipDataformContextFields: true }
    );
});

// ---------------------------------------------------------------------------
// 3. validateBaseConfig — test field
// ---------------------------------------------------------------------------

console.log('\n3. validateBaseConfig — test field\n');

test('accepts undefined test (optional)', () => {
    const config = validBaseConfig();
    delete config.test;
    validateBaseConfig(config);
});

test('accepts true', () => {
    validateBaseConfig(validBaseConfig({ test: true, self: undefined }));
});

test('accepts false', () => {
    validateBaseConfig(validBaseConfig({ test: false }));
});

test('rejects non-boolean test', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ test: 'true' })),
        /config\.test must be a boolean/
    );
});

// ---------------------------------------------------------------------------
// 4. validateBaseConfig — testConfig
// ---------------------------------------------------------------------------

console.log('\n4. validateBaseConfig — testConfig\n');

test('accepts undefined testConfig (optional)', () => {
    const config = validBaseConfig();
    delete config.testConfig;
    validateBaseConfig(config);
});

test('rejects null testConfig', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ testConfig: null })),
        /config\.testConfig must be an object/
    );
});

test('rejects array testConfig', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ testConfig: [] })),
        /config\.testConfig must be an object/
    );
});

test('rejects string testConfig', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ testConfig: 'foo' })),
        /config\.testConfig must be an object/
    );
});

test('rejects non-string dateRangeStart', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ testConfig: { dateRangeStart: 123 } })),
        /config\.testConfig\.dateRangeStart must be a non-empty string/
    );
});

test('rejects empty string dateRangeStart', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ testConfig: { dateRangeStart: '   ' } })),
        /config\.testConfig\.dateRangeStart must be a non-empty string/
    );
});

test('rejects non-string dateRangeEnd', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ testConfig: { dateRangeEnd: false } })),
        /config\.testConfig\.dateRangeEnd must be a non-empty string/
    );
});

test('rejects empty string dateRangeEnd', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ testConfig: { dateRangeEnd: '' } })),
        /config\.testConfig\.dateRangeEnd must be a non-empty string/
    );
});

test('accepts valid testConfig', () => {
    validateBaseConfig(validBaseConfig({
        testConfig: { dateRangeStart: 'current_date()-7', dateRangeEnd: 'current_date()' }
    }));
});

// ---------------------------------------------------------------------------
// 5. validateBaseConfig — preOperations
// ---------------------------------------------------------------------------

console.log('\n5. validateBaseConfig — preOperations\n');

test('rejects missing preOperations', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ preOperations: undefined })),
        /config\.preOperations is required/
    );
});

test('rejects null preOperations', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ preOperations: null })),
        /config\.preOperations must be an object/
    );
});

test('rejects array preOperations', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({ preOperations: [] })),
        /config\.preOperations must be an object/
    );
});

test('rejects missing numberOfPreviousDaysToScan', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()' }
        })),
        /numberOfPreviousDaysToScan is required/
    );
});

test('rejects negative numberOfPreviousDaysToScan', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: -1 }
        })),
        /numberOfPreviousDaysToScan must be a non-negative integer/
    );
});

test('rejects float numberOfPreviousDaysToScan', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 3.5 }
        })),
        /numberOfPreviousDaysToScan must be a non-negative integer/
    );
});

test('rejects NaN numberOfPreviousDaysToScan', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: NaN }
        })),
        /numberOfPreviousDaysToScan must be a non-negative integer/
    );
});

test('rejects string numberOfPreviousDaysToScan', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: '10' }
        })),
        /numberOfPreviousDaysToScan must be a non-negative integer/
    );
});

test('accepts zero numberOfPreviousDaysToScan', () => {
    validateBaseConfig(validBaseConfig({
        preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 0 }
    }));
});

test('rejects missing dateRangeStartFullRefresh', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 10 }
        })),
        /dateRangeStartFullRefresh is required/
    );
});

test('rejects null dateRangeStartFullRefresh', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: null, dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 10 }
        })),
        /dateRangeStartFullRefresh is required/
    );
});

test('rejects empty string dateRangeStartFullRefresh', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: '  ', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 10 }
        })),
        /dateRangeStartFullRefresh must be a non-empty string/
    );
});

test('rejects missing dateRangeEnd', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', numberOfPreviousDaysToScan: 10 }
        })),
        /dateRangeEnd is required/
    );
});

test('rejects null dateRangeEnd', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: null, numberOfPreviousDaysToScan: 10 }
        })),
        /dateRangeEnd is required/
    );
});

test('rejects empty string dateRangeEnd', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: '', numberOfPreviousDaysToScan: 10 }
        })),
        /dateRangeEnd must be a non-empty string/
    );
});

test('rejects zero numberOfDaysToProcess', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 10, numberOfDaysToProcess: 0 }
        })),
        /numberOfDaysToProcess must be a positive integer/
    );
});

test('rejects negative numberOfDaysToProcess', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 10, numberOfDaysToProcess: -5 }
        })),
        /numberOfDaysToProcess must be a positive integer/
    );
});

test('rejects float numberOfDaysToProcess', () => {
    assert.throws(
        () => validateBaseConfig(validBaseConfig({
            preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 10, numberOfDaysToProcess: 2.5 }
        })),
        /numberOfDaysToProcess must be a positive integer/
    );
});

test('accepts valid numberOfDaysToProcess', () => {
    validateBaseConfig(validBaseConfig({
        preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 10, numberOfDaysToProcess: 7 }
    }));
});

test('accepts undefined numberOfDaysToProcess (optional)', () => {
    validateBaseConfig(validBaseConfig({
        preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 10 }
    }));
});

test('accepts valid incrementalStartOverride', () => {
    validateBaseConfig(validBaseConfig({
        preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 10, incrementalStartOverride: 'date(2026,1,1)' }
    }));
});

test('accepts undefined incrementalStartOverride (optional)', () => {
    validateBaseConfig(validBaseConfig());
});

test('accepts valid incrementalEndOverride', () => {
    validateBaseConfig(validBaseConfig({
        preOperations: { dateRangeStartFullRefresh: 'date(2000,1,1)', dateRangeEnd: 'current_date()', numberOfPreviousDaysToScan: 10, incrementalEndOverride: 'date(2026,12,31)' }
    }));
});

test('accepts valid full base config', () => {
    validateBaseConfig(validBaseConfig());
});

// ---------------------------------------------------------------------------
// 6. validateEnhancedEventsConfig — error prefix
// ---------------------------------------------------------------------------

console.log('\n6. validateEnhancedEventsConfig — error prefix\n');

test('prefixes errors with "Config validation:"', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(null),
        /Config validation:/
    );
});

test('prefixes field errors with "Config validation:"', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ sourceTable: undefined })),
        /Config validation:.*sourceTable is required/
    );
});

// ---------------------------------------------------------------------------
// 7. validateEnhancedEventsConfig — sourceTable
// ---------------------------------------------------------------------------

console.log('\n7. validateEnhancedEventsConfig — sourceTable\n');

test('rejects missing sourceTable', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ sourceTable: undefined })),
        /sourceTable is required/
    );
});

test('rejects null sourceTable', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ sourceTable: null })),
        /sourceTable is required/
    );
});

test('rejects empty string sourceTable', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ sourceTable: '  ' })),
        /sourceTable must be a non-empty string/
    );
});

test('rejects string without backtick format', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ sourceTable: 'project.dataset.events_*' })),
        /sourceTable must be in the format/
    );
});

test('rejects number sourceTable', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ sourceTable: 123 })),
        /sourceTable must be a Dataform table reference/
    );
});

test('accepts valid backtick string sourceTable', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ sourceTable: '`project.dataset.events_*`' }));
});

test('accepts Dataform reference object sourceTable', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({
        sourceTable: { name: 'events_*', dataset: 'analytics_123' }
    }));
});

test('accepts Dataform reference object with schema', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({
        sourceTable: { name: 'events_*', schema: 'analytics_123' }
    }));
});

// ---------------------------------------------------------------------------
// 8. validateEnhancedEventsConfig — schemaLock
// ---------------------------------------------------------------------------

console.log('\n8. validateEnhancedEventsConfig — schemaLock\n');

test('accepts undefined schemaLock (optional)', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: undefined }));
});

test('rejects non-YYYYMMDD format', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: '2024-10-09' })),
        /schemaLock must be a string in "YYYYMMDD", "intraday_YYYYMMDD", or "fresh_YYYYMMDD" format/
    );
});

test('rejects non-string schemaLock', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: 20241009 })),
        /schemaLock must be a string in "YYYYMMDD", "intraday_YYYYMMDD", or "fresh_YYYYMMDD" format/
    );
});

test('rejects invalid date (20241332)', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: '20241332' })),
        /schemaLock must contain a valid date/
    );
});

test('rejects date before 20241009', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: '20241008' })),
        /schemaLock date must be equal to or greater than "20241009"/
    );
});

test('accepts 20241009 (minimum)', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: '20241009' }));
});

test('accepts valid future date', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: '20260101' }));
});

test('accepts intraday prefix', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: 'intraday_20260101' }));
});

test('accepts fresh prefix', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: 'fresh_20260101' }));
});

test('accepts intraday with minimum date', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: 'intraday_20241009' }));
});

test('rejects unknown prefix', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: 'streaming_20260101' })),
        /schemaLock must be a string in "YYYYMMDD", "intraday_YYYYMMDD", or "fresh_YYYYMMDD" format/
    );
});

test('rejects intraday prefix with invalid date', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: 'intraday_20241332' })),
        /schemaLock must contain a valid date/
    );
});

test('rejects fresh prefix with date before minimum', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ schemaLock: 'fresh_20241008' })),
        /schemaLock date must be equal to or greater than "20241009"/
    );
});

// ---------------------------------------------------------------------------
// 9. validateEnhancedEventsConfig — includedExportTypes
// ---------------------------------------------------------------------------

console.log('\n9. validateEnhancedEventsConfig — includedExportTypes\n');

test('rejects missing includedExportTypes', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ includedExportTypes: undefined })),
        /includedExportTypes is required/
    );
});

test('rejects non-object includedExportTypes', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ includedExportTypes: 'daily' })),
        /includedExportTypes must be an object/
    );
});

test('rejects array includedExportTypes', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ includedExportTypes: ['daily'] })),
        /includedExportTypes must be an object/
    );
});

test('rejects missing daily key', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ includedExportTypes: { fresh: false, intraday: true } })),
        /includedExportTypes\.daily is required/
    );
});

test('rejects missing fresh key', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ includedExportTypes: { daily: true, intraday: true } })),
        /includedExportTypes\.fresh is required/
    );
});

test('rejects missing intraday key', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ includedExportTypes: { daily: true, fresh: false } })),
        /includedExportTypes\.intraday is required/
    );
});

test('rejects non-boolean value', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ includedExportTypes: { daily: 'true', fresh: false, intraday: true } })),
        /includedExportTypes\.daily must be a boolean/
    );
});

test('rejects all-false export types', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ includedExportTypes: { daily: false, fresh: false, intraday: false } })),
        /At least one of/
    );
});

test('accepts single export type enabled', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({
        includedExportTypes: { daily: true, fresh: false, intraday: false }
    }));
});

// ---------------------------------------------------------------------------
// 10. validateEnhancedEventsConfig — timezone
// ---------------------------------------------------------------------------

console.log('\n10. validateEnhancedEventsConfig — timezone\n');

test('rejects missing timezone', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ timezone: undefined })),
        /timezone is required/
    );
});

test('rejects empty string timezone', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ timezone: '  ' })),
        /timezone must be a non-empty string/
    );
});

test('rejects non-string timezone', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ timezone: 123 })),
        /timezone must be a non-empty string/
    );
});

test('accepts valid timezone', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ timezone: 'Europe/Helsinki' }));
});

// ---------------------------------------------------------------------------
// 11. validateEnhancedEventsConfig — customTimestampParam
// ---------------------------------------------------------------------------

console.log('\n11. validateEnhancedEventsConfig — customTimestampParam\n');

test('accepts undefined customTimestampParam (optional)', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ customTimestampParam: undefined }));
});

test('rejects empty string customTimestampParam', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ customTimestampParam: '' })),
        /customTimestampParam must be a non-empty string/
    );
});

test('rejects non-string customTimestampParam', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ customTimestampParam: 123 })),
        /customTimestampParam must be a non-empty string/
    );
});

test('accepts valid customTimestampParam', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ customTimestampParam: 'custom_ts' }));
});

// ---------------------------------------------------------------------------
// 12. validateEnhancedEventsConfig — dataIsFinal
// ---------------------------------------------------------------------------

console.log('\n12. validateEnhancedEventsConfig — dataIsFinal\n');

test('rejects missing dataIsFinal', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ dataIsFinal: undefined })),
        /dataIsFinal is required/
    );
});

test('rejects non-object dataIsFinal', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ dataIsFinal: 'DAY_THRESHOLD' })),
        /dataIsFinal must be an object/
    );
});

test('rejects array dataIsFinal', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ dataIsFinal: [] })),
        /dataIsFinal must be an object/
    );
});

test('rejects missing detectionMethod', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ dataIsFinal: {} })),
        /detectionMethod is required/
    );
});

test('rejects invalid detectionMethod', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ dataIsFinal: { detectionMethod: 'INVALID' } })),
        /detectionMethod must be 'EXPORT_TYPE' or 'DAY_THRESHOLD'/
    );
});

test('rejects DAY_THRESHOLD without dayThreshold', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ dataIsFinal: { detectionMethod: 'DAY_THRESHOLD' } })),
        /dayThreshold is required when detectionMethod is 'DAY_THRESHOLD'/
    );
});

test('rejects negative dayThreshold', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ dataIsFinal: { detectionMethod: 'DAY_THRESHOLD', dayThreshold: -1 } })),
        /dayThreshold must be a non-negative integer/
    );
});

test('rejects float dayThreshold', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ dataIsFinal: { detectionMethod: 'DAY_THRESHOLD', dayThreshold: 2.5 } })),
        /dayThreshold must be a non-negative integer/
    );
});

test('accepts zero dayThreshold', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ dataIsFinal: { detectionMethod: 'DAY_THRESHOLD', dayThreshold: 0 } }));
});

test('rejects EXPORT_TYPE when daily is disabled', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({
            includedExportTypes: { daily: false, fresh: false, intraday: true },
            dataIsFinal: { detectionMethod: 'EXPORT_TYPE' }
        })),
        /detectionMethod must be 'DAY_THRESHOLD' when daily export is not enabled/
    );
});

test('accepts EXPORT_TYPE when daily is enabled', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({
        includedExportTypes: { daily: true, fresh: false, intraday: true },
        dataIsFinal: { detectionMethod: 'EXPORT_TYPE' }
    }));
});

test('accepts valid DAY_THRESHOLD config', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({
        dataIsFinal: { detectionMethod: 'DAY_THRESHOLD', dayThreshold: 3 }
    }));
});

// ---------------------------------------------------------------------------
// 13. validateEnhancedEventsConfig — bufferDays
// ---------------------------------------------------------------------------

console.log('\n13. validateEnhancedEventsConfig — bufferDays\n');

test('rejects missing bufferDays', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ bufferDays: undefined })),
        /bufferDays must be a non-negative integer/
    );
});

test('rejects negative bufferDays', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ bufferDays: -1 })),
        /bufferDays must be a non-negative integer/
    );
});

test('rejects float bufferDays', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ bufferDays: 1.5 })),
        /bufferDays must be a non-negative integer/
    );
});

test('rejects string bufferDays', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ bufferDays: '1' })),
        /bufferDays must be a non-negative integer/
    );
});

test('accepts zero bufferDays', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ bufferDays: 0 }));
});

test('accepts positive bufferDays', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ bufferDays: 3 }));
});

// ---------------------------------------------------------------------------
// 14. validateEnhancedEventsConfig — itemListAttribution
// ---------------------------------------------------------------------------

console.log('\n14. validateEnhancedEventsConfig — itemListAttribution\n');

test('accepts undefined itemListAttribution (disabled by default)', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: undefined }));
});

test('rejects non-object itemListAttribution', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: 'SESSION' })),
        /itemListAttribution must be an object when provided/
    );
});

test('rejects array itemListAttribution', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: [] })),
        /itemListAttribution must be an object when provided/
    );
});

test('rejects empty object (missing lookbackType)', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: {} })),
        /lookbackType is required/
    );
});

test('rejects invalid lookbackType', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: { lookbackType: 'INVALID' } })),
        /lookbackType must be 'SESSION' or 'TIME'/
    );
});

test('accepts SESSION lookbackType', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: { lookbackType: 'SESSION' } }));
});

test('accepts TIME lookbackType with lookbackTimeMs', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: { lookbackType: 'TIME', lookbackTimeMs: 86400000 } }));
});

test('rejects TIME lookbackType without lookbackTimeMs', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: { lookbackType: 'TIME' } })),
        /lookbackTimeMs is required when lookbackType is 'TIME'/
    );
});

test('rejects non-integer lookbackTimeMs', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: { lookbackType: 'TIME', lookbackTimeMs: 1.5 } })),
        /lookbackTimeMs must be a positive integer/
    );
});

test('rejects zero lookbackTimeMs', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: { lookbackType: 'TIME', lookbackTimeMs: 0 } })),
        /lookbackTimeMs must be a positive integer/
    );
});

test('rejects negative lookbackTimeMs', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: { lookbackType: 'TIME', lookbackTimeMs: -1000 } })),
        /lookbackTimeMs must be a positive integer/
    );
});

test('accepts SESSION with lookbackTimeMs (ignored)', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ itemListAttribution: { lookbackType: 'SESSION', lookbackTimeMs: 86400000 } }));
});

// ---------------------------------------------------------------------------
// 15. validateEnhancedEventsConfig — string array fields
// ---------------------------------------------------------------------------

console.log('\n15. validateEnhancedEventsConfig — string array fields\n');

const stringArrayFields = [
    'defaultExcludedEventParams',
    'excludedEventParams',
    'sessionParams',
    'defaultExcludedEvents',
    'excludedEvents',
    'excludedColumns',
];

for (const field of stringArrayFields) {
    test(`rejects missing ${field}`, () => {
        assert.throws(
            () => validateEnhancedEventsConfig(validEnhancedConfig({ [field]: undefined })),
            new RegExp(`config\\.${field} is required`)
        );
    });

    test(`rejects non-array ${field}`, () => {
        assert.throws(
            () => validateEnhancedEventsConfig(validEnhancedConfig({ [field]: 'not_array' })),
            new RegExp(`config\\.${field} must be an array`)
        );
    });

    test(`rejects ${field} with empty string element`, () => {
        assert.throws(
            () => validateEnhancedEventsConfig(validEnhancedConfig({ [field]: ['valid', ''] })),
            new RegExp(`config\\.${field}\\[1\\] must be a non-empty string`)
        );
    });

    test(`accepts empty ${field} array`, () => {
        validateEnhancedEventsConfig(validEnhancedConfig({ [field]: [] }));
    });

    test(`accepts ${field} with valid strings`, () => {
        validateEnhancedEventsConfig(validEnhancedConfig({ [field]: ['param1', 'param2'] }));
    });
}

// ---------------------------------------------------------------------------
// 16. validateEnhancedEventsConfig — eventParamsToColumns
// ---------------------------------------------------------------------------

console.log('\n16. validateEnhancedEventsConfig — eventParamsToColumns\n');

test('rejects missing eventParamsToColumns', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ eventParamsToColumns: undefined })),
        /eventParamsToColumns is required/
    );
});

test('rejects non-array eventParamsToColumns', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ eventParamsToColumns: {} })),
        /eventParamsToColumns must be an array/
    );
});

test('rejects item that is not an object', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ eventParamsToColumns: ['page_title'] })),
        /eventParamsToColumns\[0\] must be an object/
    );
});

test('rejects item without name', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ eventParamsToColumns: [{ type: 'string' }] })),
        /eventParamsToColumns\[0\]\.name must be a non-empty string/
    );
});

test('rejects item with empty name', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ eventParamsToColumns: [{ name: '', type: 'string' }] })),
        /eventParamsToColumns\[0\]\.name must be a non-empty string/
    );
});

test('rejects invalid type', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ eventParamsToColumns: [{ name: 'param', type: 'boolean' }] })),
        /eventParamsToColumns\[0\]\.type must be one of/
    );
});

test('accepts all valid types', () => {
    const validTypes = ['string', 'int', 'int64', 'double', 'float', 'float64'];
    for (const type of validTypes) {
        validateEnhancedEventsConfig(validEnhancedConfig({
            eventParamsToColumns: [{ name: 'param', type }]
        }));
    }
});

test('accepts item without type (optional)', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({
        eventParamsToColumns: [{ name: 'page_title' }]
    }));
});

test('accepts item with null type', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({
        eventParamsToColumns: [{ name: 'page_title', type: null }]
    }));
});

test('rejects whitespace-only columnName', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({
            eventParamsToColumns: [{ name: 'param', type: 'string', columnName: '   ' }]
        })),
        /eventParamsToColumns\[0\]\.columnName must be a non-empty string/
    );
});

test('accepts valid columnName', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({
        eventParamsToColumns: [{ name: 'page_title', type: 'string', columnName: 'title' }]
    }));
});

test('accepts empty eventParamsToColumns array', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ eventParamsToColumns: [] }));
});

// ---------------------------------------------------------------------------
// 17. validateEnhancedEventsConfig — valid full config
// ---------------------------------------------------------------------------

console.log('\n17. validateEnhancedEventsConfig — valid full config\n');

test('accepts fully valid enhanced config', () => {
    validateEnhancedEventsConfig(validEnhancedConfig());
});

test('accepts valid config with all optional fields populated', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({
        schemaLock: '20260101',
        customTimestampParam: 'custom_ts',
        eventParamsToColumns: [
            { name: 'page_title', type: 'string' },
            { name: 'content_group', type: 'string', columnName: 'cg' },
        ],
        sessionParams: ['engagement_time'],
        excludedEvents: ['session_start', 'first_visit', 'custom_event'],
        excludedEventParams: ['debug_mode'],
        excludedColumns: ['publisher'],
    }));
});

test('accepts valid config with skipDataformContextFields', () => {
    validateEnhancedEventsConfig(
        validEnhancedConfig({ self: undefined, incremental: undefined }),
        { skipDataformContextFields: true }
    );
});

// ---------------------------------------------------------------------------
// customSteps — Layer 1 config-shape validation
// (Layer 2 collision check is in tests/customSteps.test.js since it requires
//  generateSql to derive the package's reserved-name set at runtime.)
// ---------------------------------------------------------------------------

console.log('\nN. customSteps Layer 1 validation\n');

test('accepts undefined customSteps', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ customSteps: undefined }));
});

test('accepts empty customSteps array', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({ customSteps: [] }));
});

test('accepts valid customSteps entries', () => {
    validateEnhancedEventsConfig(validEnhancedConfig({
        customSteps: [
            { name: 'extra_a', query: 'select 1' },
            { name: 'extra_b', query: 'select 2' },
        ],
    }));
});

test('rejects non-array customSteps', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ customSteps: 'nope' })),
        /config\.customSteps must be an array/
    );
});

test('rejects null entry in customSteps', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ customSteps: [null] })),
        /config\.customSteps\[0\] must be a non-null object/
    );
});

test('rejects array entry in customSteps', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ customSteps: [['a', 'b']] })),
        /config\.customSteps\[0\] must be a non-null object/
    );
});

test('rejects entry missing name', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ customSteps: [{ query: 'select 1' }] })),
        /config\.customSteps\[0\]\.name must be a non-empty string/
    );
});

test('rejects entry with empty/whitespace name', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ customSteps: [{ name: '   ', query: 'select 1' }] })),
        /config\.customSteps\[0\]\.name must be a non-empty string/
    );
});

test('rejects entry with non-string name', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({ customSteps: [{ name: 42, query: 'select 1' }] })),
        /config\.customSteps\[0\]\.name must be a non-empty string/
    );
});

test('rejects duplicate names within customSteps', () => {
    assert.throws(
        () => validateEnhancedEventsConfig(validEnhancedConfig({
            customSteps: [
                { name: 'dup', query: 'select 1' },
                { name: 'dup', query: 'select 2' },
            ],
        })),
        /config\.customSteps contains duplicate name 'dup'/
    );
});

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

console.log(`\n---\nTotal: ${passed + failed}  Passed: ${passed}  Failed: ${failed}`);
if (failed > 0) {
    console.error(`${failed} test(s) failed.\n`);
    process.exit(1);
} else {
    console.log('All tests passed.\n');
}
