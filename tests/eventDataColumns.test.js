/**
 * Tests for the explicit-column-listing shape of the event_data CTE in tables/ga4EventsEnhanced.
 *
 * Covers:
 * - event_data emits every GA4 export column (transformed or as a pass-through), never via wildcard
 * - User-excluded GA4 columns are dropped from event_data's SELECT
 * - eventParamsToColumns promoted columns coexist with pass-throughs
 *
 * Pure Node.js — no BigQuery or Dataform runtime needed.
 */

const assert = require('assert');
const ga4EventsEnhanced = require('../tables/ga4EventsEnhanced');
const helpers = require('../helpers');

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

const baseConfig = (overrides = {}) => ({
    sourceTable: '`proj.ds.events_*`',
    test: true,
    incremental: false,
    ...overrides,
});

// Extract just the body of the event_data CTE for line-based scanning.
const eventDataBody = (sql) => {
    const startMatch = sql.match(/event_data as \(\s*\n\s*select\n/);
    if (!startMatch) throw new Error('event_data CTE not found');
    const start = startMatch.index + startMatch[0].length;
    const end = sql.indexOf('\n  from\n', start);
    return sql.slice(start, end);
};

// Pass-through entries render as a bare identifier line at indent level 4: "    column,"
// or "    column" (final entry). This regex matches those exactly.
const hasPassThroughLine = (body, column) => {
    const re = new RegExp(`^\\s+${column},?\\s*$`, 'm');
    return re.test(body);
};

console.log('\n1. event_data emits explicit columns (no wildcard)\n');

test('event_data CTE does NOT contain "* except" wildcard', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig());
    const body = eventDataBody(sql);
    assert.ok(!body.includes('* except'),
        `event_data CTE body must not contain a wildcard EXCEPT`);
});

test('event_data emits a pass-through line for non-excluded GA4 export columns', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig());
    const body = eventDataBody(sql);
    // Sample of GA4 export columns that are NOT in defaultExcludedColumns and NOT consumed as
    // value-side renames or explicit transforms — these should appear as bare pass-through lines.
    const expectedPassThroughs = [
        'event_previous_timestamp',
        'event_value_in_usd',
        'privacy_info',
        'user_first_touch_timestamp',
        'user_ltv',
        'device',
        'geo',
        'app_info',
        'stream_id',
        'platform',
        'is_active_user',
        'publisher',
    ];
    for (const col of expectedPassThroughs) {
        assert.ok(hasPassThroughLine(body, col),
            `event_data should have a pass-through line for "${col}"`);
    }
});

test('user-excluded GA4 columns are dropped from event_data', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        excludedColumns: ['app_info', 'publisher'],
    }));
    const body = eventDataBody(sql);
    assert.ok(!hasPassThroughLine(body, 'app_info'),
        'user-excluded column app_info must not have a pass-through line');
    assert.ok(!hasPassThroughLine(body, 'publisher'),
        'user-excluded column publisher must not have a pass-through line');
    // Other GA4 columns still flow through
    assert.ok(hasPassThroughLine(body, 'device'),
        'non-excluded GA4 column device should still have a pass-through line');
    assert.ok(hasPassThroughLine(body, 'geo'),
        'non-excluded GA4 column geo should still have a pass-through line');
});

test('default-excluded GA4 columns are dropped from event_data', () => {
    // defaultExcludedColumns: event_dimensions, traffic_source, session_id
    // traffic_source is also consumed as a value-side rename (user_traffic_source).
    // session_id is consumed by the explicit session_id transform.
    const sql = ga4EventsEnhanced.generateSql(baseConfig());
    const body = eventDataBody(sql);
    assert.ok(!hasPassThroughLine(body, 'event_dimensions'),
        'default-excluded event_dimensions must not appear as a pass-through');
    assert.ok(!hasPassThroughLine(body, 'traffic_source'),
        'value-side-renamed traffic_source must not appear as a pass-through');
});

test('eventParamsToColumns promoted columns coexist with pass-throughs', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        eventParamsToColumns: [{ name: 'page_title', type: 'string' }],
    }));
    const body = eventDataBody(sql);
    assert.ok(/ as page_title,?\s*$/m.test(body),
        'promoted column page_title should appear as "expr as page_title"');
    assert.ok(hasPassThroughLine(body, 'device'),
        'pass-through device should still be emitted');
});

test('helpers.ga4ExportColumns is exported and non-empty', () => {
    assert.ok(Array.isArray(helpers.ga4ExportColumns),
        'helpers.ga4ExportColumns should be an array');
    assert.ok(helpers.ga4ExportColumns.length > 0,
        'helpers.ga4ExportColumns should not be empty');
    assert.ok(helpers.ga4ExportColumns.includes('event_name'),
        'helpers.ga4ExportColumns should include event_name');
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
