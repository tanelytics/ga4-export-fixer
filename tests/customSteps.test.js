/**
 * Tests for the customSteps pipeline feature in tables/ga4EventsEnhanced.
 *
 * Covers:
 * - Layer 2 collision check (runtime-derived reserved set, including conditional reservation)
 * - Pipeline shape with custom steps (empty, one raw, one structured, multiple, mixed)
 * - The renamed `final` -> `enhanced_events` step
 *
 * Layer 1 config-shape validation lives in tests/inputValidation.test.js.
 *
 * Pure Node.js — no BigQuery or Dataform runtime needed.
 */

const assert = require('assert');
const ga4EventsEnhanced = require('../tables/ga4EventsEnhanced');

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

// Minimal config that produces valid SQL (test mode skips Dataform context requirements).
const baseConfig = (overrides = {}) => ({
    sourceTable: '`proj.ds.events_*`',
    test: true,
    incremental: false,
    ...overrides,
});

// ---------------------------------------------------------------------------
// 1. The renamed enhanced_events step (formerly 'final')
// ---------------------------------------------------------------------------

console.log('\n1. enhanced_events step (renamed from final)\n');

test('default pipeline emits enhanced_events as the final SELECT (no CTE wrap)', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig());
    // With no customSteps, enhanced_events is the LAST step → final SELECT, not a CTE
    assert.ok(!sql.includes('enhanced_events as ('), 'enhanced_events should not be wrapped in CTE');
    assert.ok(!sql.includes('final as ('), 'old name `final` should not appear');
});

test('default pipeline still produces valid CTE structure (event_data + session_data)', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig());
    assert.ok(sql.startsWith('with event_data as ('), 'should start with event_data CTE');
    assert.ok(sql.includes('session_data as ('), 'should contain session_data CTE');
});

// ---------------------------------------------------------------------------
// 2. Pipeline shape with customSteps
// ---------------------------------------------------------------------------

console.log('\n2. Pipeline shape with customSteps\n');

test('one raw customStep becomes the final SELECT; enhanced_events becomes a CTE', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        customSteps: [
            { name: 'utm_attr', query: 'select session_id, max(event_timestamp) as ts from event_data group by session_id' },
        ],
    }));
    assert.ok(sql.includes('enhanced_events as ('), 'enhanced_events should now be a CTE');
    assert.ok(sql.endsWith('select session_id, max(event_timestamp) as ts from event_data group by session_id'),
        `last step should be the user's raw query verbatim — got: ${JSON.stringify(sql.slice(-120))}`);
});

test('one structured customStep becomes the final SELECT', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        customSteps: [
            {
                name: 'final_with_marker',
                select: { columns: { '[sql]all': 'enhanced_events.*', marker: "'v2'" } },
                from: 'enhanced_events',
            },
        ],
    }));
    assert.ok(sql.includes('enhanced_events as ('), 'enhanced_events wrapped as CTE');
    // Final SELECT should not be CTE-wrapped — it's the outer query
    assert.ok(!sql.includes('final_with_marker as ('), 'last user step is final SELECT, not a CTE');
    assert.ok(sql.includes("'v2' as marker"));
});

test('multiple customSteps: all but last become CTEs, last is final SELECT, order preserved', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        customSteps: [
            { name: 'step_a', query: 'select 1 as a from enhanced_events limit 1' },
            { name: 'step_b', query: 'select 2 as b from step_a' },
            { name: 'step_c', query: 'select 3 as c from step_b' },
        ],
    }));
    assert.ok(sql.includes('step_a as ('), 'step_a should be a CTE');
    assert.ok(sql.includes('step_b as ('), 'step_b should be a CTE');
    assert.ok(!sql.includes('step_c as ('), 'step_c is the final SELECT, not a CTE');
    // Order preserved: step_a comes before step_b before step_c
    const aIdx = sql.indexOf('step_a as');
    const bIdx = sql.indexOf('step_b as');
    const cIdx = sql.indexOf('select 3 as c');
    assert.ok(aIdx < bIdx && bIdx < cIdx, 'array order must be preserved in CTE chain');
});

test('customSteps can reference event_data (pre-enhanced_events CTE)', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        customSteps: [
            { name: 'event_filtered', query: 'select * from event_data where event_name = \'login\'' },
        ],
    }));
    assert.ok(sql.includes('from event_data where event_name'), 'user reference to event_data preserved');
});

test('customSteps can reference enhanced_events', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        customSteps: [
            { name: 'enhanced_subset', query: 'select * from enhanced_events limit 100' },
        ],
    }));
    assert.ok(sql.includes('from enhanced_events limit 100'));
});

test('mixed raw and structured customSteps coexist in one pipeline', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        customSteps: [
            { name: 'raw_one', query: 'select * from enhanced_events' },
            {
                name: 'structured_two',
                select: '*',
                from: 'raw_one',
                limit: 5,
            },
        ],
    }));
    assert.ok(sql.includes('raw_one as ('), 'raw step renders as CTE');
    assert.ok(sql.includes('limit\n  5'), 'structured step renders with v2 clause format');
});

// ---------------------------------------------------------------------------
// 3. Layer 2 collision check (runtime-derived reserved set)
// ---------------------------------------------------------------------------

console.log('\n3. Layer 2 collision check\n');

test('collision: name `event_data` always reserved → throws', () => {
    assert.throws(
        () => ga4EventsEnhanced.generateSql(baseConfig({
            customSteps: [{ name: 'event_data', query: 'select 1' }],
        })),
        /collides with a reserved package CTE name/
    );
});

test('collision: name `session_data` always reserved → throws', () => {
    assert.throws(
        () => ga4EventsEnhanced.generateSql(baseConfig({
            customSteps: [{ name: 'session_data', query: 'select 1' }],
        })),
        /collides with a reserved package CTE name/
    );
});

test('collision: name `enhanced_events` always reserved → throws', () => {
    assert.throws(
        () => ga4EventsEnhanced.generateSql(baseConfig({
            customSteps: [{ name: 'enhanced_events', query: 'select 1' }],
        })),
        /collides with a reserved package CTE name/
    );
});

test('collision error message names the offender and lists active reserved set', () => {
    try {
        ga4EventsEnhanced.generateSql(baseConfig({
            customSteps: [{ name: 'event_data', query: 'select 1' }],
        }));
        assert.fail('should have thrown');
    } catch (e) {
        assert.ok(e.message.includes("'event_data'"), 'message names offender');
        assert.ok(e.message.includes('event_data'), 'reserved list includes event_data');
        assert.ok(e.message.includes('session_data'), 'reserved list includes session_data');
        assert.ok(e.message.includes('enhanced_events'), 'reserved list includes enhanced_events');
        assert.ok(e.message.includes('config.customSteps[0]'), 'message identifies position in customSteps');
    }
});

// ---------------------------------------------------------------------------
// 4. Conditional reservation (item_list_* depend on itemListAttribution config)
// ---------------------------------------------------------------------------

console.log('\n4. Conditional reservation of item_list_*\n');

test('item_list_data NOT reserved when itemListAttribution is off → does NOT throw', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        // itemListAttribution intentionally omitted/undefined
        customSteps: [{ name: 'item_list_data', query: 'select 1 as fake' }],
    }));
    assert.ok(sql.includes('select 1 as fake'), 'user step rendered when feature off');
});

test('item_list_data IS reserved when itemListAttribution is on → throws', () => {
    assert.throws(
        () => ga4EventsEnhanced.generateSql(baseConfig({
            itemListAttribution: { lookbackType: 'SESSION' },
            customSteps: [{ name: 'item_list_data', query: 'select 1' }],
        })),
        /collides with a reserved package CTE name/
    );
});

test('item_list_attribution NOT reserved when itemListAttribution is off', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        customSteps: [{ name: 'item_list_attribution', query: 'select 2 as fake' }],
    }));
    assert.ok(sql.includes('select 2 as fake'));
});

test('item_list_attribution IS reserved when itemListAttribution is on', () => {
    assert.throws(
        () => ga4EventsEnhanced.generateSql(baseConfig({
            itemListAttribution: { lookbackType: 'SESSION' },
            customSteps: [{ name: 'item_list_attribution', query: 'select 1' }],
        })),
        /collides with a reserved package CTE name/
    );
});

test('reserved set in error message reflects active config (item-list-on enlarges the set)', () => {
    try {
        ga4EventsEnhanced.generateSql(baseConfig({
            itemListAttribution: { lookbackType: 'SESSION' },
            customSteps: [{ name: 'event_data', query: 'select 1' }],
        }));
        assert.fail('should have thrown');
    } catch (e) {
        assert.ok(e.message.includes('item_list_attribution'),
            'item_list_attribution should appear in the active reserved set');
        assert.ok(e.message.includes('item_list_data'),
            'item_list_data should appear in the active reserved set');
    }
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
