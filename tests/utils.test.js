/**
 * Tests for utility functions in utils.js.
 *
 * Pure Node.js — no BigQuery or Dataform runtime needed.
 */

const assert = require('assert');
const utils = require('../utils.js');

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
// buildPassThroughs
// ---------------------------------------------------------------------------

console.log('\n1. buildPassThroughs\n');

test('empty explicit columns + non-empty source list passes every source column through', () => {
    const result = utils.buildPassThroughs({}, ['a', 'b', 'c']);
    assert.deepStrictEqual(result, { a: 'a', b: 'b', c: 'c' });
});

test('empty source list returns empty object', () => {
    const result = utils.buildPassThroughs({ foo: 'foo' }, []);
    assert.deepStrictEqual(result, {});
});

test('explicit key matching a source column skips it', () => {
    const result = utils.buildPassThroughs(
        { event_name: 'event_name' },
        ['event_name', 'event_date']
    );
    assert.deepStrictEqual(result, { event_date: 'event_date' });
});

test('explicit value matching a source column (value-side rename) skips it', () => {
    const result = utils.buildPassThroughs(
        { user_traffic_source: 'traffic_source' },
        ['traffic_source', 'event_date']
    );
    assert.deepStrictEqual(result, { event_date: 'event_date' });
});

test('SQL-expression values do NOT count as coverage', () => {
    const result = utils.buildPassThroughs(
        { event_datetime: `extract(datetime from event_timestamp)` },
        ['event_timestamp', 'event_date']
    );
    assert.deepStrictEqual(result, { event_timestamp: 'event_timestamp', event_date: 'event_date' });
});

test('undefined / function / object values do NOT count as coverage', () => {
    const result = utils.buildPassThroughs(
        {
            excluded: undefined,
            fnRef: () => 'foo',
            objRef: { a: 1 },
        },
        ['foo', 'bar']
    );
    assert.deepStrictEqual(result, { foo: 'foo', bar: 'bar' });
});

test('key and value matching different source columns both skipped', () => {
    const result = utils.buildPassThroughs(
        { user_traffic_source: 'traffic_source', device: 'device' },
        ['traffic_source', 'device', 'geo']
    );
    assert.deepStrictEqual(result, { geo: 'geo' });
});

test('sourceColumns as a Set produces the same result as an array', () => {
    const explicit = { user_traffic_source: 'traffic_source' };
    const fromArray = utils.buildPassThroughs(explicit, ['traffic_source', 'device', 'geo']);
    const fromSet = utils.buildPassThroughs(explicit, new Set(['traffic_source', 'device', 'geo']));
    assert.deepStrictEqual(fromArray, fromSet);
});

test('result preserves source-list iteration order', () => {
    const result = utils.buildPassThroughs({}, ['c', 'a', 'b']);
    assert.deepStrictEqual(Object.keys(result), ['c', 'a', 'b']);
});

test('utility is source-schema-agnostic (works with arbitrary column names)', () => {
    const result = utils.buildPassThroughs(
        { promoted: 'inner_field' },
        ['inner_field', 'unrelated_1', 'unrelated_2']
    );
    assert.deepStrictEqual(result, { unrelated_1: 'unrelated_1', unrelated_2: 'unrelated_2' });
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
