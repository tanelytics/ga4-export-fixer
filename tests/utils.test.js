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
// buildEnrichments
// ---------------------------------------------------------------------------

console.log('\n2. buildEnrichments\n');

test('empty input returns all-empty output', () => {
    const result = utils.buildEnrichments([]);
    assert.deepStrictEqual(result.steps, []);
    assert.deepStrictEqual(result.event.joins, []);
    assert.deepStrictEqual(result.event.columns, {});
    assert.strictEqual(result.event.columnNames.size, 0);
    assert.deepStrictEqual(result.item.joins, []);
    assert.deepStrictEqual(result.item.columns, {});
    assert.strictEqual(result.item.columnNames.size, 0);
    assert.deepStrictEqual(result.columnOwner, {});
});

test('undefined input is treated as empty', () => {
    const result = utils.buildEnrichments(undefined);
    assert.deepStrictEqual(result.steps, []);
});

test('single event-level enrichment with backtick-FQN source generates one CTE, join, column on the event channel', () => {
    const result = utils.buildEnrichments([
        { name: 'cohorts', level: 'event', source: '`p.d.cohorts`', joinKey: 'user_pseudo_id', columns: ['cohort_label'] },
    ]);
    assert.deepStrictEqual(result.steps, [{
        name: 'enrich_cohorts',
        select: { columns: { user_pseudo_id: 'user_pseudo_id', cohort_label: 'cohort_label' } },
        from: '`p.d.cohorts`',
    }]);
    assert.deepStrictEqual(result.event.joins, [{
        type: 'left',
        table: 'enrich_cohorts',
        on: 'using(user_pseudo_id)',
    }]);
    assert.deepStrictEqual(result.event.columns, { cohort_label: 'enrich_cohorts.cohort_label' });
    assert.ok(result.event.columnNames.has('cohort_label'));
    assert.deepStrictEqual(result.columnOwner.cohort_label, { i: 0, name: 'cohorts', level: 'event' });
    // Item channel is empty
    assert.strictEqual(result.item.joins.length, 0);
    assert.strictEqual(result.item.columnNames.size, 0);
});

test('Dataform-ref-object source passes through verbatim into the source step', () => {
    const refObj = { schema: 'analytics', name: 'cohorts' };
    const result = utils.buildEnrichments([
        { name: 'cohorts', level: 'event', source: refObj, joinKey: 'user_pseudo_id', columns: ['cohort_label'] },
    ]);
    assert.strictEqual(result.steps[0].from, refObj,
        'source ref object should be carried through unmodified');
});

test('composite joinKey selects multiple keys and compiles to using(col1, col2)', () => {
    const result = utils.buildEnrichments([
        { name: 'segments', level: 'event', source: '`p.d.t`', joinKey: ['event_date', 'user_pseudo_id'], columns: ['segment'] },
    ]);
    const sourceCols = result.steps[0].select.columns;
    assert.ok('event_date' in sourceCols, 'event_date should be selected in source CTE');
    assert.ok('user_pseudo_id' in sourceCols, 'user_pseudo_id should be selected in source CTE');
    assert.ok('segment' in sourceCols, 'segment should be selected in source CTE');
    assert.strictEqual(result.event.joins[0].on, 'using(event_date, user_pseudo_id)');
});

test('dedupe: true wraps source CTE with qualify row_number() over (partition by joinKey)', () => {
    const result = utils.buildEnrichments([
        { name: 'dim', level: 'event', source: '`p.d.t`', joinKey: 'id', columns: ['x'], dedupe: true },
    ]);
    assert.strictEqual(result.steps[0].qualify,
        'row_number() over (partition by id) = 1');
});

test('dedupe omitted/false does not add qualify', () => {
    const result = utils.buildEnrichments([
        { name: 'dim', level: 'event', source: '`p.d.t`', joinKey: 'id', columns: ['x'] },
    ]);
    assert.ok(!('qualify' in result.steps[0]),
        'qualify should not be set when dedupe is omitted');
});

test('multiple event-level enrichments aggregate on the event channel preserving entry order', () => {
    const result = utils.buildEnrichments([
        { name: 'a', level: 'event', source: '`p.d.t1`', joinKey: 'id', columns: ['x'] },
        { name: 'b', level: 'event', source: '`p.d.t2`', joinKey: 'id', columns: ['y', 'z'] },
    ]);
    assert.strictEqual(result.steps.length, 2);
    assert.strictEqual(result.steps[0].name, 'enrich_a');
    assert.strictEqual(result.steps[1].name, 'enrich_b');
    assert.strictEqual(result.event.joins.length, 2);
    assert.deepStrictEqual(result.event.columns, {
        x: 'enrich_a.x',
        y: 'enrich_b.y',
        z: 'enrich_b.z',
    });
    assert.strictEqual(result.event.columnNames.size, 3);
    assert.deepStrictEqual(result.columnOwner.x, { i: 0, name: 'a', level: 'event' });
    assert.deepStrictEqual(result.columnOwner.z, { i: 1, name: 'b', level: 'event' });
});

test('item-level enrichment routes to the item channel; event channel remains empty', () => {
    const result = utils.buildEnrichments([
        { name: 'products', level: 'item', source: '`p.d.products`', joinKey: 'item_id', columns: ['margin_bucket'] },
    ]);
    assert.strictEqual(result.steps.length, 1, 'one source CTE is emitted regardless of level');
    assert.strictEqual(result.steps[0].name, 'enrich_products');
    assert.deepStrictEqual(result.item.joins, [{
        type: 'left',
        table: 'enrich_products',
        on: 'using(item_id)',
    }]);
    assert.deepStrictEqual(result.item.columns, { margin_bucket: 'enrich_products.margin_bucket' });
    assert.ok(result.item.columnNames.has('margin_bucket'));
    assert.deepStrictEqual(result.columnOwner.margin_bucket, { i: 0, name: 'products', level: 'item' });
    // Event channel is empty for this purely item-level enrichment
    assert.strictEqual(result.event.joins.length, 0);
    assert.strictEqual(result.event.columnNames.size, 0);
});

test('mixed event + item enrichments route to their respective channels', () => {
    const result = utils.buildEnrichments([
        { name: 'cohorts', level: 'event', source: '`p.d.c`', joinKey: 'user_pseudo_id', columns: ['cohort_label'] },
        { name: 'products', level: 'item', source: '`p.d.p`', joinKey: 'item_id', columns: ['margin_bucket'] },
    ]);
    assert.strictEqual(result.steps.length, 2);
    assert.strictEqual(result.event.joins.length, 1);
    assert.strictEqual(result.item.joins.length, 1);
    assert.deepStrictEqual(result.event.columns, { cohort_label: 'enrich_cohorts.cohort_label' });
    assert.deepStrictEqual(result.item.columns, { margin_bucket: 'enrich_products.margin_bucket' });
});

test('cross-level same-name (event-level + item-level same column) does NOT throw', () => {
    // Event-level cohort lives on enhanced_events; item-level cohort lives inside items[].
    // Distinct output slots — not a collision.
    const result = utils.buildEnrichments([
        { name: 'event_cohorts', level: 'event', source: '`p.d.ec`', joinKey: 'user_pseudo_id', columns: ['cohort'] },
        { name: 'item_cohorts', level: 'item', source: '`p.d.ic`', joinKey: 'item_id', columns: ['cohort'] },
    ]);
    assert.deepStrictEqual(result.event.columns, { cohort: 'enrich_event_cohorts.cohort' });
    assert.deepStrictEqual(result.item.columns, { cohort: 'enrich_item_cohorts.cohort' });
    // columnOwner keyed by name; second writer wins but level distinguishes them.
    assert.deepStrictEqual(result.columnOwner.cohort, { i: 1, name: 'item_cohorts', level: 'item' });
});

test('same-level item-vs-item collision throws with both names, column, and level', () => {
    try {
        utils.buildEnrichments([
            { name: 'a', level: 'item', source: '`p.d.t1`', joinKey: 'item_id', columns: ['margin'] },
            { name: 'b', level: 'item', source: '`p.d.t2`', joinKey: 'item_id', columns: ['margin'] },
        ]);
        assert.fail('should have thrown');
    } catch (e) {
        assert.ok(e.message.includes("'a'") && e.message.includes("'b'"),
            `error should name both enrichments; got: ${e.message}`);
        assert.ok(e.message.includes("'margin'"),
            `error should name the conflicting column; got: ${e.message}`);
        assert.ok(e.message.includes("level 'item'"),
            `error should mention level; got: ${e.message}`);
    }
});

test('enrichment-vs-enrichment column collision throws with both names and the column', () => {
    try {
        utils.buildEnrichments([
            { name: 'a', level: 'event', source: '`p.d.t1`', joinKey: 'id', columns: ['cohort'] },
            { name: 'b', level: 'event', source: '`p.d.t2`', joinKey: 'id', columns: ['cohort'] },
        ]);
        assert.fail('should have thrown');
    } catch (e) {
        assert.ok(e.message.includes("'a'") && e.message.includes("'b'"),
            `error should name both enrichments; got: ${e.message}`);
        assert.ok(e.message.includes("'cohort'"),
            `error should name the conflicting column; got: ${e.message}`);
    }
});

// ---------------------------------------------------------------------------
// buildQualifiedPassThroughs
// ---------------------------------------------------------------------------

console.log('\n3. buildQualifiedPassThroughs\n');

test('empty step columns returns empty result', () => {
    const step = { name: 'event_data', select: { columns: {} } };
    const result = utils.buildQualifiedPassThroughs(step, []);
    assert.deepStrictEqual(result, {});
});

test('all-covered step returns empty result', () => {
    const step = { name: 'event_data', select: { columns: { a: 'a', b: 'b' } } };
    const result = utils.buildQualifiedPassThroughs(step, ['a', 'b']);
    assert.deepStrictEqual(result, {});
});

test('uncovered columns emitted as qualified entries; covered ones skipped', () => {
    const step = { name: 'event_data', select: { columns: { a: 'a', b: 'b', c: 'c' } } };
    const result = utils.buildQualifiedPassThroughs(step, ['b']);
    assert.deepStrictEqual(result, {
        a: 'event_data.a',
        c: 'event_data.c',
    });
});

test('undefined-valued entries (user-exclusion sentinels) are skipped', () => {
    const step = { name: 'event_data', select: { columns: { a: 'a', b: undefined, c: 'c' } } };
    const result = utils.buildQualifiedPassThroughs(step, []);
    assert.deepStrictEqual(result, {
        a: 'event_data.a',
        c: 'event_data.c',
    });
});

test('names in alreadyCovered that do not exist in the step are silently ignored', () => {
    const step = { name: 'event_data', select: { columns: { a: 'a' } } };
    // 'x' and 'y' aren't in the step — must not produce errors or affect output
    const result = utils.buildQualifiedPassThroughs(step, ['x', 'y']);
    assert.deepStrictEqual(result, { a: 'event_data.a' });
});

test('alreadyCovered accepts array and Set with identical results', () => {
    const step = { name: 'event_data', select: { columns: { a: 'a', b: 'b', c: 'c' } } };
    const fromArr = utils.buildQualifiedPassThroughs(step, ['b']);
    const fromSet = utils.buildQualifiedPassThroughs(step, new Set(['b']));
    assert.deepStrictEqual(fromArr, fromSet);
});

test('result preserves Object.entries iteration order', () => {
    const step = { name: 'event_data', select: { columns: { z: 'z', a: 'a', m: 'm' } } };
    const result = utils.buildQualifiedPassThroughs(step, []);
    assert.deepStrictEqual(Object.keys(result), ['z', 'a', 'm']);
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
