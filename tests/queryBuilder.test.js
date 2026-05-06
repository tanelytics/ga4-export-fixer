/**
 * Tests for queryBuilder in utils.js.
 *
 * Exercises both step shapes (structured and raw), all clause renderers,
 * canonical ordering, indentation rules, and validation error paths.
 * Pure Node.js — no BigQuery or Dataform runtime needed.
 */

const assert = require('assert');
const { queryBuilder } = require('../utils');

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

const assertThrows = (fn, pattern, message) => {
    try {
        fn();
    } catch (e) {
        if (typeof pattern === 'string') {
            assert.ok(e.message.includes(pattern),
                `${message}: expected message to contain "${pattern}", got "${e.message}"`);
        } else {
            assert.ok(pattern.test(e.message),
                `${message}: expected message to match ${pattern}, got "${e.message}"`);
        }
        return;
    }
    assert.fail(`${message}: expected throw, did not throw`);
};

// ---------------------------------------------------------------------------
// 1. Structured shape — basic clause rendering
// ---------------------------------------------------------------------------

console.log('\n1. Structured shape — basic clause rendering\n');

test('select with columns object renders alias map', () => {
    const sql = queryBuilder([
        { name: 'a', select: { columns: { x: 'col_x', y: 'sum(z)' } }, from: 't' },
    ]);
    assert.ok(sql.includes('col_x as x'), 'aliased column missing');
    assert.ok(sql.includes('sum(z) as y'), 'aliased aggregation missing');
});

test('select string form is sugar for {sql: <string>}', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 't' },
    ]);
    assert.match(sql, /select\n\s+\*\nfrom/);
});

test('select object with sql-only emits raw column-list', () => {
    const sql = queryBuilder([
        { name: 'a', select: { sql: '* except (foo)' }, from: 't' },
    ]);
    assert.match(sql, /select\n\s+\* except \(foo\)\nfrom/);
});

test('select with columns + sql joins them in order (columns first)', () => {
    const sql = queryBuilder([
        { name: 'a', select: { columns: { x: 'col_x' }, sql: '* except (y)' }, from: 't' },
    ]);
    const colIdx = sql.indexOf('col_x as x');
    const sqlIdx = sql.indexOf('* except (y)');
    assert.ok(colIdx > 0 && sqlIdx > colIdx, 'columns must appear before sql tail');
    assert.ok(sql.includes(',\n  * except (y)'), 'sql tail not joined with comma+indent');
});

test('key === value skips the alias', () => {
    const sql = queryBuilder([
        { name: 'a', select: { columns: { event_timestamp: 'event_timestamp' } }, from: 't' },
    ]);
    assert.ok(sql.includes('event_timestamp'), 'column missing');
    assert.ok(!sql.includes('event_timestamp as event_timestamp'), 'should not double-print as alias');
});

test('[sql] prefix emits raw value with no alias and no key as text', () => {
    const sql = queryBuilder([
        { name: 'a', select: { columns: { '[sql]other': '* except (x, y)' } }, from: 't' },
    ]);
    assert.ok(sql.includes('* except (x, y)'), 'raw expression missing');
    assert.ok(!sql.includes('[sql]'), 'sql prefix should not appear in output');
    assert.ok(!sql.includes('as other'), 'should not alias an [sql] key');
});

test('undefined column values are filtered out', () => {
    const sql = queryBuilder([
        { name: 'a', select: { columns: { x: 'col_x', y: undefined, z: 'col_z' } }, from: 't' },
    ]);
    assert.ok(sql.includes('col_x as x'));
    assert.ok(sql.includes('col_z as z'));
    assert.ok(!/\by\b/.test(sql) || sql.indexOf('y') < 0 || !sql.includes('as y'),
        'undefined column should not appear');
});

test('from renders with 2-space indent', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 'my_table' },
    ]);
    assert.match(sql, /from\n  my_table/);
});

test('where (single-line) renders with 2-space indent', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 't', where: 'x > 0' },
    ]);
    assert.match(sql, /where\n  x > 0/);
});

test('where (multi-line) reindents continuation lines', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 't', where: 'x > 0\nand y < 10' },
    ]);
    assert.match(sql, /where\n  x > 0\n  and y < 10/);
});

test('group by string renders verbatim', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 't', 'group by': 'x, y' },
    ]);
    assert.match(sql, /group by\n  x, y/);
});

test('having, qualify, order by all render with their keywords', () => {
    const sql = queryBuilder([
        {
            name: 'a',
            select: { columns: { x: 'x', n: 'count(*)' } },
            from: 't',
            'group by': 'x',
            having: 'n > 1',
            qualify: 'row_number() over (partition by x) = 1',
            'order by': 'x',
        },
    ]);
    assert.match(sql, /having\n  n > 1/);
    assert.match(sql, /qualify\n  row_number\(\) over/);
    assert.match(sql, /order by\n  x/);
});

test('limit accepts a number', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 't', limit: 100 },
    ]);
    assert.match(sql, /limit\n  100/);
});

test('limit accepts a string', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 't', limit: '100' },
    ]);
    assert.match(sql, /limit\n  100/);
});

test('absent clauses produce no keyword, no blank line', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 't' },
    ]);
    assert.ok(!sql.includes('where'), 'absent where should not render');
    assert.ok(!sql.includes('group by'), 'absent group by should not render');
    assert.ok(!sql.includes('\n\n'), 'no spurious blank lines');
});

// ---------------------------------------------------------------------------
// 2. Joins clause
// ---------------------------------------------------------------------------

console.log('\n2. Joins clause\n');

test('joins array with one left join', () => {
    const sql = queryBuilder([
        {
            name: 'a',
            select: '*',
            from: 't',
            joins: [{ type: 'left', table: 'u', on: 'using(x)' }],
        },
    ]);
    assert.match(sql, /left join\n  u using\(x\)/);
});

test('joins array preserves entry order across mixed types', () => {
    const sql = queryBuilder([
        {
            name: 'a',
            select: '*',
            from: 't',
            joins: [
                { type: 'left',  table: 'u', on: 'using(x)' },
                { type: 'inner', table: 'v', on: 'using(y)' },
            ],
        },
    ]);
    const leftIdx = sql.indexOf('left join');
    const innerIdx = sql.indexOf('inner join');
    assert.ok(leftIdx > 0 && innerIdx > leftIdx, 'array order must be preserved');
});

test('cross join omits on', () => {
    const sql = queryBuilder([
        {
            name: 'a',
            select: '*',
            from: 't',
            joins: [{ type: 'cross', table: 'unnest(items)' }],
        },
    ]);
    assert.match(sql, /cross join\n  unnest\(items\)/);
    // assert no trailing 'on' for the cross join entry
    const crossLine = sql.split('\n').find(l => l.includes('unnest(items)'));
    assert.ok(!/\bon\b/.test(crossLine), 'cross join should not emit on');
});

test('joins string fallback emits verbatim', () => {
    const sql = queryBuilder([
        {
            name: 'a',
            select: '*',
            from: 't',
            joins: 'left join unnest(items) as item with offset as item_offset on true',
        },
    ]);
    assert.ok(sql.includes('left join unnest(items) as item with offset as item_offset on true'));
});

test('multiple same-type joins via array each get the keyword', () => {
    const sql = queryBuilder([
        {
            name: 'a',
            select: '*',
            from: 't',
            joins: [
                { type: 'left', table: 'u', on: 'using(x)' },
                { type: 'left', table: 'v', on: 'using(y)' },
            ],
        },
    ]);
    const leftMatches = sql.match(/left join/g) || [];
    assert.strictEqual(leftMatches.length, 2, 'both left joins should emit the keyword');
});

// ---------------------------------------------------------------------------
// 3. Canonical clause ordering
// ---------------------------------------------------------------------------

console.log('\n3. Canonical clause ordering\n');

test('clauses emit in canonical order regardless of input key order', () => {
    // Pass keys deliberately out of canonical order
    const sql = queryBuilder([
        {
            name: 'a',
            limit: 10,
            'order by': 'x',
            having: 'n > 1',
            'group by': 'x',
            where: 'x > 0',
            joins: [{ type: 'left', table: 'u', on: 'using(x)' }],
            from: 't',
            select: { columns: { x: 'x', n: 'count(*)' } },
        },
    ]);
    const order = ['select', 'from', 'left join', 'where', 'group by', 'having', 'order by', 'limit'];
    let lastIdx = -1;
    for (const kw of order) {
        const idx = sql.indexOf(kw);
        assert.ok(idx > lastIdx, `${kw} must appear after the previous canonical clause`);
        lastIdx = idx;
    }
});

// ---------------------------------------------------------------------------
// 4. Multi-step CTE wrapping
// ---------------------------------------------------------------------------

console.log('\n4. Multi-step CTE wrapping\n');

test('single step has no CTE wrapping', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 't' },
    ]);
    assert.ok(!sql.startsWith('with '), 'single step should not emit with clause');
});

test('two steps emit with-clause CTE wrapping', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 't' },
        { name: 'final', select: '*', from: 'a' },
    ]);
    assert.ok(sql.startsWith('with a as ('), 'two-step should start with CTE');
    assert.ok(sql.includes(')\nselect'), 'final select should follow CTE');
});

test('CTE bodies are indented inside parentheses', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 't' },
        { name: 'final', select: '*', from: 'a' },
    ]);
    assert.match(sql, /with a as \(\n  select/);
    assert.match(sql, /  from\n    t\n\)/);
});

// ---------------------------------------------------------------------------
// 5. Raw shape
// ---------------------------------------------------------------------------

console.log('\n5. Raw shape\n');

test('raw step renders body verbatim', () => {
    const sql = queryBuilder([
        { name: 'r', query: 'select x, y from t where x > 0' },
        { name: 'final', select: '*', from: 'r' },
    ]);
    assert.ok(sql.includes('select x, y from t where x > 0'));
});

test('raw body multi-line content preserves relative indentation', () => {
    const body = 'select\n  a,\n  b\nfrom t';
    const sql = queryBuilder([
        { name: 'r', query: body },
        { name: 'final', select: '*', from: 'r' },
    ]);
    // Inside CTE, base indent shifts by 2 spaces
    assert.match(sql, /  select\n    a,\n    b\n  from t/);
});

test('raw step composes correctly with structured steps in a chain', () => {
    const sql = queryBuilder([
        { name: 'a', select: '*', from: 't' },
        { name: 'r', query: 'select * from a' },
        { name: 'final', select: '*', from: 'r' },
    ]);
    assert.ok(sql.startsWith('with a as ('));
    assert.ok(sql.includes('r as ('));
    assert.ok(sql.includes('select * from a'));
});

test('raw step as final step (no CTE wrapping) emits body alone', () => {
    const sql = queryBuilder([
        { name: 'final', query: 'select 1 as x' },
    ]);
    assert.strictEqual(sql, 'select 1 as x');
});

// ---------------------------------------------------------------------------
// 6. Validation errors
// ---------------------------------------------------------------------------

console.log('\n6. Validation errors\n');

test('unknown structured-step key throws with clear message', () => {
    assertThrows(
        () => queryBuilder([{ name: 'a', select: '*', from: 't', wehre: 'x > 0' }]),
        '`wehre`',
        'should name the offending key'
    );
});

test('unknown key error lists allowed keys', () => {
    assertThrows(
        () => queryBuilder([{ name: 'a', select: '*', from: 't', xyz: 1 }]),
        'Allowed keys:',
        'should list allowed keys'
    );
});

test('mixing raw query with structured key throws shape-mutually-exclusive error', () => {
    assertThrows(
        () => queryBuilder([{ name: 'a', query: 'select 1', where: 'x' }]),
        'mutually exclusive',
        'should explain shape conflict'
    );
});

test('structured step missing select throws', () => {
    assertThrows(
        () => queryBuilder([{ name: 'a', from: 't' }]),
        'requires `select`',
        'should require select'
    );
});

test('structured step missing from throws', () => {
    assertThrows(
        () => queryBuilder([{ name: 'a', select: '*' }]),
        'requires `from`',
        'should require from'
    );
});

test('raw step with empty query throws', () => {
    assertThrows(
        () => queryBuilder([{ name: 'a', query: '' }]),
        'non-empty `query`',
        'should reject empty query'
    );
});

test('select with no columns and no sql throws', () => {
    assertThrows(
        () => queryBuilder([{ name: 'a', select: {}, from: 't' }]),
        'must include at least one of',
        'should require columns or sql'
    );
});

test('null step throws clear error', () => {
    assertThrows(
        () => queryBuilder([null]),
        'each step must be a non-null object',
        'should reject null step'
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
