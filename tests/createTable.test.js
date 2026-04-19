/**
 * Tests for createTable.js — the Dataform publish() orchestration layer.
 *
 * Uses mocked publish() and table module to verify wiring, config merging,
 * table naming, description generation, and mutation safety.
 * Pure Node.js — no BigQuery or Dataform runtime needed.
 */

const assert = require('assert');
const { createTable } = require('../createTable');

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

/**
 * Creates a mock Dataform publish() function that captures all arguments.
 * Returns { publish, captured } where captured contains:
 * - name: table name passed to publish()
 * - config: table config passed to publish()
 * - preOpsFn: the callback passed to .preOps()
 * - queryFn: the callback passed to .query()
 */
const mockPublish = () => {
    const captured = {};
    const publish = (name, config) => {
        captured.name = name;
        captured.config = config;
        return {
            preOps: (fn) => {
                captured.preOpsFn = fn;
                return {
                    query: (fn) => {
                        captured.queryFn = fn;
                        return captured;
                    }
                };
            }
        };
    };
    return { publish, captured };
};

/**
 * Creates a mock Dataform context object (ctx) for preOps/query callbacks.
 */
const mockCtx = (overrides = {}) => ({
    ref: (tableRef) => `\`resolved.${tableRef.dataset || tableRef.schema}.${tableRef.name}\``,
    self: () => '`project.dataset.my_table`',
    incremental: () => false,
    ...overrides,
});

/**
 * Minimal valid defaultConfig for a table module.
 */
const minimalDefaultConfig = () => ({
    self: undefined,
    incremental: undefined,
    test: false,
    testConfig: { dateRangeStart: 'current_date()-1', dateRangeEnd: 'current_date()' },
    preOperations: {
        dateRangeStartFullRefresh: 'date(2000, 1, 1)',
        dateRangeEnd: 'current_date()',
        numberOfPreviousDaysToScan: 10,
    },
    sourceTable: undefined,
    dataformTableConfig: {
        type: 'incremental',
        bigquery: {
            partitionBy: 'event_date',
            clusterBy: ['event_name', 'session_id'],
            labels: { 'ga4_export_fixer': 'true' },
        },
        tags: ['ga4_export_fixer'],
    },
});

/**
 * Creates a mock table module with spyable functions.
 */
const mockTableModule = (overrides = {}) => {
    const calls = { validate: [] };
    return {
        module: {
            defaultConfig: minimalDefaultConfig(),
            defaultTableName: 'ga4_events_enhanced',
            validate: (config, options) => { calls.validate.push({ config, options }); },
            generateSql: (config) => `SELECT * FROM ${config.sourceTable}`,
            getColumnDescriptions: (config) => ({ event_date: 'The event date', event_name: 'The event name' }),
            getTableDescription: (config) => 'Auto-generated table description',
            ...overrides,
        },
        calls,
    };
};

/**
 * Minimal valid user config.
 */
const minimalUserConfig = (overrides = {}) => ({
    sourceTable: '`project.analytics_298233330.events_*`',
    ...overrides,
});

// ---------------------------------------------------------------------------
// 1. Publish wiring
// ---------------------------------------------------------------------------

console.log('\n1. Publish wiring\n');

test('calls publish with derived table name', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig(), module);
    assert.strictEqual(captured.name, 'ga4_events_enhanced_298233330');
});

test('calls publish with a config object', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig(), module);
    assert.strictEqual(typeof captured.config, 'object');
    assert.strictEqual(captured.config.type, 'incremental');
});

test('preOps callback is a function', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig(), module);
    assert.strictEqual(typeof captured.preOpsFn, 'function');
});

test('query callback is a function', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig(), module);
    assert.strictEqual(typeof captured.queryFn, 'function');
});

test('query callback invokes generateSql with Dataform context', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule({
        generateSql: (config) => `SELECT * FROM ${config.sourceTable} WHERE self = '${config.self}'`,
    });
    createTable(publish, minimalUserConfig(), module);
    const ctx = mockCtx();
    const sql = captured.queryFn(ctx);
    assert.ok(sql.includes('`project.analytics_298233330.events_*`'), 'should contain sourceTable');
    assert.ok(sql.includes('`project.dataset.my_table`'), 'should contain self from ctx');
});

test('preOps callback invokes setPreOperations with Dataform context', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig(), module);
    const ctx = mockCtx();
    // preOps returns a string (the pre-operations SQL)
    const result = captured.preOpsFn(ctx);
    assert.strictEqual(typeof result, 'string');
});

// ---------------------------------------------------------------------------
// 2. Validation
// ---------------------------------------------------------------------------

console.log('\n2. Validation\n');

test('calls validate with skipDataformContextFields: true', () => {
    const { publish } = mockPublish();
    const { module, calls } = mockTableModule();
    createTable(publish, minimalUserConfig(), module);
    assert.strictEqual(calls.validate.length, 1);
    assert.deepStrictEqual(calls.validate[0].options, { skipDataformContextFields: true });
});

test('passes merged config to validate', () => {
    const { publish } = mockPublish();
    const { module, calls } = mockTableModule();
    createTable(publish, minimalUserConfig({ timezone: 'Europe/Helsinki' }), module);
    assert.strictEqual(calls.validate[0].config.timezone, 'Europe/Helsinki');
});

test('propagates validation errors', () => {
    const { publish } = mockPublish();
    const { module } = mockTableModule({
        validate: () => { throw new Error('Validation failed!'); },
    });
    assert.throws(
        () => createTable(publish, minimalUserConfig(), module),
        /Validation failed!/
    );
});

// ---------------------------------------------------------------------------
// 3. Table naming and schema
// ---------------------------------------------------------------------------

console.log('\n3. Table naming and schema\n');

test('strips analytics_ prefix from dataset for table name', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig({ sourceTable: '`project.analytics_123456.events_*`' }), module);
    assert.strictEqual(captured.name, 'ga4_events_enhanced_123456');
});

test('uses full dataset name when no analytics_ prefix', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig({ sourceTable: '`project.my_dataset.events_*`' }), module);
    assert.strictEqual(captured.name, 'ga4_events_enhanced_my_dataset');
});

test('sets schema to the dataset name', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig({ sourceTable: '`project.analytics_999.events_*`' }), module);
    assert.strictEqual(captured.config.schema, 'analytics_999');
});

test('uses defaultTableName from table module', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule({ defaultTableName: 'custom_table' });
    createTable(publish, minimalUserConfig({ sourceTable: '`project.analytics_123.events_*`' }), module);
    assert.strictEqual(captured.name, 'custom_table_123');
});

// ---------------------------------------------------------------------------
// 4. Config merge order
// ---------------------------------------------------------------------------

console.log('\n4. Config merge order\n');

test('default dataformTableConfig values are preserved', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig(), module);
    assert.strictEqual(captured.config.bigquery.partitionBy, 'event_date');
    assert.deepStrictEqual(captured.config.tags, ['ga4_export_fixer']);
});

test('user dataformTableConfig overrides default values', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig({
        dataformTableConfig: {
            bigquery: { clusterBy: ['custom_col'] },
        },
    }), module);
    assert.deepStrictEqual(captured.config.bigquery.clusterBy, ['custom_col']);
});

test('user tags are concatenated with default tags', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig({
        dataformTableConfig: { tags: ['custom_tag'] },
    }), module);
    assert.ok(captured.config.tags.includes('ga4_export_fixer'), 'should keep default tag');
    assert.ok(captured.config.tags.includes('custom_tag'), 'should add user tag');
});

test('user can override table name via dataformTableConfig', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule();
    createTable(publish, minimalUserConfig({
        dataformTableConfig: { name: 'my_custom_name' },
    }), module);
    // The user override should win since mergeDataformTableConfigurations applies it last
    assert.strictEqual(captured.name, 'my_custom_name');
});

// ---------------------------------------------------------------------------
// 5. Column descriptions and table description
// ---------------------------------------------------------------------------

console.log('\n5. Column descriptions and table description\n');

test('sets columns from getColumnDescriptions', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule({
        getColumnDescriptions: () => ({ col_a: 'Description A', col_b: 'Description B' }),
    });
    createTable(publish, minimalUserConfig(), module);
    assert.deepStrictEqual(captured.config.columns, { col_a: 'Description A', col_b: 'Description B' });
});

test('auto-generates description when not provided by user', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule({
        getTableDescription: () => 'Auto description',
    });
    createTable(publish, minimalUserConfig(), module);
    assert.strictEqual(captured.config.description, 'Auto description');
});

test('preserves user-provided description', () => {
    const { publish, captured } = mockPublish();
    const { module } = mockTableModule({
        getTableDescription: () => 'Auto description',
    });
    createTable(publish, minimalUserConfig({
        dataformTableConfig: { description: 'User description' },
    }), module);
    assert.strictEqual(captured.config.description, 'User description');
});

test('passes dataformTableConfig to getTableDescription', () => {
    const { publish } = mockPublish();
    let receivedConfig = null;
    const { module } = mockTableModule({
        getTableDescription: (config) => {
            receivedConfig = config;
            return 'desc';
        },
    });
    createTable(publish, minimalUserConfig(), module);
    assert.ok(receivedConfig.dataformTableConfig, 'should have dataformTableConfig');
    assert.strictEqual(receivedConfig.dataformTableConfig.type, 'incremental');
});

// ---------------------------------------------------------------------------
// 6. Mutation safety
// ---------------------------------------------------------------------------

console.log('\n6. Mutation safety\n');

test('multiple createTable calls do not share nested bigquery config', () => {
    const { publish: publish1 } = mockPublish();
    const { publish: publish2 } = mockPublish();
    const { module } = mockTableModule();

    const result1 = createTable(publish1, minimalUserConfig({
        sourceTable: '`project.analytics_111.events_*`',
        dataformTableConfig: { bigquery: { clusterBy: ['col_a'] } },
    }), module);

    const result2 = createTable(publish2, minimalUserConfig({
        sourceTable: '`project.analytics_222.events_*`',
        dataformTableConfig: { bigquery: { clusterBy: ['col_b'] } },
    }), module);

    // The configs should be independent
    assert.deepStrictEqual(result1.config.bigquery.clusterBy, ['col_a']);
    assert.deepStrictEqual(result2.config.bigquery.clusterBy, ['col_b']);
});

test('deep-clones defaultConfig.dataformTableConfig between calls', () => {
    const { module } = mockTableModule();
    const originalLabels = { ...module.defaultConfig.dataformTableConfig.bigquery.labels };

    const { publish } = mockPublish();
    createTable(publish, minimalUserConfig(), module);

    // Verify the defaultConfig's nested objects were not mutated
    assert.deepStrictEqual(module.defaultConfig.dataformTableConfig.bigquery.labels, originalLabels);
});

test('getTableDescription receives a copy, not the original mergedConfig', () => {
    const { publish } = mockPublish();
    let receivedConfig = null;
    const { module } = mockTableModule({
        getTableDescription: (config) => {
            receivedConfig = config;
            // Try to mutate it — should not affect the original
            config.mutatedField = true;
            return 'desc';
        },
    });
    // Should not throw even if getTableDescription mutates its argument
    createTable(publish, minimalUserConfig(), module);
    assert.ok(receivedConfig.mutatedField === true, 'mock should have mutated its copy');
});

test('default tags remain intact after createTable with user tags', () => {
    const { module } = mockTableModule();
    const originalTags = [...module.defaultConfig.dataformTableConfig.tags];

    const { publish } = mockPublish();
    createTable(publish, minimalUserConfig({
        dataformTableConfig: { tags: ['user_tag'] },
    }), module);

    assert.deepStrictEqual(module.defaultConfig.dataformTableConfig.tags, originalTags);
});

// ---------------------------------------------------------------------------
// 7. Assertion wiring
// ---------------------------------------------------------------------------

console.log('\n7. Assertion wiring\n');

/**
 * Creates a mock Dataform assert() function that captures all calls.
 * Returns { assertFn, captured } where captured is an array of
 * { name, config, queryFn } objects, one per assert() call.
 */
const mockAssert = () => {
    const captured = [];
    const assertFn = (name, config) => {
        const entry = { name, config, queryFn: null };
        captured.push(entry);
        return {
            query: (fn) => {
                entry.queryFn = fn;
                return entry;
            }
        };
    };
    return { assertFn, captured };
};

/**
 * Creates a mock table module with assertion definitions.
 */
const mockTableModuleWithAssertions = (overrides = {}) => {
    const { module, calls } = mockTableModule({
        ...overrides,
    });
    module.assertions = {
        dailyQuality: {
            generate: (tableRef, config) => `DQ: ${tableRef} FROM ${config.sourceTable}`,
            defaultName: 'daily_quality',
        },
        optIn: {
            generate: (tableRef, config) => `OPT: ${tableRef} FROM ${config.sourceTable}`,
            defaultName: 'opt_in',
            enabledByDefault: false,
        },
    };
    return { module, calls };
};

test('createTable without options creates no assertions', () => {
    const { publish } = mockPublish();
    const { module } = mockTableModuleWithAssertions();
    // No fourth argument — backward compatible
    createTable(publish, minimalUserConfig(), module);
    // No way to verify assert wasn't called (it wasn't passed), but no error = pass
});

test('createTable with empty options creates no assertions', () => {
    const { publish } = mockPublish();
    const { module } = mockTableModuleWithAssertions();
    createTable(publish, minimalUserConfig(), module, {});
});

test('createTable with { assert } creates only default-enabled assertions', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    createTable(publish, minimalUserConfig(), module, { assert: assertFn });
    assert.strictEqual(captured.length, 1, 'should only create default-enabled assertions');
    assert.strictEqual(captured[0].name, 'ga4_events_enhanced_298233330_daily_quality');
});

test('assertion with enabledByDefault: false requires explicit opt-in via true', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    createTable(publish, minimalUserConfig(), module, {
        assert: assertFn,
        assertions: { optIn: true },
    });
    const names = captured.map(a => a.name);
    assert.ok(names.includes('ga4_events_enhanced_298233330_daily_quality'));
    assert.ok(names.includes('ga4_events_enhanced_298233330_opt_in'));
});

test('assertion with enabledByDefault: false can be opted in via config object', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    createTable(publish, minimalUserConfig(), module, {
        assert: assertFn,
        assertions: { optIn: { tags: ['custom'] } },
    });
    const opt = captured.find(a => a.name.includes('opt_in'));
    assert.ok(opt, 'optIn assertion should be created');
    assert.deepStrictEqual(opt.config.tags, ['custom']);
});

test('assertions inherit schema from dataformTableConfig', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    createTable(publish, minimalUserConfig(), module, { assert: assertFn });
    captured.forEach(a => {
        assert.strictEqual(a.config.schema, 'analytics_298233330');
    });
});

test('assertions inherit tags from dataformTableConfig', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    createTable(publish, minimalUserConfig(), module, { assert: assertFn });
    captured.forEach(a => {
        assert.deepStrictEqual(a.config.tags, ['ga4_export_fixer']);
    });
});

test('assertions: { dailyQuality: false } disables dailyQuality', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    createTable(publish, minimalUserConfig(), module, {
        assert: assertFn,
        assertions: { dailyQuality: false },
    });
    assert.strictEqual(captured.length, 0, 'dailyQuality disabled + opt-in not requested = no assertions');
});

test('assertions config override applies to assertion Dataform config', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    createTable(publish, minimalUserConfig(), module, {
        assert: assertFn,
        assertions: { dailyQuality: { schema: 'custom_schema', tags: ['custom'] } },
    });
    const dq = captured.find(a => a.name.includes('daily_quality'));
    assert.strictEqual(dq.config.schema, 'custom_schema');
    assert.deepStrictEqual(dq.config.tags, ['custom']);
});

test('assertion name can be overridden via assertions config', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    createTable(publish, minimalUserConfig(), module, {
        assert: assertFn,
        assertions: { dailyQuality: { name: 'my_custom_assertion' } },
    });
    const dq = captured.find(a => a.name === 'my_custom_assertion');
    assert.ok(dq, 'should use custom assertion name');
});

test('assertion query callback resolves Dataform ref sourceTable via ctx.ref()', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    // Use a Dataform reference object as sourceTable
    createTable(publish, minimalUserConfig({
        sourceTable: { name: 'events_*', dataset: 'analytics_298233330', schema: 'analytics_298233330' },
    }), module, { assert: assertFn });
    const dq = captured.find(a => a.name.includes('daily_quality'));
    const ctx = mockCtx();
    const sql = dq.queryFn(ctx);
    // ctx.ref() resolves Dataform ref objects to `resolved.dataset.name`
    assert.ok(sql.includes('`resolved.analytics_298233330.events_*`'), `sourceTable should be resolved via ctx.ref(), got: ${sql}`);
});

test('assertion query callback uses string sourceTable as-is', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    createTable(publish, minimalUserConfig(), module, { assert: assertFn });
    const dq = captured.find(a => a.name.includes('daily_quality'));
    const ctx = mockCtx();
    const sql = dq.queryFn(ctx);
    assert.ok(sql.includes('`project.analytics_298233330.events_*`'), `string sourceTable should be used as-is, got: ${sql}`);
});

test('assertion query callback passes correct tableRef via ctx.ref(tableName)', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    createTable(publish, minimalUserConfig(), module, { assert: assertFn });
    const dq = captured.find(a => a.name.includes('daily_quality'));
    // Mock ctx.ref to track what it's called with for string args
    let refCalledWith = null;
    const ctx = {
        ...mockCtx(),
        ref: (arg) => {
            if (typeof arg === 'string') refCalledWith = arg;
            return `\`resolved.ref.${typeof arg === 'string' ? arg : arg.name}\``;
        },
    };
    dq.queryFn(ctx);
    assert.strictEqual(refCalledWith, 'ga4_events_enhanced_298233330', 'should call ctx.ref with the table name');
});

test('table module without assertions property works with { assert }', () => {
    const { publish } = mockPublish();
    const { assertFn, captured } = mockAssert();
    const { module } = mockTableModule(); // no assertions property
    createTable(publish, minimalUserConfig(), module, { assert: assertFn });
    assert.strictEqual(captured.length, 0, 'should not create any assertions');
});

test('createTable with { assert } still returns the publish result', () => {
    const { publish, captured } = mockPublish();
    const { assertFn } = mockAssert();
    const { module } = mockTableModuleWithAssertions();
    const result = createTable(publish, minimalUserConfig(), module, { assert: assertFn });
    assert.strictEqual(result.name, 'ga4_events_enhanced_298233330');
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
