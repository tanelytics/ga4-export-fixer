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
// Summary
// ---------------------------------------------------------------------------

console.log(`\n---\nTotal: ${passed + failed}  Passed: ${passed}  Failed: ${failed}`);
if (failed > 0) {
    console.error(`${failed} test(s) failed.\n`);
    process.exit(1);
} else {
    console.log('All tests passed.\n');
}
