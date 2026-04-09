const assert = require('assert');
const {
    getColumnDescriptions,
    buildTableDescription,
    composeDescription,
    getLineageText,
    buildConfigNotes,
} = require('../documentation');
const { getTableDescriptionSections } = require('../tables/ga4EventsEnhanced/tableDescription.js');
const columnDescriptions = require('../tables/ga4EventsEnhanced/columns/columnDescriptions.json');
const columnLineage = require('../tables/ga4EventsEnhanced/columns/columnLineage.json');
const columnTypicalUse = require('../tables/ga4EventsEnhanced/columns/columnTypicalUse.json');
const { isGa4ExportColumn } = require('../helpers/index.js');

// Column metadata for tests — mirrors what the table module provides
const columnMetadata = { descriptions: columnDescriptions, lineage: columnLineage, typicalUse: columnTypicalUse };

// Helper: build a full table description the same way the table module does
const getTableDescription = (config) => buildTableDescription(config, getTableDescriptionSections(config));

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
// 1. composeDescription
// ---------------------------------------------------------------------------
console.log('\n1. composeDescription\n');

test('returns only base when other sections are null', () => {
    const result = composeDescription({ base: 'Base text', lineage: null, typicalUse: null, config: null });
    assert.strictEqual(result, 'Base text');
});

test('joins all sections with line breaks and labels', () => {
    const result = composeDescription({
        base: 'Base text',
        lineage: 'Derived -- from X',
        typicalUse: 'Use for Y',
        config: 'Timezone: UTC',
    });
    assert.strictEqual(result, 'Base text\n\nLineage: Derived -- from X\n\nTypical use: Use for Y\n\nConfig: Timezone: UTC');
});

test('omits undefined and empty sections', () => {
    const result = composeDescription({
        base: 'Base text',
        lineage: undefined,
        typicalUse: 'Use for Y',
        config: undefined,
    });
    assert.strictEqual(result, 'Base text\n\nTypical use: Use for Y');
});

test('returns empty string when all sections are falsy', () => {
    const result = composeDescription({ base: null, lineage: null, typicalUse: null, config: null });
    assert.strictEqual(result, '');
});

test('omits sections with empty string values', () => {
    const result = composeDescription({ base: 'Base', lineage: '', typicalUse: '', config: '' });
    assert.strictEqual(result, 'Base');
});

// ---------------------------------------------------------------------------
// 2. getLineageText
// ---------------------------------------------------------------------------
console.log('\n2. getLineageText\n');

test('returns formatted text for ga4_export source', () => {
    const result = getLineageText('event_timestamp', columnLineage);
    assert.strictEqual(result, 'Standard GA4 export field');
});

test('returns formatted text for ga4_export_modified source with note', () => {
    const result = getLineageText('ecommerce', columnLineage);
    assert.ok(result.startsWith('GA4 export field (modified) -- '));
});

test('returns formatted text for derived source with note', () => {
    const result = getLineageText('session_id', columnLineage);
    assert.ok(result.startsWith('Derived -- '));
});

test('returns null for unknown column', () => {
    const result = getLineageText('nonexistent_column_xyz', columnLineage);
    assert.strictEqual(result, null);
});

test('returns label only when entry has no note', () => {
    // event_timestamp and event_name are ga4_export with no note
    const result = getLineageText('event_name', columnLineage);
    assert.strictEqual(result, 'Standard GA4 export field');
});

// ---------------------------------------------------------------------------
// 3. buildConfigNotes
// ---------------------------------------------------------------------------
console.log('\n3. buildConfigNotes\n');

test('returns empty object for null config', () => {
    const result = buildConfigNotes(null);
    assert.deepStrictEqual(result, {});
});

test('returns empty object for empty config', () => {
    const result = buildConfigNotes({});
    assert.deepStrictEqual(result, {});
});

test('includes timezone note for event_datetime', () => {
    const result = buildConfigNotes({ timezone: 'Europe/Helsinki' });
    assert.ok(result.event_datetime.includes('Timezone: Europe/Helsinki'));
});

test('includes custom timestamp notes', () => {
    const result = buildConfigNotes({ customTimestampParam: 'client_ts' });
    assert.ok(result.event_datetime.includes("Custom timestamp parameter: 'client_ts'"));
    assert.ok(result.event_custom_timestamp.includes("Source parameter: 'client_ts'"));
});

test('includes DAY_THRESHOLD detection method', () => {
    const result = buildConfigNotes({ dataIsFinal: { detectionMethod: 'DAY_THRESHOLD', dayThreshold: 3 } });
    assert.ok(result.data_is_final.includes('DAY_THRESHOLD (3 days)'));
});

test('includes EXPORT_TYPE detection method', () => {
    const result = buildConfigNotes({ dataIsFinal: { detectionMethod: 'EXPORT_TYPE' } });
    assert.ok(result.data_is_final.includes('EXPORT_TYPE'));
});

test('includes excluded events', () => {
    const result = buildConfigNotes({ excludedEvents: ['session_start', 'first_visit'] });
    assert.ok(result.event_name.includes('session_start, first_visit'));
});

test('includes excluded event params', () => {
    const result = buildConfigNotes({ excludedEventParams: ['ga_session_id', 'page_location'] });
    assert.ok(result.event_params.includes('ga_session_id, page_location'));
});

test('includes session params', () => {
    const result = buildConfigNotes({ sessionParams: ['user_agent', 'currency'] });
    assert.ok(result.session_params.includes('user_agent, currency'));
});

test('includes export types', () => {
    const result = buildConfigNotes({ includedExportTypes: { daily: true, intraday: true, fresh: false } });
    assert.ok(result.export_type.includes('daily, intraday'));
    assert.ok(!result.export_type.includes('fresh'));
});

test('combines timezone and custom timestamp for event_datetime', () => {
    const result = buildConfigNotes({ timezone: 'US/Eastern', customTimestampParam: 'ts' });
    assert.ok(result.event_datetime.includes('Timezone: US/Eastern'));
    assert.ok(result.event_datetime.includes("Custom timestamp parameter: 'ts'"));
});

// ---------------------------------------------------------------------------
// 4. getColumnDescriptions integration
// ---------------------------------------------------------------------------
console.log('\n4. getColumnDescriptions integration\n');

test('returns descriptions without config', () => {
    const result = getColumnDescriptions(null, columnMetadata);
    assert.ok(typeof result === 'object');
    assert.ok(Object.keys(result).length > 0);
});

test('string columns have multi-section format with lineage', () => {
    const result = getColumnDescriptions(null, columnMetadata);
    // event_timestamp is a ga4_export column with typical use
    assert.ok(result.event_timestamp.includes('Lineage: Standard GA4 export field'));
});

test('string columns include typical use when available', () => {
    const result = getColumnDescriptions(null, columnMetadata);
    assert.ok(result.session_id.includes('Typical use:'));
});

test('struct columns have multi-section description field', () => {
    const result = getColumnDescriptions(null, columnMetadata);
    assert.ok(typeof result.ecommerce === 'object');
    assert.ok(result.ecommerce.description.includes('Lineage: GA4 export field (modified)'));
    // sub-fields should remain simple strings
    assert.ok(typeof result.ecommerce.columns.transaction_id === 'string');
    assert.ok(!result.ecommerce.columns.transaction_id.includes('Lineage:'));
});

test('config notes appear in descriptions', () => {
    const result = getColumnDescriptions({ timezone: 'Europe/Helsinki' }, columnMetadata);
    assert.ok(result.event_datetime.includes('Config: Timezone: Europe/Helsinki'));
});

test('promoted event params get all sections', () => {
    const result = getColumnDescriptions({
        eventParamsToColumns: [{ name: 'page_type', type: 'string' }],
    }, columnMetadata);
    assert.ok(result.page_type.includes("Promoted from event parameter 'page_type' (string)"));
    assert.ok(result.page_type.includes('Lineage: Derived'));
    assert.ok(result.page_type.includes('Typical use:'));
});

test('promoted event params with custom column name', () => {
    const result = getColumnDescriptions({
        eventParamsToColumns: [{ name: 'content_group', type: 'string', columnName: 'page_content_group' }],
    }, columnMetadata);
    assert.ok(result.page_content_group);
    assert.ok(result.page_content_group.includes("Promoted from event parameter 'content_group'"));
});

test('all top-level descriptions are strings (not corrupted)', () => {
    const result = getColumnDescriptions(null, columnMetadata);
    for (const [key, value] of Object.entries(result)) {
        if (typeof value === 'string') {
            assert.ok(value.length > 0, `${key} has empty description`);
        } else if (typeof value === 'object' && value.description) {
            assert.ok(typeof value.description === 'string', `${key}.description is not a string`);
            assert.ok(value.description.length > 0, `${key} has empty description`);
        }
    }
});

test('no description exceeds 1024 characters', () => {
    const result = getColumnDescriptions({
        timezone: 'America/Los_Angeles',
        customTimestampParam: 'custom_event_timestamp',
        excludedEvents: ['session_start', 'first_visit', 'user_engagement'],
        excludedEventParams: ['ga_session_id', 'ga_session_number', 'page_location', 'entrances', 'session_engaged'],
        sessionParams: ['user_agent', 'currency', 'country'],
        dataIsFinal: { detectionMethod: 'DAY_THRESHOLD', dayThreshold: 3 },
        includedExportTypes: { daily: true, intraday: true, fresh: true },
        eventParamsToColumns: [{ name: 'page_type', type: 'string' }],
    }, columnMetadata);
    for (const [key, value] of Object.entries(result)) {
        const desc = typeof value === 'string' ? value : value?.description;
        if (desc) {
            assert.ok(desc.length <= 1024, `${key} description is ${desc.length} chars (max 1024)`);
        }
    }
});

// ---------------------------------------------------------------------------
// 5. getTableDescription
// ---------------------------------------------------------------------------
console.log('\n5. getTableDescription\n');

const minimalConfig = {
    timezone: 'Etc/UTC',
    excludedEvents: ['session_start', 'first_visit'],
    excludedEventParams: [],
    excludedColumns: [],
    defaultExcludedColumns: ['event_dimensions', 'traffic_source', 'session_id'],
    defaultExcludedEvents: [],
    includedExportTypes: { daily: true, intraday: true, fresh: false },
    dataIsFinal: { detectionMethod: 'DAY_THRESHOLD', dayThreshold: 3 },
};

test('getTableDescription returns a string', () => {
    const result = getTableDescription(minimalConfig);
    assert.ok(typeof result === 'string');
    assert.ok(result.length > 0);
});

test('getTableDescription includes key fields section', () => {
    const result = getTableDescription(minimalConfig);
    assert.ok(result.includes('KEY FIELDS:'));
    assert.ok(result.includes('event_date'));
    assert.ok(result.includes('event_name'));
});

test('getTableDescription includes synonyms section', () => {
    const result = getTableDescription(minimalConfig);
    assert.ok(result.includes('SYNONYMS:'));
    assert.ok(result.includes('users'));
    assert.ok(result.includes('sessions'));
});

test('getTableDescription includes filtering guidance section', () => {
    const result = getTableDescription(minimalConfig);
    assert.ok(result.includes('FILTERING AND GROUPING:'));
    assert.ok(result.includes('partition column'));
});

test('getTableDescription includes event vocabulary', () => {
    const result = getTableDescription(minimalConfig);
    assert.ok(result.includes('COMMON EVENT NAMES:'));
    assert.ok(result.includes('page_view'));
    assert.ok(result.includes('purchase'));
});

test('getTableDescription reflects timezone from config', () => {
    const result = getTableDescription({ ...minimalConfig, timezone: 'Europe/Helsinki' });
    assert.ok(result.includes('Timezone: Europe/Helsinki'));
});

test('getTableDescription excludes ecommerce synonyms when ecommerce column is excluded', () => {
    const config = { ...minimalConfig, excludedColumns: ['ecommerce'] };
    const result = getTableDescription(config);
    assert.ok(!result.includes('purchase revenue'));
    assert.ok(!result.includes('ecommerce.purchase_revenue'));
});

test('getTableDescription includes promoted event params in key fields', () => {
    const config = {
        ...minimalConfig,
        eventParamsToColumns: [{ name: 'page_type', type: 'string' }],
    };
    const result = getTableDescription(config);
    assert.ok(result.includes('page_type'));
    assert.ok(result.includes("Promoted event parameter 'page_type'"));
});

test('getTableDescription includes config JSON', () => {
    const result = getTableDescription(minimalConfig);
    assert.ok(result.includes('The last full table refresh was done using this configuration:'));
    assert.ok(result.includes('"timezone"'));
});

test('getTableDescription does not exceed 16384 characters', () => {
    const verboseConfig = {
        ...minimalConfig,
        timezone: 'America/Los_Angeles',
        customTimestampParam: 'custom_event_timestamp',
        excludedEvents: ['session_start', 'first_visit', 'user_engagement'],
        excludedEventParams: ['ga_session_id', 'ga_session_number', 'page_location', 'entrances', 'session_engaged'],
        sessionParams: ['user_agent', 'currency', 'country'],
        eventParamsToColumns: [
            { name: 'page_type', type: 'string' },
            { name: 'content_group', type: 'string', columnName: 'page_content_group' },
            { name: 'logged_in', type: 'int' },
        ],
    };
    const result = getTableDescription(verboseConfig);
    assert.ok(result.length <= 16384, `Table description is ${result.length} chars (max 16384)`);
});

test('getTableDescription excludes events from vocabulary based on excludedEvents', () => {
    const result = getTableDescription(minimalConfig);
    // Extract the event vocabulary section
    const vocabSection = result.split('COMMON EVENT NAMES:\n')[1].split('\n\n')[0];
    // session_start and first_visit are in excludedEvents and should not appear in the vocabulary
    assert.ok(!vocabSection.includes('session_start'), 'session_start should be excluded from vocabulary');
    assert.ok(!vocabSection.includes('first_visit'), 'first_visit should be excluded from vocabulary');
    // page_view should still be present
    assert.ok(vocabSection.includes('page_view'));
});

test('getTableDescription includes package attribution', () => {
    const result = getTableDescription(minimalConfig);
    assert.ok(result.includes('Created by the ga4-export-fixer package'));
    assert.ok(result.includes('github.com/tanelytics/ga4-export-fixer'));
});

// ---------------------------------------------------------------------------
// 6. Lineage cross-checks
// ---------------------------------------------------------------------------
console.log('\n5. Lineage cross-checks\n');

test('ga4_export and ga4_export_modified entries match isGa4ExportColumn', () => {
    for (const [columnName, entry] of Object.entries(columnLineage)) {
        if (entry.source === 'ga4_export' || entry.source === 'ga4_export_modified') {
            // user_traffic_source is renamed from traffic_source — the original name is in the GA4 export list
            if (columnName === 'user_traffic_source') continue;
            assert.ok(
                isGa4ExportColumn(columnName),
                `${columnName} has source '${entry.source}' but isGa4ExportColumn returns false`
            );
        }
    }
});

test('every columnDescriptions key has a lineage entry', () => {
    const missing = [];
    for (const key of Object.keys(columnDescriptions)) {
        if (!columnLineage[key]) {
            missing.push(key);
        }
    }
    assert.deepStrictEqual(missing, [], `Missing lineage entries for: ${missing.join(', ')}`);
});

test('lineage source values are valid enum values', () => {
    const validSources = ['ga4_export', 'ga4_export_modified', 'derived'];
    for (const [key, entry] of Object.entries(columnLineage)) {
        assert.ok(
            validSources.includes(entry.source),
            `${key} has invalid source '${entry.source}'. Valid: ${validSources.join(', ')}`
        );
    }
});

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
console.log(`\n--- Results: ${passed} passed, ${failed} failed ---\n`);
if (failed > 0) {
    process.exit(1);
}
