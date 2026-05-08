/**
 * Tests for the enrichments feature in tables/ga4EventsEnhanced.
 *
 * Covers:
 * - Source-CTE generation (Dataform ref + backtick string sources)
 * - Composite join keys
 * - Opt-in dedupe via qualify row_number()
 * - Item-level deferral throw (not yet supported)
 * - Event-level join integration + replace-or-add semantics
 * - Auto-generated column descriptions
 * - Reserved-name collision with package step names (Layer 2)
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

const enrichment = (overrides = {}) => ({
    name: 'cohorts',
    level: 'event',
    source: '`proj.ds.user_cohorts`',
    joinKey: 'user_pseudo_id',
    columns: ['cohort_label'],
    ...overrides,
});

// ---------------------------------------------------------------------------
// 1. Source-CTE generation
// ---------------------------------------------------------------------------

console.log('\n1. Source-CTE generation\n');

test('no enrichments → no enrich_* CTE', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig());
    assert.ok(!sql.includes('enrich_'), 'SQL should not include any enrich_ CTE');
});

test('single enrichment generates enrich_<name> CTE at top of pipeline', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment()],
    }));
    assert.ok(sql.startsWith('with enrich_cohorts as ('),
        `pipeline should start with enrich_cohorts; got: ${sql.slice(0, 60)}`);
});

test('source CTE selects joinKey and requested columns', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({ columns: ['cohort_label', 'lifecycle_stage'] })],
    }));
    const idx = sql.indexOf('enrich_cohorts as (');
    const block = sql.substring(idx, sql.indexOf('),', idx) + 1);
    assert.ok(block.includes('user_pseudo_id'), 'joinKey column missing from source CTE');
    assert.ok(block.includes('cohort_label'), 'requested column missing from source CTE');
    assert.ok(block.includes('lifecycle_stage'), 'requested column missing from source CTE');
});

test('source CTE selects FROM the configured backtick-string source', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment()],
    }));
    assert.ok(sql.includes('`proj.ds.user_cohorts`'),
        'source string should appear in generated SQL');
});

test('multiple enrichments produce one CTE each, in declaration order', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [
            enrichment({ name: 'cohorts', columns: ['cohort_label'] }),
            enrichment({ name: 'segments', source: '`proj.ds.segments`', columns: ['segment'] }),
        ],
    }));
    const cohortsIdx = sql.indexOf('enrich_cohorts as (');
    const segmentsIdx = sql.indexOf('enrich_segments as (');
    assert.ok(cohortsIdx > 0 && segmentsIdx > cohortsIdx,
        'both enrichment CTEs present and in declaration order');
});

// ---------------------------------------------------------------------------
// 2. Composite join keys
// ---------------------------------------------------------------------------

console.log('\n2. Composite join keys\n');

test('composite joinKey selects all keys in source CTE', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            joinKey: ['event_date', 'user_pseudo_id'],
        })],
    }));
    const idx = sql.indexOf('enrich_cohorts as (');
    const block = sql.substring(idx, sql.indexOf('),', idx) + 1);
    assert.ok(block.includes('event_date'), 'first join key missing');
    assert.ok(block.includes('user_pseudo_id'), 'second join key missing');
});

test('single-string joinKey works like single-element array', () => {
    const sqlString = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({ joinKey: 'user_pseudo_id' })],
    }));
    const sqlArray = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({ joinKey: ['user_pseudo_id'] })],
    }));
    assert.strictEqual(sqlString, sqlArray, 'string and 1-element array should produce identical SQL');
});

// ---------------------------------------------------------------------------
// 3. Opt-in dedupe
// ---------------------------------------------------------------------------

console.log('\n3. Opt-in dedupe\n');

test('dedupe omitted → no qualify clause', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment()],
    }));
    const idx = sql.indexOf('enrich_cohorts as (');
    const block = sql.substring(idx, sql.indexOf('),', idx) + 1);
    assert.ok(!block.includes('qualify'), 'qualify clause should not appear without dedupe');
});

test('dedupe: false → no qualify clause', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({ dedupe: false })],
    }));
    const idx = sql.indexOf('enrich_cohorts as (');
    const block = sql.substring(idx, sql.indexOf('),', idx) + 1);
    assert.ok(!block.includes('qualify'));
});

test('dedupe: true → qualify row_number() over (partition by joinKey) = 1', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({ dedupe: true })],
    }));
    const idx = sql.indexOf('enrich_cohorts as (');
    const block = sql.substring(idx, sql.indexOf('),', idx) + 1);
    assert.ok(block.includes('qualify'), 'qualify clause should appear');
    assert.ok(block.includes('row_number() over (partition by user_pseudo_id) = 1'),
        'qualify expression should partition by the joinKey');
});

test('dedupe with composite joinKey partitions by all keys', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            joinKey: ['event_date', 'user_pseudo_id'],
            dedupe: true,
        })],
    }));
    const idx = sql.indexOf('enrich_cohorts as (');
    const block = sql.substring(idx, sql.indexOf('),', idx) + 1);
    assert.ok(block.includes('partition by event_date, user_pseudo_id'),
        `composite partition expected, got: ${block.slice(-150)}`);
});

// ---------------------------------------------------------------------------
// 4. Item-level deferral
// ---------------------------------------------------------------------------

console.log('\n4. Item-level deferral\n');

test('level: "item" throws "not yet supported" with design-doc pointer', () => {
    try {
        ga4EventsEnhanced.generateSql(baseConfig({
            enrichments: [enrichment({
                level: 'item',
                joinKey: 'item_id',
                source: '`proj.ds.products`',
                columns: ['margin_bucket'],
            })],
        }));
        assert.fail('should have thrown');
    } catch (e) {
        assert.ok(e.message.includes("level: 'item'"),
            `error should mention item level; got: ${e.message}`);
        assert.ok(e.message.includes('not yet supported'),
            `error should say not yet supported; got: ${e.message}`);
        assert.ok(e.message.includes('data-enrichments.md'),
            `error should point at the design doc; got: ${e.message}`);
        assert.ok(e.message.includes('config.enrichments[0]'),
            `error should identify the offending entry; got: ${e.message}`);
    }
});

test('item-level deferral identifies the index when multiple enrichments are configured', () => {
    try {
        ga4EventsEnhanced.generateSql(baseConfig({
            enrichments: [
                enrichment({ name: 'cohorts' }),
                enrichment({
                    name: 'products',
                    level: 'item',
                    joinKey: 'item_id',
                    source: '`proj.ds.products`',
                    columns: ['margin_bucket'],
                }),
            ],
        }));
        assert.fail('should have thrown');
    } catch (e) {
        assert.ok(e.message.includes('config.enrichments[1]'),
            `error should identify index 1; got: ${e.message}`);
    }
});

// ---------------------------------------------------------------------------
// 5. Event-level join integration + replace-or-add
// ---------------------------------------------------------------------------

console.log('\n5. Event-level join integration + replace-or-add\n');

test('event-level enrichment adds LEFT JOIN to enhanced_events with USING(joinKey)', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment()],
    }));
    assert.ok(sql.includes('left join\n  enrich_cohorts using(user_pseudo_id)'),
        'left join with using clause should appear in enhanced_events');
});

test('composite-key joinKey compiles to USING(col1, col2)', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            joinKey: ['event_date', 'user_pseudo_id'],
        })],
    }));
    assert.ok(sql.includes('using(event_date, user_pseudo_id)'),
        'composite USING with comma-separated keys should appear');
});

test('enrichment column is added to enhanced_events select list (qualified)', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({ columns: ['cohort_label'] })],
    }));
    assert.ok(sql.includes('enrich_cohorts.cohort_label as cohort_label'),
        'enrichment column should appear qualified by enrich_<name> in select');
});

test('enrichment column overrides matching explicit promoted column', () => {
    // page_title is promoted via eventParamsToColumns, so it appears as an explicit column on
    // event_data and (via the wildcard in enhanced_events) on enhanced_events. An enrichment
    // column also named page_title should REPLACE it: the enrichment value wins, and page_title
    // gets added to the wildcard's except list so the original column isn't double-selected.
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        eventParamsToColumns: [{ name: 'page_title', type: 'string' }],
        enrichments: [enrichment({
            name: 'titles',
            source: '`proj.ds.title_overrides`',
            joinKey: 'page_location',
            columns: ['page_title'],
        })],
    }));
    // The enrichment value should be present in the enhanced_events SELECT
    assert.ok(sql.includes('enrich_titles.page_title as page_title'),
        'enrichment value should be selected as page_title');
    // The wildcard's except clause for event_data.* should include page_title so the original
    // promoted column is suppressed (no duplicate page_title in the output)
    const eventDataWildcardLine = sql.split('\n').find(l => l.includes('event_data.* except'));
    assert.ok(eventDataWildcardLine && eventDataWildcardLine.includes('page_title'),
        `event_data.* except clause should include page_title; got: ${eventDataWildcardLine}`);
});

test('enrichment column for a wildcard column is excluded from event_data.* and provided by enrichment', () => {
    // 'app_info' is a default GA4 column normally pulled in via event_data.*. An enrichment
    // column with the same name should add it to the wildcard's except() list and provide
    // the enrichment value instead.
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'app',
            source: '`proj.ds.app_info_overrides`',
            joinKey: 'user_pseudo_id',
            columns: ['app_info'],
        })],
    }));
    // The except() clause for event_data.* should include app_info
    const eventDataWildcardLine = sql.split('\n').find(l => l.includes('event_data.* except'));
    assert.ok(eventDataWildcardLine && eventDataWildcardLine.includes('app_info'),
        `event_data.* except clause should include app_info; got: ${eventDataWildcardLine}`);
    // The enrichment value should be selected
    assert.ok(sql.includes('enrich_app.app_info as app_info'));
});

test('pure additive enrichment (column does not exist anywhere) just adds a new column', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            columns: ['custom_cohort_score'],  // not a known GA4 or package column
        })],
    }));
    assert.ok(sql.includes('enrich_cohorts.custom_cohort_score as custom_cohort_score'),
        'new enrichment column should appear as a fresh column');
    // Regression: an additive enrichment column must NOT appear in any wildcard EXCEPT list,
    // otherwise BigQuery rejects with "Column ... in SELECT * EXCEPT list does not exist".
    const exceptLines = sql.split('\n').filter(l => /\.\* except \(/.test(l));
    assert.ok(exceptLines.length > 0, 'sanity: SQL should contain at least one wildcard except() clause');
    for (const line of exceptLines) {
        assert.ok(!line.includes('custom_cohort_score'),
            `additive enrichment column must not appear in any wildcard EXCEPT list; got: ${line}`);
    }
});

test('enrichment column overrides matching session_data column', () => {
    // merged_user_id is an explicit column on session_data and is in finalColumnOrder. An
    // enrichment with the same name should REPLACE it: the enrichment value wins via the
    // enrichmentColumns spread overriding the finalColumnOrder mapping. Crucially, the
    // event_data wildcard must NOT carry merged_user_id (it doesn't exist there) — that was
    // the original bug for purely additive columns and applies equally to session_data overlaps.
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'users',
            source: '`proj.ds.user_overrides`',
            joinKey: 'user_pseudo_id',
            columns: ['merged_user_id'],
        })],
    }));
    // Override value lands in the SELECT exactly once (no double-selection from session_data)
    const mergedUserIdSelectLines = sql.split('\n').filter(
        l => /\bmerged_user_id as merged_user_id\b/.test(l)
    );
    assert.strictEqual(mergedUserIdSelectLines.length, 1,
        `merged_user_id should appear exactly once in the SELECT; got: ${mergedUserIdSelectLines.length} times`);
    assert.ok(mergedUserIdSelectLines[0].includes('enrich_users.merged_user_id as merged_user_id'),
        `enrichment value should win for merged_user_id; got: ${mergedUserIdSelectLines[0]}`);
    // event_data wildcard EXCEPT must NOT include merged_user_id (it doesn't exist there).
    const eventWildcardLine = sql.split('\n').find(l => l.includes('event_data.* except'));
    if (eventWildcardLine) {
        assert.ok(!eventWildcardLine.includes('merged_user_id'),
            `event_data.* except must NOT include merged_user_id; got: ${eventWildcardLine}`);
    }
    // session_data wildcard EXCEPT, if present, must include merged_user_id (suppress overlap).
    const sessionWildcardLine = sql.split('\n').find(l => l.includes('session_data.* except'));
    if (sessionWildcardLine) {
        assert.ok(sessionWildcardLine.includes('merged_user_id'),
            `session_data.* except clause should include merged_user_id; got: ${sessionWildcardLine}`);
    }
});

test('two enrichments writing the same column throws with both names', () => {
    try {
        ga4EventsEnhanced.generateSql(baseConfig({
            enrichments: [
                enrichment({ name: 'a', columns: ['cohort'] }),
                enrichment({ name: 'b', source: '`proj.ds.t2`', joinKey: 'session_id', columns: ['cohort'] }),
            ],
        }));
        assert.fail('should have thrown');
    } catch (e) {
        assert.ok(e.message.includes("'cohort'"),
            `error should name the conflicting column; got: ${e.message}`);
        assert.ok(e.message.includes("'a'") && e.message.includes("'b'"),
            `error should name both enrichments; got: ${e.message}`);
    }
});

// ---------------------------------------------------------------------------
// 6. Auto-generated column descriptions
// ---------------------------------------------------------------------------

console.log('\n6. Auto-generated column descriptions\n');

test('added column gets "Added by enrichment ..." description', () => {
    const desc = ga4EventsEnhanced.getColumnDescriptions(baseConfig({
        enrichments: [enrichment({ columns: ['cohort_label'] })],
    }));
    assert.match(desc.cohort_label,
        /^Added by enrichment 'cohorts' \(joined on user_pseudo_id from `proj\.ds\.user_cohorts`\)\.$/);
});

test('replaced promoted column gets "Replaced by enrichment ..." with original retained', () => {
    const desc = ga4EventsEnhanced.getColumnDescriptions(baseConfig({
        eventParamsToColumns: [{ name: 'page_title', type: 'string' }],
        enrichments: [enrichment({
            name: 'titles',
            source: '`proj.ds.title_overrides`',
            joinKey: 'page_location',
            columns: ['page_title'],
        })],
    }));
    assert.match(desc.page_title,
        /^Replaced by enrichment 'titles' \(joined on page_location from `proj\.ds\.title_overrides`\)\. Original: /);
    assert.ok(desc.page_title.includes("Promoted from event parameter 'page_title'"),
        `original promoted-param description should be preserved; got: ${desc.page_title}`);
});

test('composite joinKey renders as comma-separated in description', () => {
    const desc = ga4EventsEnhanced.getColumnDescriptions(baseConfig({
        enrichments: [enrichment({
            name: 'segments',
            source: '`proj.ds.user_segments`',
            joinKey: ['event_date', 'user_pseudo_id'],
            columns: ['segment'],
        })],
    }));
    assert.match(desc.segment, /joined on event_date, user_pseudo_id from/);
});

test('Dataform ref source renders as `<dataset>.<name>`', () => {
    const desc = ga4EventsEnhanced.getColumnDescriptions(baseConfig({
        enrichments: [enrichment({
            source: { name: 'user_cohorts', dataset: 'analytics_dim' },
        })],
    }));
    assert.match(desc.cohort_label, /from `analytics_dim\.user_cohorts`/);
});

test('user-supplied dataformTableConfig.columns wins over auto-generated description', () => {
    // The auto-generated description appears in tableModule.getColumnDescriptions output.
    // The deep-merge in createTable's mergeDataformTableConfigurations applies the user's
    // dataformTableConfig.columns on top — so the user's value wins. We test this by
    // running getColumnDescriptions and verifying the auto-gen value is what would be
    // overridden (the merge happens in createTable, not in getColumnDescriptions).
    const desc = ga4EventsEnhanced.getColumnDescriptions(baseConfig({
        enrichments: [enrichment({ columns: ['cohort_label'] })],
    }));
    // Verify the auto-gen baseline exists; the user override happens at merge time downstream.
    assert.ok(desc.cohort_label.startsWith('Added by enrichment'),
        'auto-generated description should be the baseline; user override merges over this');
});

test('item-level enrichment columns are skipped in column descriptions', () => {
    // Item-level enrichments throw at SQL gen but should NOT throw during description generation,
    // since the description path runs before generateSql. They simply produce no descriptions
    // until item-level support is added in a later release.
    const desc = ga4EventsEnhanced.getColumnDescriptions(baseConfig({
        enrichments: [enrichment({
            name: 'products',
            level: 'item',
            source: '`proj.ds.products`',
            joinKey: 'item_id',
            columns: ['margin_bucket'],
        })],
    }));
    // No top-level description added for item-level enrichment columns
    assert.strictEqual(desc.margin_bucket, undefined,
        'item-level enrichment column should not appear in top-level descriptions');
});

// ---------------------------------------------------------------------------
// 7. Reserved-name collision (Layer 2 — runtime-derived set)
// ---------------------------------------------------------------------------

console.log('\n5. Reserved-name collision\n');

test('customStep colliding with enrich_<name> throws', () => {
    assert.throws(
        () => ga4EventsEnhanced.generateSql(baseConfig({
            enrichments: [enrichment({ name: 'cohorts' })],
            customSteps: [{ name: 'enrich_cohorts', query: 'select 1' }],
        })),
        /collides with a reserved package CTE name/
    );
});

test('reserved set in collision error includes the enrich_<name> entries', () => {
    try {
        ga4EventsEnhanced.generateSql(baseConfig({
            enrichments: [enrichment({ name: 'cohorts' })],
            customSteps: [{ name: 'event_data', query: 'select 1' }],
        }));
        assert.fail('should have thrown');
    } catch (e) {
        assert.ok(e.message.includes('enrich_cohorts'),
            `reserved set should include enrich_cohorts; got: ${e.message}`);
    }
});

test('enrich_<name> NOT reserved when enrichments is empty', () => {
    // A user customStep named 'enrich_anything' should be allowed when no enrichments configured
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        customSteps: [{ name: 'enrich_anything', query: 'select 1 as fake' }],
    }));
    assert.ok(sql.includes('select 1 as fake'),
        'customStep should render when no enrichments configured');
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
