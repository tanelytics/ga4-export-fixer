/**
 * Tests for the enrichments feature in tables/ga4EventsEnhanced.
 *
 * Covers:
 * - Source-CTE generation (Dataform ref + backtick string sources)
 * - Composite join keys
 * - Opt-in dedupe via qualify row_number()
 * - Event-level join integration + replace-or-add semantics
 * - Item-level routing into items_rebuilt + coalesce-on-overlap
 * - Auto-generated column descriptions
 * - Reserved-name collision with package step names
 *
 * Config-shape validation lives in tests/inputValidation.test.js.
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
// 4. Item-level enrichments — end-to-end
// ---------------------------------------------------------------------------

console.log('\n4. Item-level enrichments — end-to-end\n');

test('item-level enrichment activates the items scaffold (no itemListAttribution needed)', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'products',
            level: 'item',
            source: '`proj.ds.products`',
            joinKey: 'item_id',
            columns: ['margin_bucket'],
        })],
    }));
    assert.ok(sql.includes('items_unnested as ('),
        'items_unnested CTE should be emitted when an item-level enrichment is configured');
    assert.ok(sql.includes('items_rebuilt as ('),
        'items_rebuilt CTE should be emitted when an item-level enrichment is configured');
});

test('item-level-enrichment-only config wires up _item_row_id and ecommerce filter', () => {
    // Regression: when only item enrichments activate the scaffold (no itemListAttribution),
    // event_data must still emit _item_row_id, and items_unnested must filter on the real
    // ecommerce event-name list — not `event_name in (null)`.
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'products',
            level: 'item',
            source: '`proj.ds.products`',
            joinKey: 'item_id',
            columns: ['margin_bucket'],
        })],
    }));
    // _item_row_id is generated on event_data via helpers.itemRowId (farm_fingerprint(...) as _item_row_id)
    assert.ok(/farm_fingerprint\([\s\S]*?\)[\s\S]*?as _item_row_id/.test(sql),
        'event_data should generate _item_row_id via farm_fingerprint when only item enrichments are active');
    // items_unnested WHERE filters on the real ecommerce event list, not on null
    assert.ok(sql.includes("event_name in ('view_item_list',"),
        'items_unnested.where should filter on the ecommerce event list when only item enrichments are active');
    assert.ok(!sql.includes('event_name in (null)'),
        'items_unnested.where must NOT be event_name in (null)');
});

test('items_unnested omits the LAST_VALUE window when only item enrichments are active', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'products',
            level: 'item',
            source: '`proj.ds.products`',
            joinKey: 'item_id',
            columns: ['margin_bucket'],
        })],
    }));
    assert.ok(!sql.includes('last_value('),
        'LAST_VALUE attribution window should NOT appear when itemListAttribution is not configured');
    assert.ok(!sql.includes('_item_list_attr'),
        '_item_list_attr column should NOT be selected in items_unnested when only item enrichments are active');
});

test('items_unnested keeps LAST_VALUE window when itemListAttribution is configured alongside item enrichments', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        itemListAttribution: { lookbackType: 'SESSION' },
        enrichments: [enrichment({
            name: 'products',
            level: 'item',
            source: '`proj.ds.products`',
            joinKey: 'item_id',
            columns: ['margin_bucket'],
        })],
    }));
    assert.ok(sql.includes('last_value('),
        'LAST_VALUE attribution window should appear when itemListAttribution is configured');
    assert.ok(sql.includes('_item_list_attr'),
        '_item_list_attr column should be selected when attribution is active');
});

test('item-level enrichment LEFT JOIN added to items_rebuilt with USING(joinKey)', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'products',
            level: 'item',
            source: '`proj.ds.products`',
            joinKey: 'item_id',
            columns: ['margin_bucket'],
        })],
    }));
    // Extract items_rebuilt block; verify it has the LEFT JOIN
    const itemsRebuiltBlock = sql.match(/items_rebuilt as \([\s\S]*?\)\s*(,|$)/)[0];
    assert.ok(itemsRebuiltBlock.includes('left join'),
        `items_rebuilt should have a LEFT JOIN for the item-level enrichment; got: ${itemsRebuiltBlock}`);
    assert.ok(itemsRebuiltBlock.includes('enrich_products'),
        'LEFT JOIN target should be enrich_products');
    assert.ok(itemsRebuiltBlock.includes('using(item_id)'),
        'JOIN should use USING(item_id)');
});

test('additive item-level enrichment column appears as an additional struct field', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'products',
            level: 'item',
            source: '`proj.ds.products`',
            joinKey: 'item_id',
            columns: ['margin_bucket'],
        })],
    }));
    assert.ok(sql.includes('enrich_products.margin_bucket as margin_bucket'),
        'additive item-level enrichment column should appear in the items struct as enrich_X.col as col');
});

test('item-level enrichment overlapping a standard field emits coalesce against item-struct field', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'category_fixes',
            level: 'item',
            source: '`proj.ds.cat`',
            joinKey: 'item_id',
            columns: ['item_category'],
        })],
    }));
    // The struct should have `coalesce(enrich_category_fixes.item_category, item_category) as item_category`.
    // item_category is a top-level column on items_unnested, so the original expression in
    // preItemExpressions is bare `item_category` (no `item.` prefix).
    assert.ok(sql.includes('coalesce(enrich_category_fixes.item_category, item_category) as item_category'),
        'overlapping item-level column should emit coalesce(enrich.col, original) as col in the struct');
});

test('item-level enrichment with composite joinKey carries both keys', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'date_products',
            level: 'item',
            source: '`proj.ds.dp`',
            joinKey: ['event_date', 'item_id'],
            columns: ['daily_price_bucket'],
        })],
    }));
    const itemsRebuiltBlock = sql.match(/items_rebuilt as \([\s\S]*?\)\s*(,|$)/)[0];
    assert.ok(itemsRebuiltBlock.includes('using(event_date, item_id)'),
        'composite joinKey should compile to USING(event_date, item_id)');
});

test('item-level enrichment with event_data joinKey extends items_unnested.select.columns', () => {
    // user_pseudo_id is on event_data but not a standard items-struct field; the package
    // must add it as a top-level column on items_unnested so the JOIN can USING() it.
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'user_products',
            level: 'item',
            source: '`proj.ds.up`',
            joinKey: 'user_pseudo_id',
            columns: ['margin_bucket'],
        })],
    }));
    // Extract items_unnested block; verify it selects user_pseudo_id
    const itemsUnnestedBlock = sql.match(/items_unnested as \([\s\S]*?\)\s*,/)[0];
    assert.ok(/^\s+user_pseudo_id,?\s*$/m.test(itemsUnnestedBlock),
        `items_unnested should select user_pseudo_id as a top-level column; got: ${itemsUnnestedBlock}`);
    // And items_rebuilt joins on it
    const itemsRebuiltBlock = sql.match(/items_rebuilt as \([\s\S]*?\)\s*(,|$)/)[0];
    assert.ok(itemsRebuiltBlock.includes('using(user_pseudo_id)'),
        'items_rebuilt JOIN should use USING(user_pseudo_id)');
});

test('item-level joinKey that is neither item-struct field nor event_data column throws', () => {
    assert.throws(
        () => ga4EventsEnhanced.generateSql(baseConfig({
            enrichments: [enrichment({
                name: 'bad',
                level: 'item',
                source: '`proj.ds.bad`',
                joinKey: 'totally_made_up_column',
                columns: ['x'],
            })],
        })),
        /uses item-level joinKey 'totally_made_up_column'.*neither a field on the GA4 items struct.*nor a column on event_data/s
    );
});

test('item-level enrichment columns do NOT propagate to enhanced_events.select.columns', () => {
    // Item-level columns live inside items[], not at the event grain. The outer SELECT
    // must not emit `margin_bucket` as a top-level column on enhanced_events.
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'products',
            level: 'item',
            source: '`proj.ds.products`',
            joinKey: 'item_id',
            columns: ['margin_bucket'],
        })],
    }));
    // The only `as margin_bucket` should be inside the items_rebuilt struct construction;
    // the outer SELECT must not have a top-level margin_bucket column.
    // Extract the outer SELECT (after the last CTE close, before the WHERE).
    const lines = sql.split('\n');
    const outerSelectStart = lines.findIndex(l => /^\)\s*$/.test(l));
    const outerSelectEnd = lines.findIndex((l, i) => i > outerSelectStart && /^from$/.test(l));
    const outerSelectBlock = lines.slice(outerSelectStart, outerSelectEnd).join('\n');
    assert.ok(!outerSelectBlock.includes('as margin_bucket'),
        `enhanced_events outer SELECT must NOT include margin_bucket as a top-level column; got: ${outerSelectBlock}`);
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

test('enrichment column overlapping a promoted column emits coalesce against the original', () => {
    // page_title is promoted via eventParamsToColumns, so it lands as an event_data column.
    // An enrichment with the same name overlaps it; the enhanced_events SELECT should emit
    // coalesce(enrich_titles.page_title, event_data.page_title) so a missed JOIN falls back
    // to the promoted value rather than emitting NULL.
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        eventParamsToColumns: [{ name: 'page_title', type: 'string' }],
        enrichments: [enrichment({
            name: 'titles',
            source: '`proj.ds.title_overrides`',
            joinKey: 'page_location',
            columns: ['page_title'],
        })],
    }));
    // The coalesce expression should be present in the enhanced_events SELECT
    assert.ok(sql.includes('coalesce(enrich_titles.page_title, event_data.page_title) as page_title'),
        'overlapping enrichment column should emit coalesce(enrich.col, original) as col');
    // The event_data pass-through must NOT emit page_title (it's covered by the coalesce)
    assert.ok(!sql.includes('event_data.page_title as page_title'),
        'event_data pass-through must not emit page_title (would double-select)');
});

test('enrichment column for a default GA4 column emits coalesce against the event_data pass-through', () => {
    // 'app_info' is a default GA4 column emitted as a pass-through by event_data. An enrichment
    // with the same name overlaps it; the SELECT should emit
    // coalesce(enrich_app.app_info, event_data.app_info) so missed JOINs keep the original value.
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'app',
            source: '`proj.ds.app_info_overrides`',
            joinKey: 'user_pseudo_id',
            columns: ['app_info'],
        })],
    }));
    assert.ok(sql.includes('coalesce(enrich_app.app_info, event_data.app_info) as app_info'),
        'overlapping enrichment column should emit coalesce(enrich.col, event_data.col) as col');
    // The event_data pass-through must NOT emit app_info (it's covered by the coalesce)
    assert.ok(!sql.includes('event_data.app_info as app_info'),
        'event_data pass-through must not emit app_info (would double-select)');
});

test('pure additive enrichment (column does not exist anywhere) just adds a new column', () => {
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            columns: ['custom_cohort_score'],  // not a known GA4 or package column
        })],
    }));
    assert.ok(sql.includes('enrich_cohorts.custom_cohort_score as custom_cohort_score'),
        'new enrichment column should appear as a fresh column');
    // Regression: an additive enrichment column must NEVER appear in any wildcard EXCEPT list,
    // otherwise BigQuery rejects with "Column ... in SELECT * EXCEPT list does not exist".
    // (After enhanced-events-explicit-columns shipped there are no wildcards in enhanced_events
    // at all; this loop is defensive against any future code paths reintroducing them.)
    const exceptLines = sql.split('\n').filter(l => /\.\* except \(/.test(l));
    for (const line of exceptLines) {
        assert.ok(!line.includes('custom_cohort_score'),
            `additive enrichment column must not appear in any wildcard EXCEPT list; got: ${line}`);
    }
    // Purely additive columns are NOT wrapped in coalesce (no original to fall back to)
    assert.ok(!sql.includes('coalesce(enrich_cohorts.custom_cohort_score'),
        'purely additive enrichment column must not be wrapped in coalesce');
});

test('additive and overlapping enrichment columns in the same enrichment are handled independently', () => {
    // Single enrichment with two columns: one matches a GA4 pass-through (app_info), one is
    // purely additive (custom_score). The overlap should coalesce; the additive should not.
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'mixed',
            source: '`proj.ds.mixed`',
            joinKey: 'user_pseudo_id',
            columns: ['app_info', 'custom_score'],
        })],
    }));
    // Overlapping column: coalesce
    assert.ok(sql.includes('coalesce(enrich_mixed.app_info, event_data.app_info) as app_info'),
        'overlapping column app_info should emit coalesce against event_data.app_info');
    // Additive column: no coalesce, plain reference
    assert.ok(sql.includes('enrich_mixed.custom_score as custom_score'),
        'additive column custom_score should emit as plain enrich_mixed.custom_score');
    assert.ok(!sql.includes('coalesce(enrich_mixed.custom_score'),
        'additive column custom_score must not be wrapped in coalesce');
});

test('enrichment column overlapping a session_data column emits coalesce against session_data', () => {
    // merged_user_id is an explicit column on session_data and is in finalColumnOrder
    // (mapped to session_data.merged_user_id). An enrichment with the same name should emit
    // coalesce(enrich_users.merged_user_id, session_data.merged_user_id) so a missed JOIN
    // falls back to the package-computed merged_user_id.
    const sql = ga4EventsEnhanced.generateSql(baseConfig({
        enrichments: [enrichment({
            name: 'users',
            source: '`proj.ds.user_overrides`',
            joinKey: 'user_pseudo_id',
            columns: ['merged_user_id'],
        })],
    }));
    assert.ok(sql.includes('coalesce(enrich_users.merged_user_id, session_data.merged_user_id) as merged_user_id'),
        'overlapping enrichment column should emit coalesce against session_data.merged_user_id');
    // event_data must NOT emit merged_user_id (it doesn't exist there)
    assert.ok(!sql.includes('event_data.merged_user_id'),
        'event_data must NOT reference merged_user_id (column does not exist on event_data)');
    // session_data must NOT appear as a bare pass-through (it's covered by the coalesce)
    assert.ok(!sql.includes('session_data.merged_user_id as merged_user_id'),
        'session_data pass-through must not emit merged_user_id (would double-select)');
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

test('overlapping promoted column gets "Coalesced by enrichment ..." with original retained', () => {
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
        /^Coalesced by enrichment 'titles' \(joined on page_location from `proj\.ds\.title_overrides`; falls back to original on missed JOIN\)\. Original: /);
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
    // Auto-descriptions for item-level enrichment columns are not generated — BigQuery
    // doesn't expose per-field descriptions for STRUCT-array fields cleanly through
    // Dataform's column-description mechanism. Item-level columns produce no top-level
    // descriptions; the column-description path skips them.
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
// 7. Reserved-name collision (runtime-derived set)
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
