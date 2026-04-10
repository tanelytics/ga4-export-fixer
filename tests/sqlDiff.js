/**
 * SQL Diff Tool
 *
 * Generates SQL from two versions of ga4EventsEnhanced and diffs the output.
 * Used to verify that refactoring hasn't changed the generated SQL.
 *
 * Usage:
 *   node tests/sqlDiff.js [old-git-ref] [new-git-ref]
 *
 * Defaults: old = v0.4.6, new = HEAD (working tree)
 */

require('dotenv').config({ path: require('path').join(__dirname, '.env') });

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');

const oldRef = process.argv[2] || 'v0.4.6';
const newRef = process.argv[3] || null; // null = use working tree

const sourceTable = process.env.TEST_SOURCE_TABLE;
if (!sourceTable) {
    console.error('Error: TEST_SOURCE_TABLE must be set in tests/.env');
    process.exit(1);
}

const baseConfig = {
    sourceTable,
    incremental: false,
    test: true,
};

const configurations = [
    { name: 'default', config: { ...baseConfig } },
    {
        name: 'daily-only',
        config: { ...baseConfig, includedExportTypes: { daily: true, fresh: false, intraday: false } },
    },
    {
        name: 'fresh-only',
        config: {
            ...baseConfig,
            includedExportTypes: { daily: false, fresh: true, intraday: false },
            dataIsFinal: { detectionMethod: 'DAY_THRESHOLD', dayThreshold: 1 },
        },
    },
    {
        name: 'custom-timestamp',
        config: { ...baseConfig, customTimestamParam: 'custom_event_timestamp' },
    },
    {
        name: 'helsinki-timezone',
        config: { ...baseConfig, timezone: 'Europe/Helsinki' },
    },
    {
        name: 'zero-buffer-days',
        config: { ...baseConfig, bufferDays: 0 },
    },
    {
        name: 'day-threshold',
        config: { ...baseConfig, dataIsFinal: { detectionMethod: 'DAY_THRESHOLD', dayThreshold: 5 } },
    },
    {
        name: 'session-params',
        config: { ...baseConfig, sessionParams: ['param1', 'param2'] },
    },
    {
        name: 'promoted-event-params',
        config: {
            ...baseConfig,
            eventParamsToColumns: [
                { name: 'page_title', type: 'string' },
                { name: 'page_content_group', type: 'string', columnName: 'content_group' },
                { name: 'page_title_custom' },
            ],
        },
    },
    {
        name: 'excluded-event-params',
        config: { ...baseConfig, excludedEventParams: ['param2', 'param3'] },
    },
    {
        name: 'excluded-events',
        config: { ...baseConfig, excludedEvents: ['event1', 'event2'] },
    },
];

// Generate SQL by running a snippet against a git ref using a temp worktree
const generateSqlFromRef = (ref) => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sql-diff-'));
    const repoRoot = path.resolve(__dirname, '..');

    try {
        execSync(`git worktree add "${tmpDir}" ${ref} --quiet`, { cwd: repoRoot, stdio: 'pipe' });

        // Build a self-contained script that generates all SQL
        const script = `
            const ga4EventsEnhanced = require('./tables/ga4EventsEnhanced');
            const configs = JSON.parse(process.argv[1]);
            const results = {};
            for (const { name, config } of configs) {
                results[name] = ga4EventsEnhanced.generateSql(config);
            }
            process.stdout.write(JSON.stringify(results));
        `;

        const result = execSync(
            `node -e "${script.replace(/"/g, '\\"').replace(/\n/g, ' ')}" "${JSON.stringify(configurations).replace(/"/g, '\\"')}"`,
            { cwd: tmpDir, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }
        );

        return JSON.parse(result);
    } finally {
        execSync(`git worktree remove "${tmpDir}" --force`, { cwd: repoRoot, stdio: 'pipe' });
    }
};

// Generate SQL from working tree
const generateSqlFromWorkingTree = () => {
    const ga4EventsEnhanced = require('../tables/ga4EventsEnhanced');
    const results = {};
    for (const { name, config } of configurations) {
        results[name] = ga4EventsEnhanced.generateSql(config);
    }
    return results;
};

// Normalize SQL for comparison (trim trailing whitespace, normalize newlines)
const normalizeSql = (sql) => sql.replace(/\r\n/g, '\n').replace(/[ \t]+$/gm, '').trim();

const run = () => {
    console.log(`\nComparing SQL output: ${oldRef} vs ${newRef || 'working tree'}\n`);
    console.log(`Source table: ${sourceTable}`);
    console.log(`Configurations: ${configurations.length}\n`);

    console.log(`Generating SQL from ${oldRef}...`);
    const oldSql = generateSqlFromRef(oldRef);

    console.log(`Generating SQL from ${newRef || 'working tree'}...`);
    const newSql = newRef ? generateSqlFromRef(newRef) : generateSqlFromWorkingTree();

    console.log('');
    console.log('═'.repeat(60));
    console.log('SQL DIFF RESULTS');
    console.log('═'.repeat(60));

    let identical = 0;
    let different = 0;

    for (const { name } of configurations) {
        const oldNorm = normalizeSql(oldSql[name]);
        const newNorm = normalizeSql(newSql[name]);

        if (oldNorm === newNorm) {
            console.log(`  ✅ ${name}: identical`);
            identical++;
        } else {
            console.log(`  ❌ ${name}: DIFFERENT`);
            different++;

            // Write diff files for inspection
            const diffDir = path.join(__dirname, 'sql-diff-output');
            fs.mkdirSync(diffDir, { recursive: true });
            fs.writeFileSync(path.join(diffDir, `${name}.old.sql`), oldNorm + '\n');
            fs.writeFileSync(path.join(diffDir, `${name}.new.sql`), newNorm + '\n');

            // Try to show inline diff
            try {
                const diff = execSync(
                    `diff --unified=3 "${path.join(diffDir, `${name}.old.sql`)}" "${path.join(diffDir, `${name}.new.sql`)}"`,
                    { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }
                );
                console.log(diff);
            } catch (e) {
                // diff exits with 1 when files differ
                if (e.stdout) console.log(e.stdout);
            }
        }
    }

    console.log('\n' + '═'.repeat(60));
    console.log(`Total: ${configurations.length} | Identical: ${identical} | Different: ${different}`);
    console.log('═'.repeat(60) + '\n');

    if (different > 0) {
        console.log(`Diff files written to: tests/sql-diff-output/`);
        console.log('Review <name>.old.sql vs <name>.new.sql for each difference.\n');
    }

    process.exit(different > 0 ? 1 : 0);
};

run();
