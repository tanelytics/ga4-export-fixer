/**
 * Dataform Integration Test Suite
 *
 * End-to-end validation of ga4-export-fixer in a real Dataform repository.
 * Phases: Setup → Compilation → Incremental Run → Full Refresh → Delete + Recovery → Cleanup
 *
 * Usage:
 *   npm run test:integration
 *   INTEGRATION_TEST_VERSION=0.4.5-dev.0 npm run test:integration
 */

const config = require('./config');
const dataform = require('./dataformClient');
const bq = require('./bigqueryValidator');

// ─── Helpers ─────────────────────────────────────────────────────────────────

const results = [];
let stepCounter = 0;

const pass = (phase, detail) => {
    stepCounter++;
    results.push({ phase, step: stepCounter, passed: true, detail });
    console.log(`  ✅ ${detail}`);
};

const fail = (phase, detail) => {
    stepCounter++;
    results.push({ phase, step: stepCounter, passed: false, detail });
    console.log(`  ❌ ${detail}`);
};

const formatElapsed = (ms) => {
    if (ms < 60000) return `${Math.round(ms / 1000)}s`;
    const m = Math.floor(ms / 60000);
    const s = Math.round((ms % 60000) / 1000);
    return `${m}m ${s}s`;
};

const printBanner = () => {
    console.log('\n');
    console.log('╔═══════════════════════════════════════════════════════════════════════════════╗');
    console.log('║               GA4 EXPORT FIXER - INTEGRATION TESTS                            ║');
    console.log('╚═══════════════════════════════════════════════════════════════════════════════╝');
    console.log(`\nPackage version: ${config.packageVersion}`);
    console.log(`Repository: ${config.dataformRepository}`);
    console.log(`Project: ${config.projectId}`);
    console.log('');
};

const printSummary = () => {
    const passed = results.filter(r => r.passed).length;
    const failed = results.filter(r => !r.passed).length;

    console.log('\n');
    console.log('═══════════════════════════════════════════════════════════════════════════════');
    console.log('INTEGRATION TEST SUMMARY');
    console.log('═══════════════════════════════════════════════════════════════════════════════');
    console.log(`Total: ${results.length} | Passed: ${passed} | Failed: ${failed}`);

    if (failed > 0) {
        console.log('\nFailed steps:');
        results.filter(r => !r.passed).forEach(r => {
            console.log(`  ❌ [Phase ${r.phase}] ${r.detail}`);
        });
        console.log('');
    } else {
        console.log('🎉 All integration tests passed!\n');
    }

    return failed;
};

// ─── Main Test Flow ──────────────────────────────────────────────────────────

const run = async () => {
    printBanner();

    const dfClient = dataform.createClient();
    const bigquery = bq.createClient(config.projectId);
    const workspaceId = `integration-test-${Date.now()}`;

    let workspacePath = null;
    let compilationResultName = null;
    let tables = [];
    let phaseFailed = false;

    try {
        // ─── Phase 0: Setup ──────────────────────────────────────────────
        console.log('Phase 0: Setup');

        try {
            workspacePath = await dataform.createTestWorkspace(
                dfClient, config.dataformRepository, workspaceId
            );
            pass(0, `Workspace created: ${workspaceId}`);
        } catch (err) {
            fail(0, `Workspace creation failed: ${err.message}`);
            return;
        }

        try {
            await dataform.updatePackageVersion(
                dfClient, workspacePath, config.packageName, config.packageVersion
            );
            pass(0, `Package version updated to ${config.packageVersion}`);
        } catch (err) {
            fail(0, `Package version update failed: ${err.message}`);
            return;
        }

        // ─── Phase 1: Compilation ────────────────────────────────────────
        console.log('\nPhase 1: Compilation');

        try {
            compilationResultName = await dataform.compileWorkspace(
                dfClient, config.dataformRepository, workspacePath
            );
            pass(1, 'Compilation succeeded (0 errors)');
        } catch (err) {
            fail(1, `Compilation failed: ${err.message}`);
            phaseFailed = true;
        }

        if (!phaseFailed && compilationResultName) {
            try {
                tables = await dataform.discoverTaggedActions(
                    dfClient, compilationResultName, config.tableTag
                );
                if (tables.length > 0) {
                    pass(1, `Found ${tables.length} table(s) with '${config.tableTag}' tag: ${tables.map(t => t.name).join(', ')}`);
                } else {
                    fail(1, `No tables found with '${config.tableTag}' tag`);
                    phaseFailed = true;
                }
            } catch (err) {
                fail(1, `Action discovery failed: ${err.message}`);
                phaseFailed = true;
            }
        }

        if (phaseFailed) {
            console.log('\n  ⚠️  Compilation failed — skipping run phases');
            return;
        }

        // ─── Phase 2: Incremental Run ────────────────────────────────────
        console.log('\nPhase 2: Incremental Run');

        let incrementalRan = false;
        let succeededTables = [];
        try {
            const result = await dataform.runWorkflowInvocation(
                dfClient, config.dataformRepository, compilationResultName,
                { tag: config.tableTag, fullRefresh: false, timeoutMs: config.workflowTimeoutMs }
            );

            incrementalRan = true;

            // Check individual action results
            const actions = await dataform.getActionResults(dfClient, result.name);
            const succeeded = actions.filter(a => a.state === 'SUCCEEDED');
            const failed_ = actions.filter(a => a.state === 'FAILED');
            const skipped = actions.filter(a => a.state === 'SKIPPED' || a.state === 'CANCELLED');

            if (failed_.length === 0) {
                pass(2, `Workflow SUCCEEDED: ${succeeded.length} action(s) (${formatElapsed(result.elapsedMs)})`);
            } else {
                pass(2, `Workflow completed: ${succeeded.length} succeeded, ${failed_.length} failed, ${skipped.length} skipped (${formatElapsed(result.elapsedMs)})`);
                failed_.forEach(a => fail(2, `Action ${a.target} FAILED: ${a.failureReason}`));
            }

            // Track which of our tagged tables succeeded for post-run validation
            succeededTables = tables.filter(t =>
                succeeded.some(a => a.target.endsWith(`.${t.name}`))
            );
        } catch (err) {
            fail(2, `Incremental run error: ${err.message}`);
        }

        // Post-run validation for tables whose actions succeeded
        if (incrementalRan && succeededTables.length > 0) {
            for (const table of succeededTables) {
                const expectedTypes = ['daily', 'intraday'];
                const [metadataResult, freshnessResult, exportResult] = await Promise.allSettled([
                    bq.snapshotTableMetadata(bigquery, config.projectId, table.dataset, table.name),
                    bq.validateDataFreshness(bigquery, config.projectId, table.dataset, table.name, config.maxDataAgeMinutes),
                    bq.validateExportTypes(bigquery, config.projectId, table.dataset, table.name, expectedTypes),
                ]);

                if (metadataResult.status === 'fulfilled') {
                    const metadata = metadataResult.value;
                    const totalRows = metadata.reduce((sum, p) => sum + p.totalRows, 0);
                    pass(2, `${table.name}: ${totalRows.toLocaleString()} rows, ${metadata.length} partitions`);
                } else {
                    fail(2, `${table.name} metadata query failed: ${metadataResult.reason.message}`);
                }

                if (freshnessResult.status === 'fulfilled') {
                    const freshness = freshnessResult.value;
                    if (freshness.fresh) {
                        pass(2, `${table.name}: Latest insert ${freshness.ageMinutes}m ago`);
                    } else {
                        fail(2, `${table.name}: Data not fresh (${freshness.ageMinutes}m old, max ${config.maxDataAgeMinutes}m)`);
                    }
                } else {
                    fail(2, `${table.name} freshness check failed: ${freshnessResult.reason.message}`);
                }

                if (exportResult.status === 'fulfilled') {
                    const result = exportResult.value;
                    const typesSummary = Object.entries(result.exportTypes)
                        .map(([type, count]) => `${type} (${count.toLocaleString()})`)
                        .join(', ');
                    if (result.unexpected.length === 0) {
                        pass(2, `${table.name}: Export types: ${typesSummary}`);
                    } else {
                        fail(2, `${table.name}: Unexpected export types: ${result.unexpected.join(', ')}`);
                    }
                } else {
                    fail(2, `${table.name} export type check failed: ${exportResult.reason.message}`);
                }
            }
        }

        // ─── Phase 3: Full Refresh ───────────────────────────────────────
        console.log('\nPhase 3: Full Refresh');

        let fullRefreshRan = false;
        let fullRefreshSucceededTables = [];
        try {
            const result = await dataform.runWorkflowInvocation(
                dfClient, config.dataformRepository, compilationResultName,
                { tag: config.tableTag, fullRefresh: true, timeoutMs: config.workflowTimeoutMs }
            );

            fullRefreshRan = true;

            const actions = await dataform.getActionResults(dfClient, result.name);
            const succeeded = actions.filter(a => a.state === 'SUCCEEDED');
            const failed_ = actions.filter(a => a.state === 'FAILED');
            const skipped = actions.filter(a => a.state === 'SKIPPED' || a.state === 'CANCELLED');

            if (failed_.length === 0) {
                pass(3, `Workflow SUCCEEDED: ${succeeded.length} action(s) (${formatElapsed(result.elapsedMs)})`);
            } else {
                pass(3, `Workflow completed: ${succeeded.length} succeeded, ${failed_.length} failed, ${skipped.length} skipped (${formatElapsed(result.elapsedMs)})`);
                failed_.forEach(a => fail(3, `Action ${a.target} FAILED: ${a.failureReason}`));
            }

            fullRefreshSucceededTables = tables.filter(t =>
                succeeded.some(a => a.target.endsWith(`.${t.name}`))
            );
        } catch (err) {
            fail(3, `Full refresh error: ${err.message}`);
        }

        if (fullRefreshRan && fullRefreshSucceededTables.length > 0) {
            const metadataResults = await Promise.allSettled(
                fullRefreshSucceededTables.map(table =>
                    bq.snapshotTableMetadata(bigquery, config.projectId, table.dataset, table.name)
                )
            );
            fullRefreshSucceededTables.forEach((table, i) => {
                const r = metadataResults[i];
                if (r.status === 'fulfilled') {
                    const totalRows = r.value.reduce((sum, p) => sum + p.totalRows, 0);
                    pass(3, `${table.name}: ${totalRows.toLocaleString()} rows, ${r.value.length} partitions`);
                } else {
                    fail(3, `${table.name} metadata query failed: ${r.reason.message}`);
                }
            });
        }

        // ─── Phase 4: Delete and Recovery ────────────────────────────────
        console.log('\nPhase 4: Delete and Recovery');

        if (!fullRefreshRan || fullRefreshSucceededTables.length === 0) {
            console.log('  ⚠️  No tables succeeded in full refresh — skipping delete+recovery');
        } else {
            const deletedPartitionsPerTable = {};

            // Delete recent partitions from each table that succeeded in full refresh
            const deleteResults = await Promise.allSettled(
                fullRefreshSucceededTables.map(table =>
                    bq.deleteRecentPartitions(bigquery, config.projectId, table.dataset, table.name, config.partitionsToDelete)
                )
            );
            fullRefreshSucceededTables.forEach((table, i) => {
                const r = deleteResults[i];
                if (r.status === 'fulfilled') {
                    if (r.value.deletedPartitions.length > 0) {
                        deletedPartitionsPerTable[table.name] = r.value.deletedPartitions;
                        pass(4, `${table.name}: Deleted ${r.value.deletedPartitions.length} partitions (${r.value.deletedRows.toLocaleString()} rows): ${r.value.deletedPartitions.join(', ')}`);
                    } else {
                        pass(4, `${table.name}: No partitions to delete (table may be empty)`);
                    }
                } else {
                    fail(4, `${table.name} partition delete failed: ${r.reason.message}`);
                }
            });

            // Run incremental to recover deleted data
            let recoverySucceededTables = [];
            try {
                const result = await dataform.runWorkflowInvocation(
                    dfClient, config.dataformRepository, compilationResultName,
                    { tag: config.tableTag, fullRefresh: false, timeoutMs: config.workflowTimeoutMs }
                );

                const actions = await dataform.getActionResults(dfClient, result.name);
                const succeeded = actions.filter(a => a.state === 'SUCCEEDED');
                const failed_ = actions.filter(a => a.state === 'FAILED');

                if (failed_.length === 0) {
                    pass(4, `Recovery invocation SUCCEEDED (${formatElapsed(result.elapsedMs)})`);
                } else {
                    pass(4, `Recovery completed: ${succeeded.length} succeeded, ${failed_.length} failed (${formatElapsed(result.elapsedMs)})`);
                    failed_.forEach(a => fail(4, `Action ${a.target} FAILED: ${a.failureReason}`));
                }

                recoverySucceededTables = fullRefreshSucceededTables.filter(t =>
                    succeeded.some(a => a.target.endsWith(`.${t.name}`))
                );
            } catch (err) {
                fail(4, `Recovery run error: ${err.message}`);
            }

            // Validate recovery for tables that succeeded
            const recoveryTablesToValidate = recoverySucceededTables.filter(t => {
                const dp = deletedPartitionsPerTable[t.name];
                return dp && dp.length > 0;
            });
            if (recoveryTablesToValidate.length > 0) {
                const recoveryResults = await Promise.allSettled(
                    recoveryTablesToValidate.map(table =>
                        bq.validatePartitionRecovery(
                            bigquery, config.projectId, table.dataset, table.name,
                            deletedPartitionsPerTable[table.name]
                        )
                    )
                );
                recoveryTablesToValidate.forEach((table, i) => {
                    const r = recoveryResults[i];
                    if (r.status === 'fulfilled') {
                        r.value.partitions.forEach(p => {
                            if (p.totalRows > 0) {
                                pass(4, `${table.name} partition ${p.partitionId}: recovered (${p.totalRows} rows)`);
                            } else {
                                fail(4, `${table.name} partition ${p.partitionId}: NOT recovered (0 rows)`);
                            }
                        });
                    } else {
                        fail(4, `${table.name} recovery validation failed: ${r.reason.message}`);
                    }
                });
            }
        }

    } finally {
        // ─── Phase 5: Cleanup ────────────────────────────────────────────
        console.log('\nPhase 5: Cleanup');

        if (workspacePath) {
            try {
                await dataform.deleteTestWorkspace(dfClient, workspacePath);
                pass(5, 'Workspace deleted');
            } catch (err) {
                fail(5, `Workspace cleanup failed: ${err.message}`);
            }
        }
    }

    return printSummary();
};

// ─── Entry Point ─────────────────────────────────────────────────────────────

if (require.main === module) {
    run().then(failCount => {
        process.exit(failCount > 0 ? 1 : 0);
    }).catch(err => {
        console.error('\n❌ Integration test suite failed with unexpected error:');
        console.error(err);
        process.exit(1);
    });
}

module.exports = { run };
