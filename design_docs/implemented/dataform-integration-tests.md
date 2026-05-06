# Dataform Integration Tests

**Status**: Planned
**Priority**: P1 (Medium)
**Estimated**: 2-3 days (implementation + validation)
**Dependencies**: Existing test Dataform repository in Google Cloud

## Problem Statement

The package currently has **unit-level tests** that validate:
- SQL syntax via BigQuery dry runs (zero cost, zero execution)
- Configuration merging logic
- Pre-operations SQL generation
- Documentation/description generation

What's **missing** is end-to-end validation that the package actually works when deployed in a real Dataform repository. Recent bugs have demonstrated this gap:

- **Dataform runtime object freezing** — `Object.freeze` in Dataform's sandboxed V8 caused silent mutation failures. Unit tests passed; Dataform runs did not produce expected output.
- **Shallow copy mutation across `publish()` calls** — Nested `bigquery` config was shared across multiple `createTable` calls in the same process. Unit tests test single calls in isolation.
- **Partitioning spec changes** — An incremental table with a changed partitioning spec errors at runtime, but unit tests don't detect this because they don't run against existing tables.
- **Pre-operations trailing comments** — Dataform CLI/extension parsed the comment differently than BigQuery dry runs.

These classes of bugs are only caught by running the package in an actual Dataform environment against real BigQuery tables.

## Goals

- A single command (`npm run test:integration`) validates a package version end-to-end in Dataform
- Tests cover: compilation, incremental run, full refresh, data deletion + recovery
- Each step produces clear pass/fail with diagnostic information
- Supports testing both dev (pre-release) and prod versions
- Can be run manually; architecture supports future automation via GitHub Actions
- Uses the existing test Dataform repository — no new infrastructure to set up

## Solution Design

### Architecture Overview

```
tests/integration/
  integration.test.js      — Main orchestrator (sequential phases)
  dataformClient.js        — Dataform API wrapper (compile, run, poll, workspace management)
  bigqueryValidator.js     — BigQuery validation queries (row counts, export types, partitions)
  config.js                — Environment variable loading and validation
  README.md                — Setup instructions and troubleshooting
```

The test script lives in the ga4-export-fixer repo alongside existing tests. It connects to a dedicated test Dataform repository via the Dataform API and validates results via the BigQuery API.

### Environment Configuration

Extend `tests/.env` with integration test variables:

```env
# Existing
GOOGLE_CLOUD_PROJECT=tanelytics-tf
BIGQUERY_LOCATION=EU

# New: Integration test configuration
DATAFORM_REPOSITORY=projects/tanelytics-tf/locations/europe-west1/repositories/ga4-export-fixer-test
INTEGRATION_TEST_TIMEOUT_MS=600000
```

Optional override:
```bash
# Test a specific version (defaults to current package.json version)
INTEGRATION_TEST_VERSION=0.4.5-dev.0 npm run test:integration
```

### Package Version Update Strategy

The test uses the **Dataform Workspaces API** to update the package version without committing to git:

1. Create a temporary workspace in the test Dataform repository
2. Pull latest from the default branch
3. Read the existing `package.json` from the workspace
4. Update the `ga4-export-fixer` version
5. Write the modified `package.json` back via `writeFile`
6. Call `installNpmPackages` to install dependencies
7. Compile and run against the workspace
8. Delete the workspace on cleanup

This avoids polluting the test repo's git history with version bumps.

### Test Flow (5 Phases)

#### Phase 0: Setup

```
1. Load config from environment
2. Initialize Dataform client (@google-cloud/dataform)
3. Initialize BigQuery client (@google-cloud/bigquery)
4. Determine package version to test
5. Create Dataform workspace
6. Pull latest from default branch
7. Update package.json with target version
8. Install npm packages
```

#### Phase 1: Compilation

```
9.  Create compilation result (referencing workspace, not git commitish)
10. Assert: no compilation errors
11. Query compilation result actions to discover tables with 'ga4_export_fixer' tag
12. Record table targets (dataset + table name) for subsequent validation
```

**Validation:**
- Compilation succeeds without errors
- At least one action with `ga4_export_fixer` tag exists

#### Phase 2: Incremental Run

```
13. Snapshot table metadata BEFORE run:
    - INFORMATION_SCHEMA.PARTITIONS: partition dates, row counts, last_modified_time
14. Create workflow invocation:
    - includedTags: ['ga4_export_fixer']
    - fullyRefreshIncrementalTablesEnabled: false
    - transitiveDependenciesIncluded: true
15. Poll getWorkflowInvocation until terminal state
16. Assert: invocation state === SUCCEEDED
17. Query individual action results
18. Assert: all actions SUCCEEDED
```

**Post-run validation:**
- Tables have data (total rows > 0)
- `row_inserted_timestamp` is recent (within last 30 minutes)
- Export type distribution matches `includedExportTypes` config (e.g., if `fresh: false`, no rows with `export_type = 'fresh'`)

#### Phase 3: Full Refresh Run

```
19. Create workflow invocation:
    - includedTags: ['ga4_export_fixer']
    - fullyRefreshIncrementalTablesEnabled: true
    - transitiveDependenciesIncluded: true
20. Poll until terminal state
21. Assert: invocation state === SUCCEEDED
22. Assert: all actions SUCCEEDED
```

**Post-run validation:**
- Tables exist and have rows
- Partitioning and clustering intact (no "partitioning spec changed" error — already caught by step 21)
- `row_inserted_timestamp` is recent

#### Phase 4: Delete and Incremental Recovery

```
23. For each tagged table:
    - Query the 2 most recent partition dates
    - Record pre-delete row counts per partition
    - DELETE FROM table WHERE event_date IN (partition1, partition2)
    - Verify rows were deleted (row count decreased)
24. Create workflow invocation (incremental, same as Phase 2)
25. Poll until terminal state
26. Assert: invocation state === SUCCEEDED
```

**Post-run validation:**
- Previously-deleted partitions have rows again (row count > 0)
- Data recovery is complete — row counts are within expected range

#### Phase 5: Cleanup

```
27. Delete the Dataform workspace (always runs, even on failure — try/finally)
```

Tables in BigQuery are left in place (they belong to a dedicated test dataset and will be overwritten on next run).

### Validation Queries

**Table metadata snapshot:**
```sql
SELECT
  table_name,
  partition_id,
  total_rows,
  TIMESTAMP_MILLIS(last_modified_time) as last_modified
FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = '{table}'
  AND partition_id != '__NULL__'
ORDER BY partition_id
```

**Export type distribution:**
```sql
SELECT export_type, COUNT(*) as row_count
FROM `{project}.{dataset}.{table}`
GROUP BY export_type
```

**Data freshness check:**
```sql
SELECT MAX(row_inserted_timestamp) as latest_insert
FROM `{project}.{dataset}.{table}`
```

**Delete recent partitions:**
```sql
DELETE FROM `{project}.{dataset}.{table}`
WHERE event_date IN (
  SELECT DISTINCT event_date
  FROM `{project}.{dataset}.{table}`
  ORDER BY event_date DESC
  LIMIT 2
)
```

### Table Discovery

Tables to validate are discovered from the **compilation result**, not hardcoded:

```javascript
const actions = await dataformClient.queryCompilationResultActions({
  name: compilationResultName,
});
// Filter for actions with 'ga4_export_fixer' tag
const tables = actions.filter(a =>
  a.target && a.tags?.includes('ga4_export_fixer')
);
```

This ensures the test automatically adapts when new table modules are added.

### Error Handling

- **try/finally at top level**: workspace deletion always runs
- **Timeout per invocation**: configurable via `INTEGRATION_TEST_TIMEOUT_MS` (default 10 minutes)
- **Invocation cancellation**: if timeout exceeded, cancel the running invocation before cleanup
- **Phase skipping**: if compilation fails, skip all run phases; if incremental fails, skip delete+recovery but still clean up
- **Structured results**: each phase produces a `{ passed, failed, details }` result; summary printed at end

### Output Format

Follows the existing test convention (raw Node.js, console output):

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║               GA4 EXPORT FIXER - INTEGRATION TESTS                            ║
╚═══════════════════════════════════════════════════════════════════════════════╝

Package version: 0.4.5-dev.0
Repository: projects/tanelytics-tf/locations/europe-west1/repositories/ga4-export-fixer-test

Phase 0: Setup
  ✅ Workspace created: integration-test-1712592000
  ✅ Package version updated to 0.4.5-dev.0
  ✅ NPM packages installed

Phase 1: Compilation
  ✅ Compilation succeeded (0 errors)
  ✅ Found 1 table(s) with 'ga4_export_fixer' tag

Phase 2: Incremental Run
  ✅ Workflow invocation SUCCEEDED (45s)
  ✅ All 1 action(s) succeeded
  ✅ ga4_events_enhanced_298233330: 12,453 rows, 30 partitions
  ✅ Export types: daily (8,201), intraday (4,252)
  ✅ Latest insert: 2026-04-08T14:32:01Z

Phase 3: Full Refresh
  ✅ Workflow invocation SUCCEEDED (2m 15s)
  ✅ All 1 action(s) succeeded
  ✅ ga4_events_enhanced_298233330: 892,104 rows, 365 partitions

Phase 4: Delete and Recovery
  ✅ Deleted 2 partitions from ga4_events_enhanced_298233330 (1,204 rows)
  ✅ Incremental recovery SUCCEEDED (38s)
  ✅ Partition 20260407: 0 → 602 rows (recovered)
  ✅ Partition 20260408: 0 → 598 rows (recovered)

Phase 5: Cleanup
  ✅ Workspace deleted

═══════════════════════════════════════════════════════════════════════════════
INTEGRATION TEST SUMMARY
═══════════════════════════════════════════════════════════════════════════════
Total: 14 | Passed: 14 | Failed: 0
🎉 All integration tests passed!
```

### Dependencies

Add to `devDependencies`:

```json
{
  "@google-cloud/dataform": "^3.0.0"
}
```

The `@google-cloud/bigquery` dependency already exists.

### npm Script

```json
{
  "scripts": {
    "test:integration": "node tests/integration/integration.test.js"
  }
}
```

### Future: GitHub Actions Automation

A `workflow_dispatch` action can trigger the integration test for a specific version:

```yaml
name: Integration Test
on:
  workflow_dispatch:
    inputs:
      version:
        description: 'Package version to test (e.g., 0.4.5-dev.0)'
        required: true

jobs:
  integration-test:
    runs-on: ubuntu-latest
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '20'
      - run: npm install
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.WIF_PROVIDER }}
          service_account: ${{ secrets.GCP_SERVICE_ACCOUNT }}
      - run: npm run test:integration
        env:
          INTEGRATION_TEST_VERSION: ${{ inputs.version }}
          DATAFORM_REPOSITORY: ${{ secrets.DATAFORM_REPOSITORY }}
          GOOGLE_CLOUD_PROJECT: ${{ secrets.GCP_PROJECT_ID }}
          BIGQUERY_LOCATION: EU
```

This is deferred — the manual `npm run test:integration` command is the primary interface.

### Assertion Result Collection

After each workflow invocation (Phases 2, 3, and 4), the integration test should also check the results of any Dataform assertion actions that ran. Assertions are already executed by the workflow invocation because `transitiveDependenciesIncluded: true` pulls in assertion dependencies, but their results are currently reported alongside table actions without being called out.

#### Discovery

Assertions are discovered from the compilation result the same way tables are, but filtered by action type. The existing `discoverTaggedActions` function discovers tables (`action.relation != null`). A parallel function discovers assertions using the same `ga4_export_fixer` tag:

```javascript
// In dataformClient.js
const discoverTaggedAssertions = async (client, compilationResultName, tag) => {
    const assertions = [];
    const iterable = client.queryCompilationResultActionsAsync({
        name: compilationResultName,
    });
    for await (const action of iterable) {
        if (!action.target) continue;
        const assertionTags = action.assertion?.tags || [];
        const hasTag = assertionTags.includes(tag);
        const isAssertion = action.assertion != null;
        if (hasTag && isAssertion) {
            assertions.push({
                dataset: action.target.schema || action.target.database,
                name: action.target.name,
            });
        }
    }
    return assertions;
};
```

#### Phase 1 update

After discovering tables, also discover assertions with the same tag:

```
11b. Query compilation result actions to discover assertions with 'ga4_export_fixer' tag
12b. Record assertion targets for subsequent validation
```

**Validation:**
- Report how many assertions were found (0 is acceptable — assertions are optional)

#### Post-run assertion validation (Phases 2, 3, 4)

After each workflow invocation, check the action results for assertion outcomes:

```javascript
// Filter action results for discovered assertion targets
const assertionResults = actionResults.filter(a =>
    assertions.some(assertion => a.target.endsWith(`.${assertion.name}`))
);
const assertionsPassed = assertionResults.filter(a => a.state === 'SUCCEEDED');
const assertionsFailed = assertionResults.filter(a => a.state === 'FAILED');

if (assertionsPassed.length > 0) {
    pass(phase, `${assertionsPassed.length} assertion(s) passed: ${assertionsPassed.map(a => a.target).join(', ')}`);
}
assertionsFailed.forEach(a => {
    fail(phase, `Assertion ${a.target} FAILED: ${a.failureReason}`);
});
```

#### Expected output

```
Phase 2: Incremental Run
  ✅ Workflow SUCCEEDED: 2 action(s) (45s)
  ✅ 1 assertion(s) passed: analytics_532197724.item_revenue_check
  ✅ ga4_events_enhanced_532197724: 12,453 rows, 30 partitions
  ...
```

If an assertion fails:
```
  ❌ Assertion analytics_532197724.item_revenue_check FAILED: Assertion query returned 3 row(s)
```

#### Key design decisions

- **Same tag (`ga4_export_fixer`)**: Assertions are discovered using the same tag as tables. The test Dataform repository already tags the item_revenue assertion with this tag.
- **Assertion failures are test failures**: A failed assertion in any phase causes the integration test to report a failure, but does not skip subsequent phases (unlike a table action failure).
- **No assertion-specific BigQuery validation**: The assertion SQL itself does the validation. The integration test only checks whether Dataform reported the assertion as passed or failed — it doesn't re-run the assertion query.

## Files to Create/Modify

| File | Change | Est. LOC |
|------|--------|----------|
| `tests/integration/integration.test.js` | NEW — main orchestrator | ~200 |
| `tests/integration/dataformClient.js` | NEW — Dataform API wrapper (includes `discoverTaggedAssertions`) | ~170 |
| `tests/integration/bigqueryValidator.js` | NEW — BigQuery validation queries | ~120 |
| `tests/integration/config.js` | NEW — environment config loader | ~40 |
| `tests/integration/README.md` | NEW — setup and usage docs | ~60 |
| `tests/.env` | MODIFY — add integration test variables | +5 lines |
| `package.json` | MODIFY — add `test:integration` script, `@google-cloud/dataform` devDependency | +3 lines |

## Success Criteria

- [ ] `npm run test:integration` runs all 5 phases end-to-end
- [ ] Compilation errors are detected and reported clearly
- [ ] Incremental run produces data matching configured export types
- [ ] Full refresh succeeds without partitioning/clustering errors
- [ ] Deleted partitions are recovered by incremental refresh
- [ ] Workspace is always cleaned up, even on failure
- [ ] Test works with both dev (pre-release) and prod versions
- [ ] Assertion results (pass/fail) are collected and reported after each workflow run
- [ ] A failing assertion causes the integration test to report a failure
- [ ] All existing tests (`npm test`) still pass

## Testing Strategy

- **Manual validation**: Run against the test Dataform repository, verify each phase passes
- **Failure modes**: Deliberately break the package (e.g., change partitioning config) and verify the test catches it
- **Timeout handling**: Test with a very short timeout to verify cancellation and cleanup work
- **Version testing**: Test with a known-good version and a known-bad version

## Non-Goals

- Automated triggering on npm publish (deferred to GitHub Actions setup later)
- Testing `.sqlx` deployment method (the test Dataform repo uses JS deployment)
- Performance benchmarking (this is functional validation only)
- Testing multiple Dataform repositories or projects
- Modifying the test Dataform repository's table definitions (those are maintained separately)

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Dataform API rate limits or quotas | Medium | Single sequential test run; add retry with backoff for transient errors |
| Test Dataform repository drift (someone changes table definitions) | Low | Test discovers tables dynamically from compilation result; doesn't hardcode table names |
| BigQuery costs from full refresh on large datasets | Low | Test repo uses a small GA4 property; full refresh scans are bounded by source data size |
| Workspace cleanup failure leaves dangling workspace | Low | Workspace name includes timestamp; old workspaces can be manually cleaned |
| Authentication issues in different environments | Medium | Document required IAM roles; test config validation fails fast with clear error messages |

## Required IAM Roles

The service account or user running the test needs:

- `roles/dataform.editor` — create workspaces, compile, run invocations
- `roles/bigquery.dataEditor` — read table metadata, delete partitions
- `roles/bigquery.jobUser` — run BigQuery queries for validation
