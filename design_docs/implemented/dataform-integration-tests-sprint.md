# Sprint Plan: Dataform Integration Tests

## Summary
Build an end-to-end integration test suite that validates ga4-export-fixer package versions by compiling and running them in a real Dataform repository, then validating the BigQuery output.

**Duration:** 3 days (5 milestones)
**Dependencies:** Existing test Dataform repository in Google Cloud, `@google-cloud/dataform` npm package
**Risk Level:** Medium (new API surface — Dataform Workspaces API; real BigQuery costs)

## Current Status Analysis

### Completed Recently
- Pre-operations trailing comment fix: ~3 LOC in 1 commit
- dataformTableConfig migration + bug fixes: ~50 LOC across 4 commits
- Design doc for multi-table module architecture: ~340 lines
- Design doc for integration tests: ~280 lines

### Velocity
- Recent average: Focused refactoring/feature tasks complete in single sessions
- Test files average ~300-400 LOC each (sqlValidator: 341, ga4EventsEnhanced.test: 228, preOperations.test: 545, documentation.test: 403)
- This sprint creates 4 new modules + 1 README

### Remaining from Design Doc
- M1 — Config and BigQuery validator: ~160 LOC
- M2 — Dataform API client: ~150 LOC
- M3 — Test orchestrator (Phases 0-1): ~100 LOC
- M4 — Test orchestrator (Phases 2-4): ~150 LOC
- M5 — Wiring, docs, end-to-end validation: ~40 LOC

## Proposed Milestones

### Milestone 1: Config loader and BigQuery validator
**Goal:** Create the environment config module and BigQuery validation queries needed by all subsequent milestones
**Estimated:** ~60 LOC implementation + ~100 LOC validator = ~160 LOC
**Duration:** Half day

**Tasks:**
- Create `tests/integration/config.js` (~40 LOC):
  - Load dotenv from `tests/.env`
  - Read and validate required env vars: `GOOGLE_CLOUD_PROJECT`, `BIGQUERY_LOCATION`, `DATAFORM_REPOSITORY`
  - Read optional env vars: `INTEGRATION_TEST_VERSION` (default: current package.json version), `INTEGRATION_TEST_TIMEOUT_MS` (default: 600000)
  - Export frozen config object with derived values
  - Fail fast with clear error if required vars missing

- Create `tests/integration/bigqueryValidator.js` (~120 LOC):
  - `snapshotTableMetadata(bigquery, project, dataset, tableName)` — query `INFORMATION_SCHEMA.PARTITIONS` for partition dates, row counts, last_modified_time
  - `validateExportTypes(bigquery, project, dataset, tableName, expectedTypes)` — query `GROUP BY export_type`, assert matches config
  - `validateDataFreshness(bigquery, project, dataset, tableName, maxAgeMinutes)` — check `MAX(row_inserted_timestamp)` is recent
  - `deleteRecentPartitions(bigquery, project, dataset, tableName, count)` — delete N most recent partitions via DML, return deleted partition info
  - `validatePartitionRecovery(bigquery, project, dataset, tableName, partitions)` — check previously-deleted partitions have rows again
  - Reuse `createBigQueryClient` pattern from existing `tests/sqlValidator.js`

- Update `tests/.env` with integration test variable examples (commented out)

**Acceptance Criteria:**
- [ ] `config.js` loads all required env vars and fails fast on missing
- [ ] `bigqueryValidator.js` exports all 5 validation functions
- [ ] Each function uses parameterized queries (no SQL injection)
- [ ] BigQuery client reuses existing authentication pattern

**Risks:**
- INFORMATION_SCHEMA query syntax differs between regions — Mitigation: Use standard `INFORMATION_SCHEMA.PARTITIONS` view which is consistent across regions

### Milestone 2: Dataform API client
**Goal:** Create the Dataform API wrapper that handles workspace lifecycle, compilation, and workflow invocations
**Estimated:** ~150 LOC
**Duration:** Half day

**Tasks:**
- Add `@google-cloud/dataform` to devDependencies in `package.json`
- Create `tests/integration/dataformClient.js` (~150 LOC):
  - `createTestWorkspace(client, repositoryPath, workspaceId)` — create workspace, pull latest from default branch
  - `updatePackageVersion(client, workspacePath, packageName, version)` — read existing package.json via `readFile`, update version, write back via `writeFile`, call `installNpmPackages`
  - `compileWorkspace(client, repositoryPath, workspacePath)` — create compilation result referencing workspace, check for compilation errors, return result name
  - `discoverTaggedActions(client, compilationResultName, tag)` — query compilation result actions, filter by tag, return target list (dataset + table name pairs)
  - `runWorkflowInvocation(client, repositoryPath, compilationResultName, { tag, fullRefresh, timeoutMs })` — create invocation with `includedTags` and `fullyRefreshIncrementalTablesEnabled`, poll `getWorkflowInvocation` until terminal state, return result
  - `getActionResults(client, invocationName)` — query workflow invocation actions, return structured results with per-action status
  - `deleteTestWorkspace(client, workspacePath)` — delete the workspace (for cleanup)
  - Polling helper: exponential backoff starting at 5s, capped at 30s, total timeout configurable

**Acceptance Criteria:**
- [ ] All 7 functions exported and handle API errors gracefully
- [ ] Workspace lifecycle: create → update → compile → run → delete works end-to-end
- [ ] Polling respects timeout and reports elapsed time
- [ ] API errors wrapped with clear context (which operation failed and why)

**Risks:**
- Dataform API client version compatibility — Mitigation: Pin to specific `@google-cloud/dataform` version in devDependencies
- Workspace name collisions — Mitigation: Include timestamp in workspace ID

### Milestone 3: Test orchestrator — Setup and Compilation (Phases 0-1)
**Goal:** Create the main test file with the orchestrator structure, Phase 0 (setup), and Phase 1 (compilation)
**Estimated:** ~100 LOC
**Duration:** Half day

**Tasks:**
- Create `tests/integration/integration.test.js` (~100 LOC for this milestone):
  - Banner and header output matching existing test style (box-drawing characters)
  - Phase 0: Setup
    - Initialize Dataform and BigQuery clients
    - Create workspace
    - Update package version
    - Install packages
    - Report success/failure for each setup step
  - Phase 1: Compilation
    - Compile the workspace
    - Assert no compilation errors
    - Discover tables with `ga4_export_fixer` tag
    - Assert at least one tagged action found
    - Store table targets for subsequent phases
  - try/finally structure ensuring workspace cleanup on any failure
  - Structured result collection (array of `{ phase, step, passed, detail }`)
  - Summary output at end

- Add `test:integration` script to `package.json`

**Acceptance Criteria:**
- [ ] `npm run test:integration` runs Phases 0-1 end-to-end
- [ ] Compilation errors are reported with clear error messages
- [ ] Tagged tables are discovered and logged
- [ ] Workspace is cleaned up on success and on failure
- [ ] Phase 1 failure skips subsequent phases but still cleans up

**Risks:**
- Authentication not configured — Mitigation: config.js validation catches this early with instructions

### Milestone 4: Run phases — Incremental, Full Refresh, Delete+Recovery (Phases 2-4)
**Goal:** Add the three execution phases that validate actual Dataform runs and BigQuery data
**Estimated:** ~150 LOC (added to `integration.test.js`)
**Duration:** 1 day

**Tasks:**
- Phase 2: Incremental Run (~50 LOC)
  - Snapshot table metadata before run
  - Run workflow invocation (incremental)
  - Assert invocation SUCCEEDED
  - Assert all actions SUCCEEDED
  - Validate: tables have rows, `row_inserted_timestamp` is recent, export types match config
  - Report per-table results

- Phase 3: Full Refresh Run (~40 LOC)
  - Run workflow invocation (full refresh)
  - Assert invocation SUCCEEDED
  - Assert all actions SUCCEEDED
  - Validate: tables exist with rows, no partitioning spec errors
  - Report table row counts

- Phase 4: Delete and Recovery (~60 LOC)
  - For each tagged table: delete 2 most recent partitions
  - Log what was deleted (partition dates, row counts)
  - Run workflow invocation (incremental)
  - Assert invocation SUCCEEDED
  - Validate: deleted partitions recovered (row count > 0)
  - Report recovery results per partition

**Acceptance Criteria:**
- [ ] Phase 2 detects incremental run failures (e.g., partitioning spec change)
- [ ] Phase 2 validates export type distribution against expected types
- [ ] Phase 3 full refresh succeeds and rebuilds all data
- [ ] Phase 4 confirms incremental refresh can recover deleted partitions
- [ ] Each phase logs elapsed time
- [ ] Phase failures are reported but don't prevent cleanup

**Risks:**
- Full refresh takes too long — Mitigation: Test Dataform repo uses a small GA4 property; configurable timeout
- Delete query fails on empty tables — Mitigation: Check row count before attempting delete; skip if empty

### Milestone 5: Documentation and end-to-end validation
**Goal:** Write the README, run the full test suite against the real Dataform repository, fix any issues
**Estimated:** ~40 LOC (README) + bug fixes
**Duration:** Half day

**Tasks:**
- Create `tests/integration/README.md` (~60 lines):
  - Prerequisites (GCP project, Dataform repository, IAM roles, authentication)
  - Environment variable reference
  - Usage examples (default version, specific version)
  - Troubleshooting (common errors and fixes)
  - IAM roles required: `roles/dataform.editor`, `roles/bigquery.dataEditor`, `roles/bigquery.jobUser`

- End-to-end validation:
  - Run `npm run test:integration` against the test Dataform repository
  - Verify all 5 phases pass
  - Fix any issues found during real execution
  - Run `npm test` to confirm existing tests still pass

**Acceptance Criteria:**
- [ ] `npm run test:integration` passes all phases end-to-end
- [ ] README covers setup, usage, and troubleshooting
- [ ] `npm test` (existing tests) still passes
- [ ] Test output is clear and diagnostic (matches existing test style)

**Risks:**
- Real API behavior differs from expected — Mitigation: Budget time for debugging; this milestone is the validation buffer

## Success Metrics
- `npm run test:integration` runs all 5 phases with clear pass/fail output
- Tests catch real bugs (e.g., partitioning spec change, compilation error)
- Existing tests (`npm test`) unaffected
- README enables someone else to run the integration tests

## Dependencies
- Test Dataform repository must exist and be accessible
- GCP authentication configured (Application Default Credentials or service account)
- `@google-cloud/dataform` npm package available

## Resolved Questions
- **Dataform repository path:** `projects/tanelytics-tf/locations/europe-north1/repositories/ga4_export_fixer_test`
- **Table definitions:** Already exist in the test repository — no setup needed
- **Test command:** Stays separate as `npm run test:integration` (not included in `npm test`)

## Notes
- The Dataform Workspaces API approach means we never commit test version bumps to the repo — workspaces are ephemeral
- BigQuery costs are minimal: the test GA4 property is small, and we only run 3 invocations (incremental + full refresh + incremental recovery)
- The GitHub Actions automation is intentionally deferred — get the manual test working first, automate later
- Each milestone is independently testable: M1-M2 produce utility modules that can be tested in isolation; M3 tests setup+compilation; M4 adds the run phases; M5 validates everything together
