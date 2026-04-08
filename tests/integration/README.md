# Integration Tests

End-to-end validation of ga4-export-fixer in a real Google Cloud Dataform repository.

## What it tests

1. **Compilation** — installs the target package version, compiles, checks for errors
2. **Incremental run** — runs tagged actions, validates data exists, export types match, timestamps are fresh
3. **Full refresh** — rebuilds all tables, validates no partitioning/clustering errors
4. **Delete + recovery** — deletes recent partitions, runs incremental again, validates data recovers

## Prerequisites

- A Google Cloud Dataform repository with table definitions that use `ga4-export-fixer`
- GCP authentication configured (`gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`)
- Required IAM roles on the service account / user:
  - `roles/dataform.editor` — create workspaces, compile, run invocations
  - `roles/bigquery.dataEditor` — read table metadata, delete partitions
  - `roles/bigquery.jobUser` — run BigQuery queries for validation

## Configuration

Set these in `tests/.env` or as environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GOOGLE_CLOUD_PROJECT` | Yes | — | GCP project ID |
| `BIGQUERY_LOCATION` | No | `EU` | BigQuery dataset location |
| `DATAFORM_REPOSITORY` | Yes | — | Full Dataform repository resource path |
| `INTEGRATION_TEST_VERSION` | No | Current `package.json` version | Package version to test |
| `INTEGRATION_TEST_TIMEOUT_MS` | No | `600000` (10 min) | Max wait time per workflow invocation |

The `DATAFORM_REPOSITORY` value should be in the format:
```
projects/{PROJECT_ID}/locations/{LOCATION}/repositories/{REPOSITORY_ID}
```

## Usage

```bash
# Test the current package version
npm run test:integration

# Test a specific version
INTEGRATION_TEST_VERSION=0.4.5-dev.0 npm run test:integration
```

## Troubleshooting

**"Missing required environment variable: DATAFORM_REPOSITORY"**
Add `DATAFORM_REPOSITORY=projects/.../locations/.../repositories/...` to `tests/.env`.

**"Compilation failed"**
The package version may have breaking changes. Check the compilation error messages for details.

**"Workflow invocation TIMEOUT"**
Increase `INTEGRATION_TEST_TIMEOUT_MS` or check if the Dataform repository has long-running queries.

**"PERMISSION_DENIED"**
Ensure the authenticated user/service account has the required IAM roles listed above.

**"Workspace creation failed"**
Check that the Dataform repository exists and the user has `dataform.editor` permissions.
