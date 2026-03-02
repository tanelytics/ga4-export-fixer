# GA4 Export Fixer - SQL Validation Tests

This directory contains SQL validation tests for the GA4 Export Fixer package using BigQuery dry run validation.

## Overview

The test suite validates generated SQL queries against BigQuery's API **without executing them or incurring query costs**. This ensures:

- ✅ SQL syntax is correct
- ✅ Schema references are valid
- ✅ Permissions are sufficient
- ✅ Queries will run successfully in production

## Test Structure

```
tests/
├── sqlValidator.js              # Core SQL validation utility
├── ga4EventsEnhanced.test.js   # Test suite for GA4 Events Enhanced
├── .env.example                # Example environment configuration
└── README.md                   # This file
```

## Setup

### 1. Install Dev Dependencies

```bash
npm install
```

This installs `@google-cloud/bigquery` and `dotenv` as **devDependencies** (they won't be included when others install your package).

### 2. Configure Environment

Copy the example env file:

```bash
cp tests/.env.example tests/.env
```

Edit `tests/.env` with your values:

```env
GOOGLE_CLOUD_PROJECT=your-project-id
BIGQUERY_LOCATION=US

# Source table: GA4 export table to read from
TEST_SOURCE_TABLE=`your-project.your-dataset.events_*`

# Target table: Destination table for processed data
# Used for pre-operations validation (incremental refresh logic)
TEST_TARGET_TABLE=`your-project.your-dataset.ga4_all_events`
```

**Environment Variables Explained:**

- `GOOGLE_CLOUD_PROJECT` - Your GCP project ID
- `BIGQUERY_LOCATION` - BigQuery location (US, EU, etc.) - should match your dataset location
- `TEST_SOURCE_TABLE` - The GA4 export table (source data) - supports wildcards like `events_*`
- `TEST_TARGET_TABLE` - The destination table where processed data is written
  - Used by pre-operations to validate DELETE statements
  - Required for testing incremental refresh logic
  - Must exist in BigQuery or the pre-operations test will fail

### 3. Authenticate with Google Cloud

Choose one authentication method:

#### Option A: Application Default Credentials (Recommended)

```bash
gcloud auth application-default login
```

#### Option B: Service Account Key

1. Create a service account with `bigquery.jobs.create` permission
2. Download the JSON key file
3. Set in `.env`:

```env
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json
```

## Running Tests

### Run All Tests

```bash
npm test
```

or

```bash
npm run test:sql
```

### Run Specific Test Functions

```javascript
const { testMainQuery, testPreOperationsSQL } = require('./tests/ga4EventsEnhanced.test');

// Test main query only
await testMainQuery();

// Test pre-operations only
await testPreOperationsSQL();
```

## Test Output Example

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                     GA4 EXPORT FIXER - SQL VALIDATION TESTS                   ║
╚═══════════════════════════════════════════════════════════════════════════════╝


📝 TEST 1: Main Query Validation

================================================================================
VALIDATING SQL QUERY
================================================================================

Query:
with event_data as (
  select
    cast(event_date as date format 'YYYYMMDD') as event_date,
    ...
)
select * from event_data

--------------------------------------------------------------------------------

✅ SQL Validation PASSED

Query Statistics:
  📊 Estimated bytes: 2.45 GB (2,453,678,234 bytes)
  💰 Estimated cost: $0.012268
  🔄 Cache hit: false
  📝 Statement type: SELECT
  📋 Output columns: 18

  Column Schema:
    1. event_date (DATE)
    2. event_datetime (DATETIME)
    3. event_timestamp (INT64)
    ...

  📚 Referenced tables: 1
    1. your-project.your-dataset.events_20250123

================================================================================


📝 TEST 2: Pre-operations SQL Validation

✅ SQL Validation PASSED

...

╔═══════════════════════════════════════════════════════════════════════════════╗
║                              TEST SUMMARY                                      ║
╚═══════════════════════════════════════════════════════════════════════════════╝

Total Tests: 6
✅ Passed: 6
❌ Failed: 0

Total Estimated Processing:
  Bytes: 12,345,678,901
  Cost: $0.061728

🎉 All tests passed!
```

## Using the SQL Validator Utility

The `sqlValidator.js` module provides reusable functions for SQL validation:

### Basic Usage

```javascript
const { validateSQL } = require('./tests/sqlValidator');

const result = await validateSQL('SELECT * FROM `project.dataset.table`', {
  projectId: 'my-project',
  location: 'US',
  verbose: true
});

if (result.success) {
  console.log('Query is valid!');
  console.log('Would process:', result.statistics.totalBytesProcessed, 'bytes');
  console.log('Estimated cost:', result.statistics.estimatedCostUSD, 'USD');
} else {
  console.error('Query failed:', result.errorMessage);
}
```

### Validate Multiple Queries

```javascript
const { validateMultipleSQL } = require('./tests/sqlValidator');

const queries = [
  { name: 'Query 1', sql: 'SELECT ...' },
  { name: 'Query 2', sql: 'SELECT ...' },
];

const results = await validateMultipleSQL(queries, {
  location: 'US'
});

// Results array with success/failure for each query
```

### API Reference

#### `validateSQL(sql, config)`

Validates a single SQL query.

**Parameters:**
- `sql` (string): SQL query to validate
- `config` (object): Configuration options
  - `projectId` (string): GCP project ID
  - `location` (string): BigQuery location (default: 'US')
  - `verbose` (boolean): Enable detailed logging (default: true)
  - `timeout` (number): Query timeout in milliseconds

**Returns:** Promise<ValidationResult>

```typescript
interface ValidationResult {
  success: boolean;
  sql: string;
  statistics?: {
    totalBytesProcessed: number;
    estimatedCostUSD: number;
    query: {
      cacheHit: boolean;
      statementType: string;
      schema: { fields: Array<{name: string, type: string}> };
      referencedTables: Array<{projectId, datasetId, tableId}>;
    };
  };
  error?: Error;
  errorMessage?: string;
  errorDetails?: Array<{message, location, reason}>;
  validationTimeMs: number;
}
```

#### `validateMultipleSQL(queries, config)`

Validates multiple SQL queries and provides summary.

**Parameters:**
- `queries` (Array<{name: string, sql: string}>): Named queries to validate
- `config` (object): Configuration options (same as `validateSQL`)

**Returns:** Promise<Array<ValidationResult>>

## Writing New Tests

To create tests for a new SQL generator:

1. Create a new test file in `tests/` directory:

```javascript
// tests/myNewTable.test.js
const { validateSQL } = require('./sqlValidator');
const myTable = require('../tables/myNewTable');

const testMyTable = async () => {
  const config = { /* your config */ };
  const sql = myTable.generateSQL(config);
  
  const result = await validateSQL(sql, {
    location: 'US',
    verbose: true
  });
  
  return result;
};

if (require.main === module) {
  testMyTable().then(result => {
    process.exit(result.success ? 0 : 1);
  });
}

module.exports = { testMyTable };
```

2. Add test script to `package.json`:

```json
{
  "scripts": {
    "test:my-table": "node tests/myNewTable.test.js"
  }
}
```

## Benefits of Dev Dependencies

By using `devDependencies` instead of `dependencies`:

✅ **Smaller package size**: Users installing your package won't download BigQuery SDK and dotenv
✅ **Faster installs**: Production dependencies are minimal
✅ **Clear separation**: Test tools separate from runtime code
✅ **Better DX**: Contributors get full test suite with `npm install`

When publishing:
```bash
npm publish  # devDependencies are automatically excluded
```

## Troubleshooting

### "Could not load the default credentials"

**Solution**: Authenticate with `gcloud auth application-default login`

### "Permission denied"

**Solution**: Ensure your account has `bigquery.jobs.create` permission

### "Not found: Table ..."

**Solution**: Update table references in `tests/.env` or test configuration

### "Module not found: @google-cloud/bigquery"

**Solution**: Run `npm install` to install dev dependencies

## Cost

- ✅ Dry runs are **FREE** - they don't process any data
- ✅ Validation only checks syntax and schema
- ✅ No query execution costs

## CI/CD Integration

Add to your GitHub Actions workflow:

```yaml
- name: Run SQL Validation Tests
  run: npm test
  env:
    GOOGLE_APPLICATION_CREDENTIALS: ${{ secrets.GCP_SA_KEY }}
    GOOGLE_CLOUD_PROJECT: ${{ secrets.GCP_PROJECT_ID }}
```

## Resources

- [BigQuery Dry Run Documentation](https://cloud.google.com/bigquery/docs/samples/bigquery-query-dry-run)
- [BigQuery Node.js Client](https://github.com/googleapis/nodejs-bigquery)
- [Authentication Guide](https://cloud.google.com/docs/authentication/getting-started)
