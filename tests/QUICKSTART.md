# Quick Start: SQL Validation Tests

## 1. Install Dependencies

```bash
npm install
```

## 2. Setup Environment

```bash
# Copy example env file
cp tests/.env.example tests/.env

# Edit tests/.env with your values
# Update: GOOGLE_CLOUD_PROJECT, TEST_SOURCE_TABLE, TEST_TARGET_TABLE
```

## 3. Authenticate

```bash
# Option A: Application Default Credentials (Recommended)
gcloud auth application-default login

# Option B: Service Account
# Download key.json and set GOOGLE_APPLICATION_CREDENTIALS in tests/.env
```

## 4. Run Tests

```bash
# Run all SQL validation tests
npm test

# Or specifically
npm run test:sql
```

## Example Output

```
✅ SQL Validation PASSED

Query Statistics:
  📊 Estimated bytes: 2.45 GB
  💰 Estimated cost: $0.012268
  📋 Output columns: 18

🎉 All tests passed!
```

## What Gets Validated?

✅ SQL syntax correctness
✅ Table/column existence
✅ Schema compatibility
✅ Permission sufficiency
✅ Query execution feasibility

## Cost

**FREE** - Dry runs don't process data or incur costs

## Next Steps

- Read full documentation: `tests/README.md`
- Customize test configurations in `tests/ga4EventsEnhanced.test.js`
- Add tests for new SQL generators

## Need Help?

See `tests/README.md` for detailed documentation and troubleshooting.
