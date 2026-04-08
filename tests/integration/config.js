/**
 * Integration Test Configuration
 *
 * Loads environment variables for Dataform integration tests.
 * Fails fast with clear messages if required variables are missing.
 */

require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const pkg = require('../../package.json');

const required = (name) => {
    const value = process.env[name];
    if (!value) {
        throw new Error(
            `Missing required environment variable: ${name}\n` +
            `Set it in tests/.env or export it before running integration tests.`
        );
    }
    return value;
};

const config = Object.freeze({
    // GCP project and location
    projectId: required('GOOGLE_CLOUD_PROJECT'),
    bigqueryLocation: process.env.BIGQUERY_LOCATION || 'EU',

    // Dataform repository resource path
    dataformRepository: required('DATAFORM_REPOSITORY'),

    // Package version to test (defaults to current package.json version)
    packageVersion: process.env.INTEGRATION_TEST_VERSION || pkg.version,
    packageName: pkg.name,

    // Timeouts
    workflowTimeoutMs: parseInt(process.env.INTEGRATION_TEST_TIMEOUT_MS || '600000', 10),

    // Tag used to discover and filter tables
    tableTag: 'ga4_export_fixer',

    // How many recent partitions to delete in Phase 4
    partitionsToDelete: 2,

    // Max age (minutes) for data freshness validation
    maxDataAgeMinutes: 30,
});

module.exports = config;
