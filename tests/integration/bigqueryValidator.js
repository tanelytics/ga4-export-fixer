/**
 * BigQuery Validation Queries for Integration Tests
 *
 * Provides functions to validate table state after Dataform runs:
 * row counts, partition metadata, export types, data freshness, and partition recovery.
 */

const { BigQuery } = require('@google-cloud/bigquery');

/**
 * Create a BigQuery client using the same auth pattern as sqlValidator.js.
 * @param {string} projectId
 * @returns {BigQuery}
 */
const createClient = (projectId) => {
    const opts = {};
    if (projectId) opts.projectId = projectId;
    if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
        opts.keyFilename = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    }
    return new BigQuery(opts);
};

/**
 * Snapshot partition-level metadata for a table.
 * Returns an array of { partitionId, totalRows, lastModified } sorted by partitionId.
 *
 * @param {BigQuery} bigquery
 * @param {string} project
 * @param {string} dataset
 * @param {string} tableName
 * @returns {Promise<Array<{partitionId: string, totalRows: number, lastModified: Date}>>}
 */
const snapshotTableMetadata = async (bigquery, project, dataset, tableName) => {
    const query = `
        SELECT
            partition_id,
            total_rows,
            last_modified_time as last_modified
        FROM \`${project}.${dataset}.INFORMATION_SCHEMA.PARTITIONS\`
        WHERE table_name = @tableName
          AND partition_id != '__NULL__'
        ORDER BY partition_id`;

    const [rows] = await bigquery.query({
        query,
        params: { tableName },
        location: process.env.BIGQUERY_LOCATION || 'EU',
    });

    return rows.map(r => ({
        partitionId: r.partition_id,
        totalRows: Number(r.total_rows),
        lastModified: r.last_modified.value ? new Date(r.last_modified.value) : r.last_modified,
    }));
};

/**
 * Validate that the export_type distribution in a table matches expectations.
 * Returns { exportTypes: {type: count}, unexpected: string[] }.
 *
 * @param {BigQuery} bigquery
 * @param {string} project
 * @param {string} dataset
 * @param {string} tableName
 * @param {string[]} expectedTypes - export types that should be present (e.g. ['daily', 'intraday'])
 * @returns {Promise<{exportTypes: Object, unexpected: string[], missing: string[]}>}
 */
const validateExportTypes = async (bigquery, project, dataset, tableName, expectedTypes) => {
    const query = `
        SELECT export_type, COUNT(*) as row_count
        FROM \`${project}.${dataset}.${tableName}\`
        GROUP BY export_type`;

    const [rows] = await bigquery.query({
        query,
        location: process.env.BIGQUERY_LOCATION || 'EU',
    });

    const exportTypes = {};
    rows.forEach(r => {
        exportTypes[r.export_type] = Number(r.row_count);
    });

    const presentTypes = Object.keys(exportTypes);
    const unexpected = presentTypes.filter(t => !expectedTypes.includes(t));
    const missing = expectedTypes.filter(t => !presentTypes.includes(t));

    return { exportTypes, unexpected, missing };
};

/**
 * Validate that data was recently inserted into a table.
 *
 * @param {BigQuery} bigquery
 * @param {string} project
 * @param {string} dataset
 * @param {string} tableName
 * @param {number} maxAgeMinutes - maximum age of the latest insert in minutes
 * @returns {Promise<{fresh: boolean, latestInsert: Date|null, ageMinutes: number|null}>}
 */
const validateDataFreshness = async (bigquery, project, dataset, tableName, maxAgeMinutes) => {
    const query = `
        SELECT MAX(row_inserted_timestamp) as latest_insert
        FROM \`${project}.${dataset}.${tableName}\``;

    const [rows] = await bigquery.query({
        query,
        location: process.env.BIGQUERY_LOCATION || 'EU',
    });

    const latestInsert = rows[0]?.latest_insert;
    if (!latestInsert) {
        return { fresh: false, latestInsert: null, ageMinutes: null };
    }

    const insertDate = latestInsert.value ? new Date(latestInsert.value) : latestInsert;
    const ageMinutes = (Date.now() - insertDate.getTime()) / 60000;

    return {
        fresh: ageMinutes <= maxAgeMinutes,
        latestInsert: insertDate,
        ageMinutes: Math.round(ageMinutes),
    };
};

/**
 * Delete the N most recent partitions from a table.
 * Returns info about what was deleted.
 *
 * @param {BigQuery} bigquery
 * @param {string} project
 * @param {string} dataset
 * @param {string} tableName
 * @param {number} count - number of most recent partitions to delete
 * @returns {Promise<{deletedPartitions: string[], deletedRows: number}>}
 */
const deleteRecentPartitions = async (bigquery, project, dataset, tableName, count) => {
    // First, find the most recent partitions
    const findQuery = `
        SELECT partition_id, total_rows
        FROM \`${project}.${dataset}.INFORMATION_SCHEMA.PARTITIONS\`
        WHERE table_name = @tableName
          AND partition_id != '__NULL__'
          AND total_rows > 0
        ORDER BY partition_id DESC
        LIMIT @count`;

    const [partitions] = await bigquery.query({
        query: findQuery,
        params: { tableName, count },
        location: process.env.BIGQUERY_LOCATION || 'EU',
    });

    if (partitions.length === 0) {
        return { deletedPartitions: [], deletedRows: 0 };
    }

    const partitionIds = partitions.map(p => p.partition_id);
    const totalRows = partitions.reduce((sum, p) => sum + Number(p.total_rows), 0);

    // Convert partition IDs (YYYYMMDD) to date strings for the WHERE clause
    const dateStrings = partitionIds.map(id =>
        `'${id.substring(0, 4)}-${id.substring(4, 6)}-${id.substring(6, 8)}'`
    ).join(', ');

    const deleteQuery = `
        DELETE FROM \`${project}.${dataset}.${tableName}\`
        WHERE event_date IN (${dateStrings})`;

    await bigquery.query({
        query: deleteQuery,
        location: process.env.BIGQUERY_LOCATION || 'EU',
    });

    return { deletedPartitions: partitionIds, deletedRows: totalRows };
};

/**
 * Validate that previously-deleted partitions have been recovered (have rows again).
 *
 * @param {BigQuery} bigquery
 * @param {string} project
 * @param {string} dataset
 * @param {string} tableName
 * @param {string[]} partitionIds - partition IDs (YYYYMMDD format) to check
 * @returns {Promise<{recovered: boolean, partitions: Array<{partitionId: string, totalRows: number}>}>}
 */
const validatePartitionRecovery = async (bigquery, project, dataset, tableName, partitionIds) => {
    const query = `
        SELECT partition_id, total_rows
        FROM \`${project}.${dataset}.INFORMATION_SCHEMA.PARTITIONS\`
        WHERE table_name = @tableName
          AND partition_id IN UNNEST(@partitionIds)`;

    const [rows] = await bigquery.query({
        query,
        params: { tableName, partitionIds },
        location: process.env.BIGQUERY_LOCATION || 'EU',
    });

    const results = rows.map(r => ({
        partitionId: r.partition_id,
        totalRows: Number(r.total_rows),
    }));

    const allRecovered = partitionIds.every(pid => {
        const found = results.find(r => r.partitionId === pid);
        return found && found.totalRows > 0;
    });

    return { recovered: allRecovered, partitions: results };
};

/**
 * Get the set of column names for a table.
 *
 * @param {BigQuery} bigquery
 * @param {string} project
 * @param {string} dataset
 * @param {string} tableName
 * @returns {Promise<Set<string>>}
 */
const getTableColumns = async (bigquery, project, dataset, tableName) => {
    const query = `
        SELECT column_name
        FROM \`${project}.${dataset}.INFORMATION_SCHEMA.COLUMNS\`
        WHERE table_name = @tableName`;

    const [rows] = await bigquery.query({
        query,
        params: { tableName },
        location: process.env.BIGQUERY_LOCATION || 'EU',
    });

    return new Set(rows.map(r => r.column_name));
};

module.exports = {
    createClient,
    snapshotTableMetadata,
    validateExportTypes,
    validateDataFreshness,
    deleteRecentPartitions,
    validatePartitionRecovery,
    getTableColumns,
};
