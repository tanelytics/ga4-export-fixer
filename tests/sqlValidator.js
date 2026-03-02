/**
 * SQL Query Validator Utility
 * 
 * Provides functions to validate SQL queries against BigQuery using dry run.
 * This utility is designed for testing generated SQL without executing queries.
 */

const { BigQuery } = require('@google-cloud/bigquery');
require('dotenv').config();

/**
 * Configuration for SQL validator
 * @typedef {Object} ValidatorConfig
 * @property {string} [projectId] - GCP project ID (defaults to env GOOGLE_CLOUD_PROJECT)
 * @property {string} [location] - BigQuery location (defaults to 'US')
 * @property {boolean} [verbose] - Enable verbose logging (defaults to true)
 * @property {number} [timeout] - Query timeout in milliseconds (defaults to 30000)
 */

/**
 * Result from SQL validation
 * @typedef {Object} ValidationResult
 * @property {boolean} success - Whether validation passed
 * @property {string} sql - The SQL query that was validated
 * @property {Object} [statistics] - Query statistics from BigQuery
 * @property {number} [statistics.totalBytesProcessed] - Estimated bytes to process
 * @property {number} [statistics.estimatedCostUSD] - Estimated cost in USD
 * @property {Object} [statistics.schema] - Output schema information
 * @property {Error} [error] - Error object if validation failed
 * @property {string} [errorMessage] - Human-readable error message
 * @property {Array} [errorDetails] - Detailed error information from BigQuery
 */

/**
 * Create a BigQuery client with optional configuration
 * @param {ValidatorConfig} config - Validator configuration
 * @returns {BigQuery} Configured BigQuery client
 */
const createBigQueryClient = (config = {}) => {
  const clientConfig = {};
  
  if (config.projectId) {
    clientConfig.projectId = config.projectId;
  }
  
  if (config.keyFilename || process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    clientConfig.keyFilename = config.keyFilename || process.env.GOOGLE_APPLICATION_CREDENTIALS;
  }
  
  return new BigQuery(clientConfig);
};

/**
 * Format bytes to human-readable string
 * @param {number} bytes - Number of bytes
 * @returns {string} Formatted string (e.g., "1.23 GB")
 */
const formatBytes = (bytes) => {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

/**
 * Calculate estimated cost based on bytes processed
 * @param {number} bytes - Number of bytes to process
 * @returns {number} Estimated cost in USD (at $5/TB)
 */
const calculateCost = (bytes) => {
  const TB = 1e12;
  const costPerTB = 5;
  return (bytes / TB) * costPerTB;
};

/**
 * Log validation results in a formatted way
 * @param {ValidationResult} result - Validation result to log
 * @param {ValidatorConfig} config - Validator configuration
 */
const logValidationResult = (result, config = {}) => {
  const verbose = config.verbose !== false;
  
  if (result.success) {
    console.log('\n✅ SQL Validation PASSED\n');
    
    if (result.statistics && verbose) {
      const stats = result.statistics;
      const bytes = parseInt(stats.totalBytesProcessed) || 0;
      const cost = calculateCost(bytes);
      
      console.log('Query Statistics:');
      console.log(`  📊 Estimated bytes: ${formatBytes(bytes)} (${bytes.toLocaleString()} bytes)`);
      console.log(`  💰 Estimated cost: $${cost.toFixed(6)}`);
      
      if (stats.query) {
        console.log(`  🔄 Cache hit: ${stats.query.cacheHit || false}`);
        console.log(`  📝 Statement type: ${stats.query.statementType || 'N/A'}`);
        
        if (stats.query.schema && stats.query.schema.fields) {
          console.log(`  📋 Output columns: ${stats.query.schema.fields.length}`);
          
          if (verbose && stats.query.schema.fields.length > 0) {
            console.log('\n  Column Schema:');
            stats.query.schema.fields.forEach((field, index) => {
              console.log(`    ${index + 1}. ${field.name} (${field.type}${field.mode !== 'NULLABLE' ? ', ' + field.mode : ''})`);
            });
          }
        }
        
        if (stats.query.referencedTables && stats.query.referencedTables.length > 0) {
          console.log(`\n  📚 Referenced tables: ${stats.query.referencedTables.length}`);
          if (verbose) {
            stats.query.referencedTables.forEach((table, index) => {
              console.log(`    ${index + 1}. ${table.projectId}.${table.datasetId}.${table.tableId}`);
            });
          }
        }
      }
    }
  } else {
    console.error('\n❌ SQL Validation FAILED\n');
    console.error('Error Message:');
    console.error(`  ${result.errorMessage}\n`);
    
    if (result.errorDetails && result.errorDetails.length > 0) {
      console.error('Detailed Errors:');
      result.errorDetails.forEach((err, index) => {
        console.error(`\n  ${index + 1}. ${err.message || err.reason || 'Unknown error'}`);
        
        if (err.location) {
          const location = [];
          if (err.location.line) location.push(`Line ${err.location.line}`);
          if (err.location.column) location.push(`Column ${err.location.column}`);
          if (location.length > 0) {
            console.error(`     Location: ${location.join(', ')}`);
          }
          
          // Show the problematic line from the SQL
          if (err.location.line && result.sql) {
            const lines = result.sql.split('\n');
            const errorLine = err.location.line - 1; // 0-indexed
            
            if (errorLine >= 0 && errorLine < lines.length) {
              console.error(`\n     Problematic SQL line:`);
              const lineNumber = String(err.location.line).padStart(4, ' ');
              console.error(`     ${lineNumber} | ${lines[errorLine]}`);
              
              // Show pointer to error column if available
              if (err.location.column) {
                const pointer = ' '.repeat(lineNumber.length + 3 + err.location.column) + '^';
                console.error(`     ${pointer}`);
              }
            }
          }
        }
        
        if (err.reason && err.reason !== err.message) {
          console.error(`     Reason: ${err.reason}`);
        }
        
        if (err.debugInfo) {
          console.error(`     Debug: ${err.debugInfo}`);
        }
      });
    }
  }
  
  if (verbose) {
    console.log('\n' + '='.repeat(80) + '\n');
  }
};

/**
 * Validate SQL query using BigQuery dry run
 * 
 * @param {string} sql - SQL query to validate
 * @param {ValidatorConfig} config - Validator configuration
 * @returns {Promise<ValidationResult>} Validation result
 * 
 * @example
 * const result = await validateSQL('SELECT * FROM `project.dataset.table`', {
 *   projectId: 'my-project',
 *   location: 'US',
 *   verbose: true
 * });
 * 
 * if (result.success) {
 *   console.log('Query is valid!');
 * } else {
 *   console.error('Query failed:', result.errorMessage);
 * }
 */
const validateSQL = async (sql, config = {}) => {
  const startTime = Date.now();
  const verbose = config.verbose !== false;
  
  try {
    if (!sql || typeof sql !== 'string') {
      throw new Error('SQL query must be a non-empty string');
    }
    
    if (verbose) {
      console.log('\n' + '='.repeat(80));
      console.log('VALIDATING SQL QUERY');
      console.log('='.repeat(80));
      console.log('\nQuery:');
      console.log(sql);
      console.log('\n' + '-'.repeat(80) + '\n');
    }
    
    // Create BigQuery client
    const bigquery = createBigQueryClient(config);
    
    // Prepare dry run options
    const options = {
      query: sql,
      dryRun: true,
      location: config.location || process.env.BIGQUERY_LOCATION || 'US',
      useLegacySql: false,
    };
    
    if (config.timeout) {
      options.timeoutMs = config.timeout;
    }
    
    // Run the dry run
    const [job] = await bigquery.createQueryJob(options);
    const statistics = job.metadata.statistics;
    
    // Calculate additional statistics
    const bytes = parseInt(statistics.totalBytesProcessed) || 0;
    const estimatedCostUSD = calculateCost(bytes);
    
    const result = {
      success: true,
      sql,
      statistics: {
        ...statistics,
        estimatedCostUSD,
        totalBytesProcessed: bytes,
      },
      validationTimeMs: Date.now() - startTime,
    };
    
    if (verbose) {
      logValidationResult(result, config);
    }
    
    return result;
    
  } catch (error) {
    const result = {
      success: false,
      sql,
      error,
      errorMessage: error.message,
      errorDetails: error.errors || [],
      validationTimeMs: Date.now() - startTime,
    };
    
    if (verbose) {
      logValidationResult(result, config);
    }
    
    return result;
  }
};

/**
 * Validate multiple SQL queries and return combined results
 * 
 * @param {Array<{name: string, sql: string}>} queries - Array of named queries to validate
 * @param {ValidatorConfig} config - Validator configuration
 * @returns {Promise<Array<ValidationResult>>} Array of validation results
 */
const validateMultipleSQL = async (queries, config = {}) => {
  const results = [];
  
  console.log(`\nValidating ${queries.length} SQL queries...\n`);
  
  for (let i = 0; i < queries.length; i++) {
    const query = queries[i];
    console.log(`\n[${i + 1}/${queries.length}] Validating: ${query.name}`);
    
    const result = await validateSQL(query.sql, { ...config, verbose: false });
    result.name = query.name;
    results.push(result);
    
    if (result.success) {
      const bytes = result.statistics?.totalBytesProcessed || 0;
      const cost = result.statistics?.estimatedCostUSD || 0;
      console.log(`  ✅ ${query.name} - PASSED`);
      console.log(`     Bytes: ${formatBytes(bytes)} | Cost: $${cost.toFixed(6)}`);
    } else {
      console.log(`  ❌ ${query.name} - FAILED: ${result.errorMessage}`);
    }
  }
  
  // Summary
  const passed = results.filter(r => r.success).length;
  const failed = results.filter(r => !r.success).length;
  
  console.log('\n' + '='.repeat(80));
  console.log('VALIDATION SUMMARY');
  console.log('='.repeat(80));
  console.log(`Total: ${results.length} | Passed: ${passed} | Failed: ${failed}`);
  
  if (passed > 0) {
    console.log('\nPassed queries:');
    results.filter(r => r.success).forEach(r => {
      const bytes = r.statistics?.totalBytesProcessed || 0;
      const cost = r.statistics?.estimatedCostUSD || 0;
      console.log(`  ✅ ${r.name}`);
      console.log(`     Bytes: ${formatBytes(bytes)} | Cost: $${cost.toFixed(6)}`);
    });
  }
  
  if (failed > 0) {
    console.log('\nFailed queries:');
    results.filter(r => !r.success).forEach(r => {
      console.log(`  ❌ ${r.name}: ${r.errorMessage}`);
    });
  }
  
  console.log('='.repeat(80) + '\n');
  
  return results;
};

module.exports = {
  validateSQL,
  validateMultipleSQL,
  createBigQueryClient,
  formatBytes,
  calculateCost,
  logValidationResult,
};
